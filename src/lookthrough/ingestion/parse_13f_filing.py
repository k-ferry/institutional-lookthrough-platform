"""Parse SEC EDGAR 13F-HR filings to extract public equity holdings.

Downloads 13F-HR filings from EDGAR for configured institutional managers
and maps holdings to the Silver layer schema (dim_fund, fact_fund_report,
fact_reported_holding). Supports both PostgreSQL and CSV output modes.

Usage:
    python -m src.lookthrough.ingestion.parse_13f_filing
    python -m src.lookthrough.ingestion.parse_13f_filing --csv
    python -m src.lookthrough.ingestion.parse_13f_filing --quarters 4
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import time
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd
from bs4 import BeautifulSoup

from src.lookthrough.db.models import DimCompany, DimFund, FactFundReport, FactReportedHolding
from src.lookthrough.db.repository import (
    _is_csv_mode,
    dataframe_to_records,
    ensure_tables,
    execute_update,
    get_all,
    upsert_rows,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RAW_DIR = Path("data/raw/13f")
SILVER_DIR = Path("data/silver")
SOURCE_13F = "13f_filing"
EXTRACTION_CONFIDENCE = 1.0  # Structured XML — no parsing uncertainty
EDGAR_BASE = "https://data.sec.gov"
EDGAR_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
EDGAR_HEADERS = {"User-Agent": "LookThrough kyle@lookthrough.com"}
REQUEST_DELAY = 1.5   # SEC rate-limit requirement (seconds between requests)
RETRY_MAX = 3         # Maximum retries on 503 Service Unavailable
RETRY_BASE_WAIT = 5   # First retry wait in seconds; doubles each attempt


# ---------------------------------------------------------------------------
# Filer Configuration
# ---------------------------------------------------------------------------

FILERS: list[dict] = [
    {
        "cik": "0001336528",
        "display_name": "Vertex Macro Fund",
        "manager_name": "Pershing Square Capital Management",
        "fund_type": "hedge_fund",
        "strategy": "Concentrated Value",
        # Raw <value> is in whole dollars (not thousands) for this filer
        "value_multiplier": 1,
    },
    {
        "cik": "0001697748",
        "display_name": "Brightline Innovation ETF",
        "manager_name": "ARK Investment Management",
        "fund_type": "etf",
        "strategy": "Disruptive Innovation",
        # Raw <value> is in whole dollars (not thousands) for this filer
        "value_multiplier": 1,
    },
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def make_uuid(seed: str) -> str:
    """Generate a deterministic UUID from a seed string."""
    hash_bytes = hashlib.md5(seed.encode()).digest()
    return str(uuid.UUID(bytes=hash_bytes))


def edgar_get(url: str) -> Optional[httpx.Response]:
    """Rate-limited GET to EDGAR with exponential backoff on 503.

    Always waits REQUEST_DELAY before the first attempt. On HTTP 503, retries
    up to RETRY_MAX times with waits of RETRY_BASE_WAIT * 2^attempt seconds.
    Returns None on 404, exhausted retries, or unrecoverable error.
    """
    time.sleep(REQUEST_DELAY)
    for attempt in range(RETRY_MAX + 1):
        try:
            resp = httpx.get(url, headers=EDGAR_HEADERS, timeout=30, follow_redirects=True)
        except httpx.HTTPError as e:
            logger.error("Request failed for %s: %s", url, e)
            return None

        if resp.status_code == 404:
            logger.warning("404 Not Found: %s", url)
            return None

        if resp.status_code == 503:
            if attempt < RETRY_MAX:
                wait = RETRY_BASE_WAIT * (2 ** attempt)  # 5s, 10s, 20s
                logger.warning(
                    "503 Service Unavailable for %s — retry %d/%d in %ds",
                    url, attempt + 1, RETRY_MAX, wait,
                )
                time.sleep(wait)
                continue
            logger.error("503 persists after %d retries: %s", RETRY_MAX, url)
            return None

        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("HTTP error for %s: %s", url, e)
            return None

        return resp

    return None  # unreachable, but satisfies type checker


def _local_tag(element: ET.Element) -> str:
    """Strip XML namespace from an element's tag: {ns}localname -> localname."""
    tag = element.tag
    if "}" in tag:
        return tag.split("}")[1]
    return tag


def _child_text(parent: ET.Element, *local_names: str) -> Optional[str]:
    """
    Find the first descendant whose local tag matches one of the given names
    (case-insensitive) and return its stripped text content.
    """
    lower_names = {n.lower() for n in local_names}
    for elem in parent.iter():
        if _local_tag(elem).lower() in lower_names:
            return (elem.text or "").strip() or None
    return None


# ---------------------------------------------------------------------------
# EDGAR Filing Discovery
# ---------------------------------------------------------------------------

def get_filing_list(cik: str, quarters: int = 8) -> list[dict]:
    """
    Return the most recent `quarters` 13F-HR filings for a CIK.

    Uses the EDGAR submissions API. Excludes 13F-HR/A amendments.
    Sorted by reportDate descending.
    """
    url = f"{EDGAR_BASE}/submissions/CIK{cik}.json"
    resp = edgar_get(url)
    if not resp:
        return []

    data = resp.json()
    filings = _extract_13f_from_recent(data.get("filings", {}).get("recent", {}))

    # Fetch additional history pages when needed
    for file_entry in data.get("filings", {}).get("files", []):
        if len(filings) >= quarters:
            break
        page_url = f"{EDGAR_BASE}/{file_entry['name']}"
        page_resp = edgar_get(page_url)
        if page_resp:
            page_data = page_resp.json()
            filings.extend(
                _extract_13f_from_recent(page_data.get("filings", {}).get("recent", {}))
            )

    filings.sort(key=lambda x: x["reportDate"], reverse=True)
    return filings[:quarters]


def _extract_13f_from_recent(recent: dict) -> list[dict]:
    """Parse the 'recent' filings object from EDGAR submissions JSON.

    Returns only 13F-HR filings (not 13F-HR/A amendments).
    """
    accessions = recent.get("accessionNumber", [])
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])

    results = []
    for acc, form, filing_date, report_date, primary_doc in zip(
        accessions, forms, filing_dates, report_dates, primary_docs
    ):
        if form != "13F-HR":
            continue
        results.append(
            {
                "accessionNumber": acc,
                "form": form,
                "filingDate": filing_date,
                "reportDate": report_date or filing_date,
                "primaryDocument": primary_doc,
            }
        )
    return results


# ---------------------------------------------------------------------------
# Document Discovery
# ---------------------------------------------------------------------------

def find_info_table_url(cik: str, accession_number: str) -> Optional[str]:
    """
    Locate the information table XML URL within a 13F-HR filing.

    Tries URL patterns in this order:
      1. {acc}-index.htm  (most common)
      2. {acc}-index.html (some older/newer filings)
      3. CGI browse-edgar listing — finds the filing index link by accession
         number, then fetches and parses that page

    Returns the full https:// URL of the information table XML, or None.
    """
    numeric_cik = cik.lstrip("0") or "0"
    acc_no_dash = accession_number.replace("-", "")
    base = f"{EDGAR_ARCHIVES}/{numeric_cik}/{acc_no_dash}"

    # --- Attempt 1 & 2: direct index URL patterns ---
    for suffix in ("-index.htm", "-index.html"):
        index_url = f"{base}/{acc_no_dash}{suffix}"
        resp = edgar_get(index_url)
        if resp:
            result = _parse_index_html(resp.text, source_url=index_url)
            if result:
                return result
            # HTML fetched but no XML found — no point trying the other suffix
            break

    # --- Attempt 3: CGI browse-edgar listing ---
    cgi_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={numeric_cik}&type=13F-HR"
        f"&dateb=&owner=include&count=10"
    )
    logger.info("Falling back to CGI browse-edgar for %s", accession_number)
    cgi_resp = edgar_get(cgi_url)
    if cgi_resp:
        soup = BeautifulSoup(cgi_resp.text, "html.parser")
        # The CGI page has anchor links to filing index pages; find the one
        # matching our accession number (both dashed and no-dash forms work).
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if acc_no_dash in href.replace("-", "") and "index" in href.lower():
                index_resp = edgar_get(_absolute_url(href))
                if index_resp:
                    result = _parse_index_html(index_resp.text, source_url=href)
                    if result:
                        return result
                break

    logger.warning(
        "Could not find information table XML for accession %s (CIK %s)",
        accession_number, cik,
    )
    return None


def _parse_index_html(html: str, source_url: str = "") -> Optional[str]:
    """
    Extract the information table XML URL from a filing index HTML page.

    Primary scan: rows where the Type column is exactly "INFORMATION TABLE"
    and the document link points to a raw XML file (not the XSL transformer).
    Secondary scan: first XML link whose type column contains "INFORMATION TABLE",
    again excluding XSL-transformer paths.

    XSL-transformed URLs (containing 'xslForm13F_X02' or 'xslForm13F' in the
    path) are always skipped — they serve an HTML view, not the raw XML.

    Returns an absolute URL or None.
    """
    soup = BeautifulSoup(html, "html.parser")

    def _is_xsl_path(href: str) -> bool:
        return "xslForm13F" in href

    # Primary: Type column == "INFORMATION TABLE" (exact, case-insensitive)
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        type_text = cells[3].get_text(strip=True).upper()
        if type_text != "INFORMATION TABLE":
            continue
        link = cells[2].find("a", href=True)
        if not link:
            continue
        href = link["href"]
        if href.lower().endswith(".xml") and not _is_xsl_path(href):
            return _absolute_url(href)

    # Secondary: Type column contains "INFORMATION TABLE" (partial match)
    # Catches slight variations like "13F INFORMATION TABLE"
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        type_text = cells[3].get_text(strip=True).upper()
        if "INFORMATION TABLE" not in type_text:
            continue
        link = cells[2].find("a", href=True)
        if not link:
            continue
        href = link["href"]
        if href.lower().endswith(".xml") and not _is_xsl_path(href):
            return _absolute_url(href)

    if source_url:
        logger.debug("No info table XML found in index page: %s", source_url)
    return None


def _absolute_url(href: str) -> str:
    """Make a relative EDGAR href into an absolute URL."""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return f"https://www.sec.gov{href}"
    return href


# ---------------------------------------------------------------------------
# XML Parsing
# ---------------------------------------------------------------------------

def parse_info_table_xml(
    xml_content: str,
    period_of_report: str,
    fund_report_id: str,
    value_multiplier: float = 1,
) -> list[dict]:
    """
    Parse a 13F information table XML document.

    Handles both namespaced (2013+) and non-namespaced XML formats.
    Returns a list of raw dicts matching the fact_reported_holding schema.

    Value is in thousands USD in the XML — multiplied by 1000 here.
    CUSIP is stored in the dedicated cusip column.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.error("XML parse error: %s", e)
        return []

    holdings = []
    row_num = 0

    for elem in root.iter():
        if _local_tag(elem).lower() != "infotable":
            continue

        row_num += 1
        issuer_name = _child_text(elem, "nameOfIssuer") or ""
        title_of_class = _child_text(elem, "titleOfClass") or ""
        cusip = _child_text(elem, "cusip") or ""
        value_str = _child_text(elem, "value") or "0"
        shares_str = _child_text(elem, "sshPrnamt") or "0"
        investment_discretion = _child_text(elem, "investmentDiscretion")

        # Voting authority — look inside votingAuthority subtree
        voting_sole = voting_shared = voting_none = 0
        for child in elem.iter():
            local = _local_tag(child).lower()
            if local == "votingauthority":
                for sub in child:
                    sub_local = _local_tag(sub).lower()
                    try:
                        val = int((sub.text or "0").replace(",", ""))
                    except ValueError:
                        val = 0
                    if sub_local == "sole":
                        voting_sole = val
                    elif sub_local == "shared":
                        voting_shared = val
                    elif sub_local == "none":
                        voting_none = val
                break

        try:
            value_usd = float(value_str.replace(",", "")) * value_multiplier
        except (ValueError, TypeError):
            value_usd = 0.0

        holding_id = make_uuid(f"13f_{fund_report_id}_{cusip}_{row_num}")

        holdings.append(
            {
                "reported_holding_id": holding_id,
                "fund_report_id": fund_report_id,
                "company_id": None,  # Resolved by entity_resolution step
                "raw_company_name": issuer_name,
                "reported_sector": None,  # 13F does not include sector
                "reported_country": "United States",  # 13F covers US-listed securities
                "reported_value_usd": value_usd,
                "reported_pct_nav": None,  # Not available in 13F
                "cusip": cusip or None,
                "extraction_method": "13f_xml",
                "extraction_confidence": EXTRACTION_CONFIDENCE,
                "document_id": None,
                "page_number": None,
                "row_number": float(row_num),
                "source": SOURCE_13F,
                "as_of_date": period_of_report,
            }
        )

    return holdings


# ---------------------------------------------------------------------------
# Raw XML Storage
# ---------------------------------------------------------------------------

def save_raw_xml(cik: str, period: str, content: bytes) -> None:
    """Save raw XML bytes to data/raw/13f/{cik}/{period}.xml."""
    out_dir = RAW_DIR / cik
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{period}.xml"
    out_path.write_bytes(content)
    logger.debug("Saved raw XML → %s", out_path)


# ---------------------------------------------------------------------------
# Per-Filer Orchestration
# ---------------------------------------------------------------------------

def parse_13f_filer(
    cik: str,
    display_name: str,
    manager_name: str,
    fund_type: str,
    strategy: str,
    quarters: int = 8,
    value_multiplier: float = 1,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Fetch and parse 13F-HR filings for a single CIK.

    Returns (fund_records, fund_report_records, holding_records).
    """
    fund_id = make_uuid(f"13f_fund_{cik}")

    fund_record = {
        "fund_id": fund_id,
        "fund_name": display_name,
        "manager_name": manager_name,
        "fund_type": fund_type,
        "strategy": strategy,
        "vintage_year": None,
        "base_currency": "USD",
        "source": SOURCE_13F,
    }

    filings = get_filing_list(cik, quarters=quarters)
    if not filings:
        logger.warning("No 13F-HR filings found for CIK %s (%s)", cik, display_name)
        return [fund_record], [], []

    all_reports: list[dict] = []
    all_holdings: list[dict] = []
    quarters_parsed = 0

    for filing in filings:
        acc = filing["accessionNumber"]
        period = filing["reportDate"]
        filing_date = filing["filingDate"]
        fund_report_id = make_uuid(f"13f_report_{cik}_{acc}")

        logger.info("  Fetching %s — period %s", acc, period)

        xml_url = find_info_table_url(cik, acc)
        if not xml_url:
            logger.warning("  Skipping %s — info table not found", acc)
            continue

        xml_resp = edgar_get(xml_url)
        if not xml_resp:
            logger.warning("  Skipping %s — XML download failed", acc)
            continue

        save_raw_xml(cik, period, xml_resp.content)

        holdings = parse_info_table_xml(
            xml_resp.text, period, fund_report_id, value_multiplier=value_multiplier
        )
        if not holdings:
            logger.warning("  No holdings parsed from %s", xml_url)
            continue

        all_reports.append(
            {
                "fund_report_id": fund_report_id,
                "fund_id": fund_id,
                "report_period_end": period,
                "received_date": filing_date,
                "document_id": None,
                "coverage_estimate": 1.0,
                "nav_usd": None,  # 13F does not disclose NAV
                "source": SOURCE_13F,
            }
        )
        all_holdings.extend(holdings)
        quarters_parsed += 1

        logger.info(
            "  Parsed %d holdings for %s (period: %s)",
            len(holdings),
            display_name,
            period,
        )

    print(
        f"Parsed {len(all_holdings)} holdings across {quarters_parsed} quarters"
        f" for {display_name}"
    )
    return [fund_record], all_reports, all_holdings


# ---------------------------------------------------------------------------
# Company Stub Creation
# ---------------------------------------------------------------------------

def _build_company_stubs(holdings: list[dict], db_mode: bool) -> list[dict]:
    """
    Create dim_company stub entries for 13F issuers not already in dim_company.

    Skips names that already exist (case-insensitive). In db_mode, checks the
    live database; in CSV mode, deduplicates within this batch only.
    """
    existing_lower: set[str] = set()
    if db_mode:
        existing_df = get_all(DimCompany)
        if not existing_df.empty:
            existing_lower = set(existing_df["company_name"].str.lower().dropna())

    seen: set[str] = set()
    new_companies = []

    for h in holdings:
        name = (h.get("raw_company_name") or "").strip()
        if not name:
            continue
        name_lower = name.lower()
        if name_lower in existing_lower or name_lower in seen:
            continue
        seen.add(name_lower)
        new_companies.append(
            {
                "company_id": make_uuid(f"13f_company_{name}"),
                "company_name": name,
                "primary_sector": None,
                "primary_industry": None,
                "primary_country": "United States",
                "industry_taxonomy_node_id": None,
                "country_taxonomy_node_id": None,
                "website": None,
                "created_at": pd.Timestamp.now().date().isoformat(),
                "source": SOURCE_13F,
            }
        )

    return new_companies


# ---------------------------------------------------------------------------
# Write Helpers
# ---------------------------------------------------------------------------

def _run_migrations() -> None:
    """Apply any schema changes required by this module that aren't in the base DDL.

    Safe to run repeatedly — uses IF NOT EXISTS so it's a no-op after the first run.
    """
    execute_update(
        "ALTER TABLE fact_reported_holding "
        "ADD COLUMN IF NOT EXISTS cusip VARCHAR(12)"
    )


def _write_to_db(
    funds_df: pd.DataFrame,
    reports_df: pd.DataFrame,
    holdings_df: pd.DataFrame,
    companies_df: pd.DataFrame,
) -> None:
    ensure_tables()
    _run_migrations()

    if not companies_df.empty:
        upsert_rows(DimCompany, dataframe_to_records(companies_df), ["company_id"])
        print(f"  Upserted {len(companies_df)} company stubs to dim_company")

    upsert_rows(DimFund, dataframe_to_records(funds_df), ["fund_id"])
    print(f"  Upserted {len(funds_df)} funds to dim_fund")

    upsert_rows(FactFundReport, dataframe_to_records(reports_df), ["fund_report_id"])
    print(f"  Upserted {len(reports_df)} fund reports to fact_fund_report")

    upsert_rows(
        FactReportedHolding, dataframe_to_records(holdings_df), ["reported_holding_id"]
    )
    print(f"  Upserted {len(holdings_df)} holdings to fact_reported_holding")


def _write_to_csv(
    funds_df: pd.DataFrame,
    reports_df: pd.DataFrame,
    holdings_df: pd.DataFrame,
    companies_df: pd.DataFrame,
) -> None:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    if not companies_df.empty:
        path = SILVER_DIR / "13f_dim_company.csv"
        companies_df.to_csv(path, index=False)
        print(f"  Wrote {len(companies_df)} rows to {path}")

    for df, name in [
        (funds_df, "13f_dim_fund.csv"),
        (reports_df, "13f_fact_fund_report.csv"),
        (holdings_df, "13f_fact_reported_holding.csv"),
    ]:
        path = SILVER_DIR / name
        df.to_csv(path, index=False)
        print(f"  Wrote {len(df)} rows to {path}")


# ---------------------------------------------------------------------------
# Main Entry Points
# ---------------------------------------------------------------------------

def parse_all_13f_filers(quarters: int = 8, db_mode: bool = True) -> None:
    """
    Parse all configured FILERS for the last N quarters and persist results.

    Args:
        quarters: Number of quarterly filings to fetch per filer (default 8).
        db_mode:  True → write to PostgreSQL; False → write to CSV files.
    """
    all_funds: list[dict] = []
    all_reports: list[dict] = []
    all_holdings: list[dict] = []

    for filer in FILERS:
        logger.info(
            "Processing filer: %s (CIK %s)", filer["display_name"], filer["cik"]
        )
        funds, reports, holdings = parse_13f_filer(
            cik=filer["cik"],
            display_name=filer["display_name"],
            manager_name=filer["manager_name"],
            fund_type=filer["fund_type"],
            strategy=filer["strategy"],
            quarters=quarters,
            value_multiplier=filer.get("value_multiplier", 1),
        )
        all_funds.extend(funds)
        all_reports.extend(reports)
        all_holdings.extend(holdings)

    if not all_reports:
        print("No filings parsed — nothing to write.")
        return

    funds_df = pd.DataFrame(all_funds).drop_duplicates(subset=["fund_id"])
    reports_df = pd.DataFrame(all_reports).drop_duplicates(subset=["fund_report_id"])
    holdings_df = pd.DataFrame(all_holdings).drop_duplicates(
        subset=["reported_holding_id"]
    )

    companies_df = pd.DataFrame(
        _build_company_stubs(all_holdings, db_mode=db_mode)
    )

    print(
        f"\nTotal: {len(funds_df)} funds, {len(reports_df)} reports,"
        f" {len(holdings_df)} holdings, {len(companies_df)} new company stubs"
    )

    if db_mode:
        print("Writing to PostgreSQL...")
        _write_to_db(funds_df, reports_df, holdings_df, companies_df)
    else:
        print("Writing to CSV...")
        _write_to_csv(funds_df, reports_df, holdings_df, companies_df)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse SEC EDGAR 13F-HR filings for configured institutional managers."
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Write output to CSV files instead of PostgreSQL",
    )
    parser.add_argument(
        "--quarters",
        type=int,
        default=8,
        help="Number of quarterly filings to fetch per filer (default: 8)",
    )
    args = parser.parse_args()

    csv_mode = args.csv or _is_csv_mode()

    print("=" * 60)
    print("13F Filing Ingestion")
    print("=" * 60)
    print(f"Filers     : {len(FILERS)}")
    print(f"Quarters   : {args.quarters}")
    print(f"Output mode: {'CSV' if csv_mode else 'PostgreSQL'}")
    print()

    parse_all_13f_filers(quarters=args.quarters, db_mode=not csv_mode)

    print("\nDone.")


if __name__ == "__main__":
    main()
