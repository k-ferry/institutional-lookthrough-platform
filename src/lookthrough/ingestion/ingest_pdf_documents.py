"""Ingest PDF fund documents and extract structured holdings data.

Supports three document types:
- financial_statements: PE/VC quarterly financials (Schedule of Investments)
- lp_statement:         LP Capital Account Statements (fund-level only)
- transparency_report:  Hedge fund transparency reports (position list)

Uses pdfplumber for PDF text extraction and Claude Haiku for AI-driven parsing.
Tracks processed files by content hash (data/bronze/pdf_ingestion_manifest.json)
to avoid re-ingesting the same document on successive pipeline runs.

Usage:
    python -m src.lookthrough.ingestion.ingest_pdf_documents
    python -m src.lookthrough.ingestion.ingest_pdf_documents --csv
    python -m src.lookthrough.ingestion.ingest_pdf_documents --folder /path/to/docs
    python -m src.lookthrough.ingestion.ingest_pdf_documents --force  # re-ingest all
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import uuid
from pathlib import Path
from typing import Optional

import anthropic
import pandas as pd
import pdfplumber

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.lookthrough.db.engine import get_session_context
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

DEFAULT_BASE_FOLDER = Path(r"C:\Users\kylej\OneDrive\LookThrough Fund Documents")
BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")
MANIFEST_PATH = BRONZE_DIR / "pdf_ingestion_manifest.json"
SOURCE = "pdf_document"
EXTRACTION_CONFIDENCE = 0.80
CLAUDE_MODEL = "claude-haiku-4-5-20251001"
MAX_TEXT_CHARS = 40_000  # Truncate PDFs beyond this before sending to Claude


# ---------------------------------------------------------------------------
# Fund Configuration
# ---------------------------------------------------------------------------

FUND_CONFIG = [
    {
        "folder": "meridian",
        "fund_name": "Meridian Capital Partners III",
        "fund_type": "private_equity",
        "doc_types": ["financial_statements", "lp_statement"],
    },
    {
        "folder": "hartwell",
        "fund_name": "Hartwell Buyout Fund V",
        "fund_type": "private_equity",
        "doc_types": ["financial_statements", "lp_statement"],
    },
    {
        "folder": "brightline",
        "fund_name": "Brightline Ventures II",
        "fund_type": "venture_capital",
        "doc_types": ["financial_statements", "lp_statement"],
    },
    {
        "folder": "apex",
        "fund_name": "Apex Growth Fund",
        "fund_type": "venture_capital",
        "doc_types": ["financial_statements", "lp_statement"],
    },
    {
        "folder": "foundry",
        "fund_name": "Foundry Seed Fund I",
        "fund_type": "venture_capital",
        "doc_types": ["financial_statements", "lp_statement"],
    },
    {
        "folder": "irongate",
        "fund_name": "Irongate Credit Fund",
        "fund_type": "private_credit",
        "doc_types": ["financial_statements", "lp_statement"],
    },
    {
        "folder": "crestview",
        "fund_name": "Crestview Technology Partners",
        "fund_type": "hedge_fund",
        "doc_types": ["transparency_report"],
    },
]


# ---------------------------------------------------------------------------
# Claude Extraction Prompts
# ---------------------------------------------------------------------------

_MONETARY_NOTE = (
    "IMPORTANT: All monetary values must be returned as full integers in USD, "
    "not abbreviated (e.g. 68400000 not 68.4 or 68.4M or 68,400).\n\n"
)

PROMPTS: dict[str, str] = {
    "financial_statements": (
        "Extract the Schedule of Investments from this fund document. "
        "Return ONLY valid JSON:\n"
        "{\n"
        '  "fund_name": "...",\n'
        '  "reporting_date": "YYYY-MM-DD",\n'
        '  "holdings": [\n'
        "    {\n"
        '      "company_name": "...",\n'
        '      "sector": "...",\n'
        '      "cost_basis_usd": 0.0,\n'
        '      "fair_value_usd": 0.0,\n'
        '      "ownership_pct": 0.0\n'
        "    }\n"
        "  ]\n"
        "}\n\n"
        + _MONETARY_NOTE
        + "Document text:\n"
    ),
    "lp_statement": (
        "Extract the LP capital account data from this document. "
        "Return ONLY valid JSON:\n"
        "{\n"
        '  "fund_name": "...",\n'
        '  "lp_name": "...",\n'
        '  "reporting_date": "YYYY-MM-DD",\n'
        '  "nav_usd": 0.0,\n'
        '  "contributions_usd": 0.0,\n'
        '  "distributions_usd": 0.0,\n'
        '  "irr_pct": 0.0,\n'
        '  "moic": 0.0,\n'
        '  "unfunded_commitment_usd": 0.0\n'
        "}\n\n"
        + _MONETARY_NOTE
        + "Document text:\n"
    ),
    "transparency_report": (
        "Extract all holdings from this hedge fund transparency report. "
        "Return ONLY valid JSON:\n"
        "{\n"
        '  "fund_name": "...",\n'
        '  "reporting_date": "YYYY-MM-DD",\n'
        '  "holdings": [\n'
        "    {\n"
        '      "company_name": "...",\n'
        '      "is_public": true,\n'
        '      "fair_value_usd": 0.0,\n'
        '      "pct_nav": 0.0,\n'
        '      "sector": "..."\n'
        "    }\n"
        "  ]\n"
        "}\n\n"
        + _MONETARY_NOTE
        + "Document text:\n"
    ),
}


# ---------------------------------------------------------------------------
# Manifest — tracks processed files by content hash
# ---------------------------------------------------------------------------

def _load_manifest() -> dict[str, dict]:
    """Load the PDF ingestion manifest from disk. Returns {} if missing or corrupt."""
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_manifest(manifest: dict[str, dict]) -> None:
    """Persist the ingestion manifest to disk."""
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _file_hash(pdf_path: Path) -> str:
    """Return an MD5 hex digest of the file's raw bytes."""
    return hashlib.md5(pdf_path.read_bytes()).hexdigest()


def _run_migrations() -> None:
    """Add new columns to existing tables. Safe to run repeatedly (IF NOT EXISTS)."""
    stmts = [
        "ALTER TABLE fact_fund_report ADD COLUMN IF NOT EXISTS lp_name VARCHAR(255)",
        "ALTER TABLE fact_fund_report ADD COLUMN IF NOT EXISTS irr_pct FLOAT",
        "ALTER TABLE fact_fund_report ADD COLUMN IF NOT EXISTS moic FLOAT",
        "ALTER TABLE fact_fund_report ADD COLUMN IF NOT EXISTS contributions_usd FLOAT",
        "ALTER TABLE fact_fund_report ADD COLUMN IF NOT EXISTS distributions_usd FLOAT",
        "ALTER TABLE fact_fund_report ADD COLUMN IF NOT EXISTS unfunded_commitment_usd FLOAT",
        "ALTER TABLE fact_reported_holding ADD COLUMN IF NOT EXISTS cost_basis_usd FLOAT",
        "ALTER TABLE fact_reported_holding ADD COLUMN IF NOT EXISTS ownership_pct FLOAT",
    ]
    for stmt in stmts:
        execute_update(stmt)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _make_uuid(seed: str) -> str:
    """Generate a deterministic UUID from a seed string."""
    hash_bytes = hashlib.md5(seed.encode()).digest()
    return str(uuid.UUID(bytes=hash_bytes))


def _parse_date(raw) -> Optional[str]:
    """Normalise a date value to YYYY-MM-DD; return the raw string if unparseable."""
    if raw is None:
        return None
    try:
        return pd.to_datetime(str(raw)).strftime("%Y-%m-%d")
    except Exception:
        return str(raw)


def _extract_pdf_text(pdf_path: Path) -> str:
    """Extract page text from a PDF using pdfplumber. Returns concatenated pages."""
    pages: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            if text.strip():
                pages.append(f"[Page {i}]\n{text}")
    return "\n\n".join(pages)


# ---------------------------------------------------------------------------
# Main Public Functions
# ---------------------------------------------------------------------------

def detect_document_type(filename: str, text: str) -> Optional[str]:
    """
    Return 'financial_statements', 'lp_statement', or 'transparency_report'.

    Detection order: filename keywords → content keywords (first 3 000 chars).
    Returns None when the type cannot be determined.
    """
    name = filename.lower()

    # --- Filename-based detection (faster, more reliable) ---
    if any(kw in name for kw in ("transparency", "holdings_letter")):
        return "transparency_report"
    if any(kw in name for kw in ("lp_statement", "capital_account", "lp statement")):
        return "lp_statement"
    if any(kw in name for kw in ("financial_statement", "quarterly_report",
                                  "schedule_of_investments", "10-k")):
        return "financial_statements"

    # --- Content-based detection ---
    snippet = text[:3_000].lower()
    if "schedule of investments" in snippet or "portfolio investments" in snippet:
        return "financial_statements"
    if "capital account" in snippet or "limited partner statement" in snippet:
        return "lp_statement"
    if "transparency report" in snippet or (
        "holdings" in snippet and "% of nav" in snippet
    ):
        return "transparency_report"

    return None


def extract_with_claude(text: str, doc_type: str) -> Optional[dict]:
    """
    Send extracted PDF text to Claude Haiku with the appropriate structured prompt.

    Strips markdown code fences from the response before parsing JSON.
    Returns a parsed dict on success, None on any error.
    """
    prompt = PROMPTS[doc_type]

    if len(text) > MAX_TEXT_CHARS:
        logger.warning(
            "Truncating PDF text from %d to %d chars before sending to Claude",
            len(text), MAX_TEXT_CHARS,
        )
        text = text[:MAX_TEXT_CHARS]

    client = anthropic.Anthropic()
    try:
        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4_096,
            messages=[{"role": "user", "content": prompt + text}],
        )
        raw = message.content[0].text.strip()

        # Strip markdown code fences if Claude wraps the JSON
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw.strip())

        return json.loads(raw)

    except json.JSONDecodeError as exc:
        logger.error("JSON parse error from Claude response: %s", exc)
        return None
    except anthropic.APIError as exc:
        logger.error("Claude API error: %s", exc)
        return None


def _pg_upsert(session, model, records: list[dict], key_columns: list[str]) -> int:
    """Execute a PostgreSQL INSERT ... ON CONFLICT DO UPDATE within an existing session.

    Does NOT commit — the caller owns the transaction.
    Returns the number of records passed (rowcount from ON CONFLICT is unreliable).
    """
    if not records:
        return 0
    stmt = pg_insert(model).values(records)
    mapper = sa_inspect(model)
    all_cols = [col.key for col in mapper.columns]
    update_cols = [c for c in all_cols if c not in key_columns]
    update_dict = {col: stmt.excluded[col] for col in update_cols}
    if update_dict:
        stmt = stmt.on_conflict_do_update(index_elements=key_columns, set_=update_dict)
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=key_columns)
    session.execute(stmt)
    return len(records)


def write_to_db(
    extracted_data: dict,
    doc_type: str,
    fund_config: dict,
) -> tuple[int, int]:
    """
    Upsert one document's extracted data into dim_fund, fact_fund_report,
    dim_company (stubs), and fact_reported_holding — all in a single session
    with one commit at the end.

    Returns (reports_written, holdings_written).
    """
    fund_name = fund_config.get("fund_name", fund_config["folder"])
    fund_id = _make_uuid(f"pdf_fund_{fund_name}")
    reporting_date = _parse_date(extracted_data.get("reporting_date"))
    fund_report_id = _make_uuid(f"pdf_report_{fund_name}_{reporting_date}")

    logger.info(
        "  write_to_db: fund=%s  doc_type=%s  date=%s",
        fund_name, doc_type, reporting_date,
    )

    # ------------------------------------------------------------------ #
    # Build all records before opening the session so any Python errors  #
    # are caught before we touch the DB.                                  #
    # ------------------------------------------------------------------ #

    fund_record = {
        "fund_id": fund_id,
        "fund_name": fund_name,
        "manager_name": None,
        "fund_type": fund_config["fund_type"],
        "strategy": None,
        "vintage_year": None,
        "base_currency": "USD",
        "source": SOURCE,
    }

    is_lp = doc_type == "lp_statement"
    holdings_raw: list[dict] = extracted_data.get("holdings", [])

    fund_report = {
        "fund_report_id": fund_report_id,
        "fund_id": fund_id,
        "report_period_end": reporting_date,
        "received_date": None,
        "document_id": None,
        "coverage_estimate": 1.0 if holdings_raw else None,
        "nav_usd": extracted_data.get("nav_usd") if doc_type != "financial_statements" else None,
        "lp_name": extracted_data.get("lp_name") if is_lp else None,
        "irr_pct": extracted_data.get("irr_pct") if is_lp else None,
        "moic": extracted_data.get("moic") if is_lp else None,
        "contributions_usd": extracted_data.get("contributions_usd") if is_lp else None,
        "distributions_usd": extracted_data.get("distributions_usd") if is_lp else None,
        "unfunded_commitment_usd": extracted_data.get("unfunded_commitment_usd") if is_lp else None,
        "source": SOURCE,
    }

    if is_lp:
        logger.info(
            "  LP metrics — NAV: $%.0f  IRR: %.1f%%  MOIC: %.2fx  "
            "Contributions: $%.0f  Distributions: $%.0f  Unfunded: $%.0f",
            extracted_data.get("nav_usd") or 0,
            extracted_data.get("irr_pct") or 0,
            extracted_data.get("moic") or 0,
            extracted_data.get("contributions_usd") or 0,
            extracted_data.get("distributions_usd") or 0,
            extracted_data.get("unfunded_commitment_usd") or 0,
        )

    holding_records: list[dict] = []
    for row_num, h in enumerate(holdings_raw, start=1):
        company_name = (h.get("company_name") or "").strip()
        if not company_name:
            continue
        holding_records.append({
            "reported_holding_id": _make_uuid(
                f"pdf_holding_{fund_report_id}_{row_num}_{company_name}"
            ),
            "fund_report_id": fund_report_id,
            "company_id": None,
            "raw_company_name": company_name,
            "reported_sector": h.get("sector"),
            "reported_country": None,
            "reported_value_usd": h.get("fair_value_usd"),
            "reported_pct_nav": h.get("pct_nav"),
            "cusip": None,
            "extraction_method": "claude_haiku_pdf",
            "extraction_confidence": EXTRACTION_CONFIDENCE,
            "document_id": None,
            "page_number": None,
            "row_number": float(row_num),
            "source": SOURCE,
            "as_of_date": reporting_date,
            "cost_basis_usd": h.get("cost_basis_usd"),
            "ownership_pct": h.get("ownership_pct"),
        })

    logger.info("  Built %d holding records", len(holding_records))

    # dim_company stubs — check existing names before opening the write session
    existing_df = get_all(DimCompany)
    existing_lower: set[str] = (
        set(existing_df["company_name"].str.lower().dropna())
        if not existing_df.empty
        else set()
    )
    seen: set[str] = set()
    new_companies: list[dict] = []
    for rec in holding_records:
        name = rec["raw_company_name"]
        name_lower = name.lower()
        if name_lower in existing_lower or name_lower in seen:
            continue
        seen.add(name_lower)
        sector = next(
            (h.get("sector") for h in holdings_raw
             if (h.get("company_name") or "").strip() == name),
            None,
        )
        new_companies.append({
            "company_id": _make_uuid(f"pdf_company_{name}"),
            "company_name": name,
            "primary_sector": sector,
            "primary_industry": None,
            "primary_country": None,
            "industry_taxonomy_node_id": None,
            "country_taxonomy_node_id": None,
            "website": None,
            "created_at": pd.Timestamp.now().date().isoformat(),
            "source": SOURCE,
        })

    # ------------------------------------------------------------------ #
    # Single session — all four tables, one commit.                       #
    # get_session_context() commits on clean exit, rolls back on error.   #
    # ------------------------------------------------------------------ #
    with get_session_context() as session:
        _pg_upsert(session, DimFund, [fund_record], ["fund_id"])
        logger.info("  → dim_fund: 1 row")

        _pg_upsert(session, FactFundReport, [fund_report], ["fund_report_id"])
        logger.info("  → fact_fund_report: 1 row")

        if new_companies:
            _pg_upsert(session, DimCompany, new_companies, ["company_id"])
            logger.info("  → dim_company: %d new stubs", len(new_companies))
        else:
            logger.info("  → dim_company: no new stubs")

        if holding_records:
            _pg_upsert(session, FactReportedHolding, holding_records, ["reported_holding_id"])
            logger.info("  → fact_reported_holding: %d rows", len(holding_records))
        else:
            logger.info("  → fact_reported_holding: no holdings")

    # session.commit() is called by get_session_context on clean exit
    logger.info("  Committed. (fund=%s  date=%s)", fund_name, reporting_date)
    return 1, len(holding_records)


def ingest_fund_folder(
    folder_path: Path,
    fund_config: dict,
    db_mode: bool = True,
    manifest: Optional[dict] = None,
    force: bool = False,
) -> tuple[list[dict], list[dict], list[dict], dict]:
    """
    Process all PDFs in one fund's subfolder.

    In DB mode calls write_to_db() per document.
    In CSV mode accumulates and returns records for batch writing.

    Returns (fund_records, report_records, holding_records, stats) where
    stats = {'financial_statements': N, 'lp_statement': N, 'transparency_report': N,
             'holdings': N, 'skipped': N, 'errors': N}.
    """
    if manifest is None:
        manifest = {}

    fund_name = fund_config.get("fund_name", fund_config["folder"])
    fund_id = _make_uuid(f"pdf_fund_{fund_name}")

    fund_record = {
        "fund_id": fund_id,
        "fund_name": fund_name,
        "manager_name": None,
        "fund_type": fund_config["fund_type"],
        "strategy": None,
        "vintage_year": None,
        "base_currency": "USD",
        "source": SOURCE,
    }

    stats: dict[str, int] = {
        "financial_statements": 0,
        "lp_statement": 0,
        "transparency_report": 0,
        "holdings": 0,
        "skipped": 0,
        "errors": 0,
    }

    all_reports: list[dict] = []
    all_holdings: list[dict] = []

    pdf_files = sorted(folder_path.glob("*.pdf"))
    if not pdf_files:
        logger.warning("No PDF files found in %s", folder_path)
        return [fund_record], [], [], stats

    for pdf_path in pdf_files:
        # --- Hash-based deduplication ---
        fhash = _file_hash(pdf_path)
        if fhash in manifest:
            if not force:
                logger.info(
                    "Skipping %s (already ingested on %s)",
                    pdf_path.name, manifest[fhash].get("ingested_at", "?"),
                )
                stats["skipped"] += 1
                continue
            else:
                logger.info(
                    "  --force: re-ingesting %s (previously ingested on %s)",
                    pdf_path.name, manifest[fhash].get("ingested_at", "?"),
                )

        logger.info("Processing: %s", pdf_path.name)

        # --- Extract text ---
        try:
            text = _extract_pdf_text(pdf_path)
        except Exception as exc:
            logger.error("pdfplumber failed on %s: %s", pdf_path.name, exc)
            stats["errors"] += 1
            continue

        if not text.strip():
            logger.warning("No text extracted from %s — skipping", pdf_path.name)
            stats["errors"] += 1
            continue

        # --- Detect doc type ---
        doc_type = detect_document_type(pdf_path.name, text)
        if not doc_type:
            logger.warning(
                "Cannot detect doc type for %s — skipping", pdf_path.name
            )
            stats["errors"] += 1
            continue

        if doc_type not in fund_config["doc_types"]:
            logger.info(
                "Doc type '%s' not expected for %s — skipping",
                doc_type, fund_name,
            )
            stats["skipped"] += 1
            continue

        logger.info("  Detected: %s", doc_type)

        # --- AI extraction ---
        extracted = extract_with_claude(text, doc_type)
        if not extracted:
            logger.error("Extraction failed for %s", pdf_path.name)
            stats["errors"] += 1
            continue

        n_holdings = len(extracted.get("holdings", []))
        logger.info(
            "  Extracted: date=%s  holdings=%d",
            extracted.get("reporting_date"), n_holdings,
        )

        # --- Persist ---
        if db_mode:
            _, n_written = write_to_db(extracted, doc_type, fund_config)
            stats[doc_type] += 1
            stats["holdings"] += n_written
        else:
            # Accumulate for batch CSV write
            reporting_date = _parse_date(extracted.get("reporting_date"))
            fund_report_id = _make_uuid(
                f"pdf_report_{fund_name}_{reporting_date}"
            )
            nav_usd = (
                extracted.get("nav_usd")
                if doc_type != "financial_statements"
                else None
            )
            _is_lp = doc_type == "lp_statement"
            all_reports.append({
                "fund_report_id": fund_report_id,
                "fund_id": fund_id,
                "report_period_end": reporting_date,
                "received_date": None,
                "document_id": None,
                "coverage_estimate": 1.0 if n_holdings else None,
                "nav_usd": nav_usd,
                "lp_name": extracted.get("lp_name") if _is_lp else None,
                "irr_pct": extracted.get("irr_pct") if _is_lp else None,
                "moic": extracted.get("moic") if _is_lp else None,
                "contributions_usd": extracted.get("contributions_usd") if _is_lp else None,
                "distributions_usd": extracted.get("distributions_usd") if _is_lp else None,
                "unfunded_commitment_usd": extracted.get("unfunded_commitment_usd") if _is_lp else None,
                "source": SOURCE,
            })
            for row_num, h in enumerate(extracted.get("holdings", []), start=1):
                company_name = (h.get("company_name") or "").strip()
                if not company_name:
                    continue
                all_holdings.append({
                    "reported_holding_id": _make_uuid(
                        f"pdf_holding_{fund_report_id}_{row_num}_{company_name}"
                    ),
                    "fund_report_id": fund_report_id,
                    "company_id": None,
                    "raw_company_name": company_name,
                    "reported_sector": h.get("sector"),
                    "reported_country": None,
                    "reported_value_usd": h.get("fair_value_usd"),
                    "reported_pct_nav": h.get("pct_nav"),
                    "cusip": None,
                    "extraction_method": "claude_haiku_pdf",
                    "extraction_confidence": EXTRACTION_CONFIDENCE,
                    "document_id": None,
                    "page_number": None,
                    "row_number": float(row_num),
                    "source": SOURCE,
                    "as_of_date": reporting_date,
                    "cost_basis_usd": h.get("cost_basis_usd"),
                    "ownership_pct": h.get("ownership_pct"),
                })
            stats[doc_type] += 1
            stats["holdings"] += n_holdings

        # Mark as ingested
        manifest[fhash] = {
            "filename": pdf_path.name,
            "fund": fund_name,
            "doc_type": doc_type,
            "ingested_at": pd.Timestamp.now().isoformat(),
        }

    return [fund_record], all_reports, all_holdings, stats


def ingest_all_funds(
    base_path: Path,
    db_mode: bool = True,
    force: bool = False,
    fund_filter: Optional[str] = None,
) -> None:
    """
    Loop through FUND_CONFIG, process each fund's PDF folder, and persist results.

    After processing configured funds, scans base_path for unknown subfolders and
    processes them with an inferred config (fund_type="unknown", all doc_types tried).

    Args:
        fund_filter: Case-insensitive substring matched against folder names.
                     When set, only matching folders are processed.

    Prints a per-fund summary, manifest stats, and a grand total.
    Saves the ingestion manifest after all funds are processed.
    """
    if db_mode:
        ensure_tables()
        _run_migrations()

    manifest = _load_manifest()
    manifest_size_before = len(manifest)

    all_funds: list[dict] = []
    all_reports: list[dict] = []
    all_holdings: list[dict] = []

    # Track per-run counters across all funds
    run_totals: dict[str, int] = {
        "financial_statements": 0,
        "lp_statement": 0,
        "transparency_report": 0,
        "holdings": 0,
        "skipped": 0,
        "errors": 0,
    }

    # --- Build the work list: configured funds + auto-discovered unknowns ---
    known_folders = {cfg["folder"] for cfg in FUND_CONFIG}
    work_list: list[dict] = list(FUND_CONFIG)

    if base_path.exists():
        for subfolder in sorted(base_path.iterdir()):
            if not subfolder.is_dir():
                continue
            if subfolder.name in known_folders:
                continue
            logger.warning(
                "Unknown fund folder found: %s — inferring config", subfolder.name
            )
            work_list.append({
                "folder": subfolder.name,
                "fund_type": "unknown",
                "doc_types": ["financial_statements", "lp_statement", "transparency_report"],
                "_auto_discovered": True,
            })

    configured_count = len(FUND_CONFIG)
    discovered_count = len(work_list) - configured_count

    # --- Apply --fund filter (case-insensitive substring match) ---
    if fund_filter:
        needle = fund_filter.lower()
        filtered = [c for c in work_list if needle in c["folder"].lower()]
        if not filtered:
            print(f"ERROR: --fund '{fund_filter}' matched no folders.")
            print("Known folders:")
            for cfg in work_list:
                print(f"  {cfg['folder']}")
            return
        work_list = filtered

    # --- Process every fund in the work list ---
    for fund_cfg in work_list:
        folder = base_path / fund_cfg["folder"]
        if not folder.exists():
            logger.warning("Folder not found — skipping: %s", folder)
            continue

        funds, reports, holdings, stats = ingest_fund_folder(
            folder, fund_cfg, db_mode=db_mode, manifest=manifest, force=force
        )

        all_funds.extend(funds)
        all_reports.extend(reports)
        all_holdings.extend(holdings)

        for key in run_totals:
            run_totals[key] += stats.get(key, 0)

        # Per-fund summary
        label = fund_cfg.get("fund_name", fund_cfg["folder"])
        if fund_cfg.get("_auto_discovered"):
            label += " [auto]"
        parts = []
        for dt in ("financial_statements", "lp_statement", "transparency_report"):
            if stats[dt]:
                parts.append(f"{stats[dt]} {dt.replace('_', ' ')}")
        doc_summary = ", ".join(parts) if parts else "0 documents"
        print(
            f"  {label}: {doc_summary}"
            f"  |  {stats['holdings']} holdings"
            f"  |  {stats['skipped']} skipped"
            + (f"  |  {stats['errors']} errors" if stats["errors"] else "")
        )

    _save_manifest(manifest)

    if not db_mode and (all_reports or all_holdings):
        _write_to_csv(all_funds, all_reports, all_holdings)

    # --- Manifest summary ---
    manifest_size_after = len(manifest)
    new_this_run = manifest_size_after - manifest_size_before
    print(
        f"\nManifest: {manifest_size_after} total PDFs tracked"
        f"  |  {new_this_run} new this run"
        f"  |  {run_totals['skipped']} skipped (already ingested)"
        f"  →  {MANIFEST_PATH}"
    )
    print(
        f"Folders : {configured_count} configured"
        + (f"  |  {discovered_count} auto-discovered" if discovered_count else "")
    )

    # --- Grand total ---
    print(
        f"Totals  : {run_totals['financial_statements']} financial statements"
        f"  |  {run_totals['lp_statement']} LP statements"
        f"  |  {run_totals['transparency_report']} transparency reports"
        f"  |  {run_totals['holdings']} holdings"
        + (f"  |  {run_totals['errors']} errors" if run_totals["errors"] else "")
    )


# ---------------------------------------------------------------------------
# CSV Write Helper
# ---------------------------------------------------------------------------

def _write_to_csv(
    funds: list[dict],
    reports: list[dict],
    holdings: list[dict],
) -> None:
    """Write accumulated records to CSV files in data/silver/."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    funds_df = pd.DataFrame(funds).drop_duplicates(subset=["fund_id"])
    reports_df = pd.DataFrame(reports).drop_duplicates(subset=["fund_report_id"])
    holdings_df = (
        pd.DataFrame(holdings).drop_duplicates(subset=["reported_holding_id"])
        if holdings
        else pd.DataFrame()
    )

    # Company stubs
    new_companies: list[dict] = []
    seen: set[str] = set()
    for h in holdings:
        name = (h.get("raw_company_name") or "").strip()
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        new_companies.append({
            "company_id": _make_uuid(f"pdf_company_{name}"),
            "company_name": name,
            "primary_sector": h.get("reported_sector"),
            "primary_industry": None,
            "primary_country": None,
            "industry_taxonomy_node_id": None,
            "country_taxonomy_node_id": None,
            "website": None,
            "created_at": pd.Timestamp.now().date().isoformat(),
            "source": SOURCE,
        })
    companies_df = pd.DataFrame(new_companies)

    for df, name in [
        (companies_df, "pdf_dim_company.csv"),
        (funds_df, "pdf_dim_fund.csv"),
        (reports_df, "pdf_fact_fund_report.csv"),
        (holdings_df, "pdf_fact_reported_holding.csv"),
    ]:
        if df.empty:
            continue
        path = SILVER_DIR / name
        df.to_csv(path, index=False)
        print(f"  Wrote {len(df)} rows → {path}")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest PDF fund documents (financial statements, LP statements, "
            "transparency reports) into the LookThrough platform."
        )
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Write output to CSV files instead of PostgreSQL",
    )
    parser.add_argument(
        "--folder",
        type=str,
        default=str(DEFAULT_BASE_FOLDER),
        help=f"Base folder containing fund subfolders (default: {DEFAULT_BASE_FOLDER})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-ingest all PDFs, ignoring the deduplication manifest",
    )
    parser.add_argument(
        "--fund",
        type=str,
        default=None,
        metavar="NAME",
        help=(
            "Case-insensitive substring filter — process only the matching fund folder. "
            'Example: --fund "foundry" matches "Foundry Seed Fund I"'
        ),
    )
    args = parser.parse_args()

    csv_mode = args.csv or _is_csv_mode()
    base_folder = Path(args.folder)

    if not base_folder.exists():
        print(f"ERROR: Base folder does not exist: {base_folder}")
        return

    print("=" * 60)
    print("PDF Fund Document Ingestion")
    print("=" * 60)
    print(f"Base folder : {base_folder}")
    print(f"Fund filter : {args.fund or '(all)'}")
    print(f"Output mode : {'CSV' if csv_mode else 'PostgreSQL'}")
    print(f"Force re-run: {args.force}")
    print()

    ingest_all_funds(base_folder, db_mode=not csv_mode, force=args.force, fund_filter=args.fund)

    print("\nDone.")


if __name__ == "__main__":
    main()
