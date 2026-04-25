from __future__ import annotations

import argparse
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

from anthropic import Anthropic, RateLimitError, transform_schema

from src.lookthrough.db.repository import (
    _is_csv_mode,
    get_all,
    upsert_rows,
)
from src.lookthrough.db.models import (
    DimCompany,
    DimTaxonomyNode,
    FactExposureClassification,
)


# ----------------------------
# Schema for structured output
# ----------------------------
class CompanyClassificationOut(BaseModel):
    taxonomy_type: str = Field(..., description="One of: sector, industry, geography")
    node_name: Optional[str] = Field(
        None, description="Must be one of allowed_nodes; null if cannot classify"
    )
    confidence: float = Field(..., description="Float in [0,1]")
    rationale: str = Field(..., description="1-3 sentences")
    assumptions: list[str] = Field(default_factory=list)


INSTRUMENT_RULES: list[tuple[list[str], str, str]] = [
    # Crypto/Digital Assets
    (["bitcoin", "ethereum", "ether", "crypto", "blockchain", "btc", "eth",
      "digital asset", "defi", "web3"], "Financials", "Capital Markets"),
    # ETFs and Index Funds
    (["etf", "ishares", "vanguard etf", "spdr", "invesco", "proshares",
      "direxion", "wisdom tree"], "Financials", "Capital Markets"),
    # SPACs and Acquisition vehicles
    (["spac", "acquisition corp", "blank check"], "Financials", "Capital Markets"),
    # Warrants and Options
    (["warrant", "rights", "call option"], "Financials", "Capital Markets"),
    # Government/Treasury
    (["treasury", "t-bill", "t-bond", "tbill", "us government"],
     "Financials", "Capital Markets"),
]


def _check_instrument_rules(company_name: str) -> Optional[tuple[str, str]]:
    """Return (sector, industry) if name matches an instrument rule, else None."""
    name_lower = company_name.lower()
    for keywords, sector, industry in INSTRUMENT_RULES:
        if any(kw in name_lower for kw in keywords):
            return sector, industry
    return None


# Strict ISO-3166-1 alpha-2 pattern: exactly two uppercase letters
_ISO2_RE = re.compile(r"^[A-Z]{2}$")


@dataclass(frozen=True)
class ClassifierConfig:
    # Default to Haiku — 20x cheaper than Sonnet, fully capable for structured classification.
    # Override with ANTHROPIC_MODEL env var if Sonnet or another model is needed.
    model: str = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
    max_tokens: int = 512
    prompt_version: str = "v1"
    confidence_threshold: float = 0.70  # below this should go to review queue later


def _repo_root() -> Path:
    # src/lookthrough/ai/classify_companies.py -> repo root is 4 parents up
    return Path(__file__).resolve().parents[3]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _estimate_tokens(n_nodes: int, company_name: str, company_description: Optional[str]) -> int:
    """Rough token estimate for a classify_one() API call."""
    base = 100  # prompt text + taxonomy_type line + JSON framing
    name_tokens = max(1, len(company_name) // 4)
    desc_tokens = max(0, len(company_description or "") // 4)
    node_tokens = n_nodes * 7  # ~7 tokens per node name on average
    return base + name_tokens + desc_tokens + node_tokens


def _build_sector_to_industry_map(taxonomy: pd.DataFrame) -> dict[str, list[str]]:
    """Build mapping: sector node_name -> list of child industry node_names.

    Links industry nodes to sectors via parent_node_id. BDC-sourced industry
    nodes (which have null parent_node_id) are excluded automatically.
    """
    sector_nodes = taxonomy[taxonomy["taxonomy_type"] == "sector"].copy()
    industry_nodes = taxonomy[taxonomy["taxonomy_type"] == "industry"].copy()

    sector_id_to_name: dict[str, str] = {
        str(row["taxonomy_node_id"]): str(row["node_name"])
        for _, row in sector_nodes.iterrows()
    }

    result: dict[str, list[str]] = {name: [] for name in sector_id_to_name.values()}
    for _, row in industry_nodes.iterrows():
        parent_id = row.get("parent_node_id")
        if pd.isna(parent_id):
            continue  # BDC-sourced nodes have no sector parent — skip
        sector_name = sector_id_to_name.get(str(parent_id))
        if sector_name:
            result[sector_name].append(str(row["node_name"]))

    return {k: sorted(v) for k, v in result.items()}


def classify_one(
    client: Anthropic,
    cfg: ClassifierConfig,
    prompt_text: str,
    taxonomy_type: str,
    allowed_nodes: list[str],
    company_name: str,
    company_country: Optional[str],
    company_description: Optional[str],
) -> CompanyClassificationOut:
    user_payload = {
        "company_name": company_name,
        "company_country": company_country,
        "company_description": company_description,
        "allowed_nodes": allowed_nodes,
    }

    # Structured outputs: force JSON that matches our schema.
    response = client.messages.create(
        model=cfg.model,
        max_tokens=cfg.max_tokens,
        messages=[
            {
                "role": "user",
                "content": prompt_text
                + "\n\n"
                + "taxonomy_type: "
                + taxonomy_type
                + "\n"
                + "Return JSON matching the required schema.\n\n"
                + json.dumps(user_payload, ensure_ascii=False),
            }
        ],
        output_config={
            "format": {
                "type": "json_schema",
                "schema": transform_schema(CompanyClassificationOut),
            }
        },
    )

    raw = response.content[0].text
    try:
        parsed = CompanyClassificationOut.model_validate_json(raw)
    except ValidationError as e:
        # If schema validation fails (should be rare with structured outputs), fallback safely
        return CompanyClassificationOut(
            taxonomy_type=taxonomy_type,
            node_name=None,
            confidence=0.0,
            rationale=f"ValidationError: {str(e)[:200]}",
            assumptions=[],
        )

    # Post-validate
    parsed.confidence = _clamp01(parsed.confidence)
    if parsed.node_name is not None and parsed.node_name not in allowed_nodes:
        # Disallow hallucinated nodes
        parsed.node_name = None
        parsed.confidence = 0.0
        parsed.rationale = "Returned node_name was not in allowed_nodes; set to null."
    return parsed


def _classify_one_with_retry(
    client: Anthropic,
    cfg: ClassifierConfig,
    prompt_text: str,
    taxonomy_type: str,
    allowed_nodes: list[str],
    company_name: str,
    company_country: Optional[str],
    company_description: Optional[str],
) -> CompanyClassificationOut:
    """Wrapper around classify_one that retries once on RateLimitError."""
    try:
        return classify_one(
            client=client,
            cfg=cfg,
            prompt_text=prompt_text,
            taxonomy_type=taxonomy_type,
            allowed_nodes=allowed_nodes,
            company_name=company_name,
            company_country=company_country,
            company_description=company_description,
        )
    except RateLimitError:
        print("Rate limited — waiting 60s and retrying")
        time.sleep(60)
        return classify_one(
            client=client,
            cfg=cfg,
            prompt_text=prompt_text,
            taxonomy_type=taxonomy_type,
            allowed_nodes=allowed_nodes,
            company_name=company_name,
            company_country=company_country,
            company_description=company_description,
        )


def _make_row(
    run_id: str,
    company_id: Optional[str],
    company_name: str,
    result: CompanyClassificationOut,
    taxonomy_node_id: str,
    model: str,
    prompt_version: str,
) -> dict:
    return {
        "classification_id": str(uuid.uuid4()),
        "run_id": run_id,
        "company_id": company_id,
        "raw_company_name": company_name,
        "taxonomy_type": result.taxonomy_type,
        "taxonomy_node_id": taxonomy_node_id,
        "confidence": result.confidence,
        "rationale": result.rationale,
        "assumptions_json": json.dumps(result.assumptions, ensure_ascii=False),
        "model": model,
        "prompt_version": prompt_version,
    }


def _lookup_node_id(taxonomy: pd.DataFrame, taxonomy_type: str, node_name: str) -> str:
    """Return the taxonomy_node_id for a given type+name, or the zero UUID if not found."""
    match = taxonomy[
        (taxonomy["taxonomy_type"] == taxonomy_type)
        & (taxonomy["node_name"].astype(str) == node_name)
    ]
    if len(match):
        return str(match.iloc[0]["taxonomy_node_id"])
    return "00000000-0000-0000-0000-000000000000"


def _classify_country_one(client: Anthropic, cfg: ClassifierConfig, company_name: str) -> Optional[str]:
    """Ask Haiku for the ISO-2 country code of a single company.

    Returns a valid 2-letter code (e.g. 'US') or None if the model reports it
    cannot determine the country, or if the response is not a bare 2-letter code.
    Raises RateLimitError so the retry wrapper can handle backoff.
    """
    prompt = (
        f"What country is '{company_name}' headquartered or domiciled in? "
        "Return ONLY the ISO 2-letter country code (e.g. US, GB, DE, JP, CN). "
        "If this is an opaque holding company, SPAC, or you cannot determine "
        "the country with confidence, return NULL. "
        "Do not explain. Return only the code or NULL."
    )
    response = client.messages.create(
        model=cfg.model,
        max_tokens=10,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip().upper()
    if raw in ("NULL", "NONE", "N/A", ""):
        return None
    if _ISO2_RE.match(raw):
        return raw
    return None  # reject anything that isn't exactly a 2-letter code


def _classify_country_with_retry(
    client: Anthropic, cfg: ClassifierConfig, company_name: str
) -> Optional[str]:
    """Wrapper around _classify_country_one that retries once on RateLimitError."""
    try:
        return _classify_country_one(client, cfg, company_name)
    except RateLimitError:
        print("Rate limited — waiting 60s and retrying")
        time.sleep(60)
        return _classify_country_one(client, cfg, company_name)


def _update_dim_company_country(country_updates: dict[str, str]) -> None:
    """Write primary_country values back to dim_company in PostgreSQL.

    Loads the full company records first so all other columns are preserved
    through the upsert (same pattern as _update_dim_company_from_classifications).
    """
    all_companies = get_all(DimCompany)
    if all_companies.empty:
        return

    records = []
    for _, row in all_companies.iterrows():
        cid = str(row["company_id"])
        if cid not in country_updates:
            continue
        record = row.to_dict()
        record["primary_country"] = country_updates[cid]
        records.append(record)

    if records:
        upsert_rows(DimCompany, records, ["company_id"])
        print(f"Updated {len(records)} dim_company rows (primary_country)")
    else:
        print("No dim_company rows needed updating for country.")


def classify_countries(
    client: Anthropic,
    cfg: ClassifierConfig,
    companies: pd.DataFrame,
    csv_mode: bool,
    silver_path: Path,
    limit: int,
) -> None:
    """Classify primary_country for companies where it is NULL.

    Rules applied in priority order (no API call for rules 1–2):
      1. source = '13f_filing' → 'US'  (13F only covers US-listed securities)
      2. Instrument rule match → skip  (financial instruments, not operating companies)
      3. Haiku free-text prompt → ISO-2 code or None

    Updates dim_company.primary_country in-place (CSV or PostgreSQL).
    """
    # Filter to companies with null/blank primary_country
    if "primary_country" in companies.columns:
        null_mask = companies["primary_country"].isna() | (
            companies["primary_country"].astype(str).str.strip() == ""
        )
    else:
        null_mask = pd.Series([True] * len(companies), index=companies.index)

    to_classify = companies[null_mask].copy()

    if to_classify.empty:
        print("Country classification: no companies with null primary_country.")
        return

    source_col = "source" if "source" in to_classify.columns else None
    country_updates: dict[str, str] = {}
    rule_count = 0
    skipped_instrument = 0

    # --- Rule 1: 13F-sourced companies are always US-listed ---
    if source_col:
        mask_13f = to_classify[source_col].astype(str) == "13f_filing"
        for _, row in to_classify[mask_13f].iterrows():
            country_updates[str(row["company_id"])] = "US"
            rule_count += 1
        to_classify = to_classify[~mask_13f].copy()

    print(
        f"Country classification: {rule_count} set to US via 13F rule, "
        f"{len(to_classify)} remaining for AI (capped at {limit})"
    )

    # Cap API calls at limit
    to_classify = to_classify.head(limit).copy()
    total = len(to_classify)
    api_count = 0

    for i, (_, c) in enumerate(to_classify.iterrows(), start=1):
        company_id = str(c["company_id"])
        company_name = str(c.get("company_name", c.get("raw_company_name", "")))

        # --- Rule 2: Skip instrument rule matches ---
        if _check_instrument_rules(company_name) is not None:
            skipped_instrument += 1
            continue

        # --- Rule 3: Haiku API call ---
        country_code = _classify_country_with_retry(client, cfg, company_name)
        api_count += 1

        if country_code:
            country_updates[company_id] = country_code
            print(f"Country classified [{i}/{total}] {company_name} → {country_code}")
        else:
            print(f"Country unclassifiable [{i}/{total}] {company_name}")

        if i < total:
            time.sleep(0.3)

    print(
        f"Country classification complete: {rule_count} via 13F rule, "
        f"{api_count} API calls, {skipped_instrument} instrument skips, "
        f"{len(country_updates)} total updates"
    )

    if not country_updates:
        print("No country updates to write.")
        return

    # --- Write back to dim_company ---
    if csv_mode:
        dim_path = silver_path / "dim_company.csv"
        if not dim_path.exists():
            print(f"Warning: {dim_path} not found — cannot write country updates.")
            return
        df = pd.read_csv(dim_path)
        for cid, code in country_updates.items():
            df.loc[df["company_id"].astype(str) == cid, "primary_country"] = code
        df.to_csv(dim_path, index=False)
        print(f"Wrote {len(country_updates)} country updates to {dim_path}")
    else:
        _update_dim_company_country(country_updates)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20, help="Max companies to classify (V1 safety).")
    parser.add_argument(
        "--taxonomy-type",
        type=str,
        default="industry",
        choices=["sector", "industry", "geography", "country"],
        help="Taxonomy type to classify.",
    )
    parser.add_argument("--csv", action="store_true", help="Use CSV mode instead of PostgreSQL")
    args = parser.parse_args()

    csv_mode = args.csv or _is_csv_mode()

    cfg = ClassifierConfig()
    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    prompt_path = root / "src" / "lookthrough" / "ai" / "prompts" / "company_classification.md"
    prompt_text = _read_text(prompt_path)

    # -----------------------------------------------------------------------
    # Load inputs
    # -----------------------------------------------------------------------
    if csv_mode:
        companies = _read_csv(silver / "dim_company.csv")
        taxonomy = _read_csv(silver / "dim_taxonomy_node.csv")
    else:
        companies = get_all(DimCompany)
        taxonomy = get_all(DimTaxonomyNode)

        # In PostgreSQL mode, only classify companies that are missing the
        # relevant taxonomy node ID — avoids re-classifying already-known companies.
        if args.taxonomy_type == "industry":
            unclassified_mask = (
                companies["industry_taxonomy_node_id"].isna()
                | (companies["industry_taxonomy_node_id"].astype(str) == "")
            )
        elif args.taxonomy_type == "geography":
            unclassified_mask = (
                companies["country_taxonomy_node_id"].isna()
                | (companies["country_taxonomy_node_id"].astype(str) == "")
            )
        elif args.taxonomy_type == "country":
            unclassified_mask = (
                companies["primary_country"].isna()
                | (companies["primary_country"].astype(str).str.strip() == "")
            )
        else:  # sector
            unclassified_mask = (
                companies["primary_sector"].isna()
                | (companies["primary_sector"].astype(str).str.strip() == "")
            )

        companies = companies[unclassified_mask].copy()
        print(f"Found {len(companies)} unclassified companies (taxonomy_type={args.taxonomy_type})")

    # Country classification diverges here: no taxonomy nodes, writes directly to dim_company
    if args.taxonomy_type == "country":
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        classify_countries(
            client=client,
            cfg=cfg,
            companies=companies,
            csv_mode=csv_mode,
            silver_path=silver,
            limit=args.limit,
        )
        return

    # Build allowed node list for the requested taxonomy type (used in single-step path)
    tax = taxonomy[taxonomy["taxonomy_type"] == args.taxonomy_type].copy()
    allowed_nodes = sorted(tax["node_name"].astype(str).unique().tolist())

    # Cap at limit
    companies = companies.head(args.limit).copy()

    # -----------------------------------------------------------------------
    # Load existing classifications to skip already-classified companies
    # -----------------------------------------------------------------------
    already_classified: set[tuple[str, str]] = set()
    # Maps company_id -> sector node_name from a prior run (used in two-step industry mode)
    existing_sector_for_company: dict[str, str] = {}

    if csv_mode:
        out_path = gold / "fact_exposure_classification.csv"
        existing_df: Optional[pd.DataFrame] = None
        if out_path.exists():
            existing_df = pd.read_csv(out_path)
            for _, row in existing_df.iterrows():
                already_classified.add((str(row["company_id"]), str(row["taxonomy_type"])))
            print(f"Loaded {len(existing_df)} existing classifications from {out_path}")

            if args.taxonomy_type == "industry":
                sector_id_to_name = {
                    str(r["taxonomy_node_id"]): str(r["node_name"])
                    for _, r in taxonomy[taxonomy["taxonomy_type"] == "sector"].iterrows()
                }
                for _, row in existing_df[existing_df["taxonomy_type"] == "sector"].iterrows():
                    name = sector_id_to_name.get(str(row["taxonomy_node_id"]))
                    if name:
                        existing_sector_for_company[str(row["company_id"])] = name
    else:
        existing_classifications = get_all(FactExposureClassification)
        if not existing_classifications.empty:
            for _, row in existing_classifications.iterrows():
                already_classified.add((str(row["company_id"]), str(row["taxonomy_type"])))
            print(f"Loaded {len(existing_classifications)} existing classifications from PostgreSQL")

            if args.taxonomy_type == "industry":
                sector_id_to_name = {
                    str(r["taxonomy_node_id"]): str(r["node_name"])
                    for _, r in taxonomy[taxonomy["taxonomy_type"] == "sector"].iterrows()
                }
                sector_class = existing_classifications[
                    existing_classifications["taxonomy_type"] == "sector"
                ]
                for _, row in sector_class.iterrows():
                    name = sector_id_to_name.get(str(row["taxonomy_node_id"]))
                    if name:
                        existing_sector_for_company[str(row["company_id"])] = name
        existing_df = None  # not used in DB mode

    # Optional fields present in some tables
    country_col = "primary_country" if "primary_country" in companies.columns else None
    desc_col = "company_description" if "company_description" in companies.columns else None

    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    run_id = str(uuid.uuid4())
    out_rows: list[dict] = []
    skipped = 0
    total = len(companies)
    total_written = 0

    # Pre-build data structures for two-step industry classification
    sector_to_industry_map: dict[str, list[str]] = {}
    sector_allowed_nodes: list[str] = []
    if args.taxonomy_type == "industry":
        sector_to_industry_map = _build_sector_to_industry_map(taxonomy)
        sector_allowed_nodes = sorted(
            taxonomy[taxonomy["taxonomy_type"] == "sector"]["node_name"].astype(str).unique().tolist()
        )
        industry_counts = {k: len(v) for k, v in sector_to_industry_map.items() if v}
        print(
            f"Two-step industry mode: {len(sector_allowed_nodes)} sector nodes, "
            f"{sum(industry_counts.values())} GICS industry nodes across {len(industry_counts)} sectors"
        )

    # -----------------------------------------------------------------------
    # Classification loop
    # -----------------------------------------------------------------------
    for i, (_, c) in enumerate(companies.iterrows(), start=1):
        company_id = str(c["company_id"]) if "company_id" in companies.columns else None
        company_name = (
            str(c["company_name"]) if "company_name" in companies.columns
            else str(c.get("raw_company_name", ""))
        )
        company_country = (
            str(c[country_col]) if country_col and pd.notna(c[country_col]) else None
        )
        company_desc = (
            str(c[desc_col]) if desc_col and pd.notna(c[desc_col]) else None
        )

        if args.taxonomy_type == "industry":
            # -----------------------------------------------------------------
            # Two-step: sector -> filtered industry
            # -----------------------------------------------------------------
            sector_done = (company_id, "sector") in already_classified
            industry_done = (company_id, "industry") in already_classified

            if sector_done and industry_done:
                skipped += 1
                continue

            # Pre-classification: instrument rules bypass both API calls
            instrument_match = _check_instrument_rules(company_name)
            if instrument_match is not None:
                matched_sector, matched_industry = instrument_match
                print(f"Pre-classified {company_name} as {matched_sector}/{matched_industry} (instrument rule match)")
                if not sector_done:
                    sector_result = CompanyClassificationOut(
                        taxonomy_type="sector",
                        node_name=matched_sector,
                        confidence=1.0,
                        rationale="Pre-classified by instrument rule — no API call.",
                        assumptions=[],
                    )
                    sec_node_id = _lookup_node_id(taxonomy, "sector", matched_sector)
                    out_rows.append(
                        _make_row(run_id, company_id, company_name, sector_result, sec_node_id, "instrument_rule", cfg.prompt_version)
                    )
                if not industry_done:
                    industry_result = CompanyClassificationOut(
                        taxonomy_type="industry",
                        node_name=matched_industry,
                        confidence=1.0,
                        rationale="Pre-classified by instrument rule — no API call.",
                        assumptions=[],
                    )
                    ind_node_id = _lookup_node_id(taxonomy, "industry", matched_industry)
                    out_rows.append(
                        _make_row(run_id, company_id, company_name, industry_result, ind_node_id, "instrument_rule", cfg.prompt_version)
                    )
                continue

            # Step 1 — Sector (skip if already classified)
            sector_name: Optional[str] = None
            if sector_done:
                sector_name = existing_sector_for_company.get(company_id)
                print(f"[{i}/{total}] {company_name} — sector already classified: {sector_name}")
            else:
                n = len(sector_allowed_nodes)
                est = _estimate_tokens(n, company_name, company_desc)
                print(
                    f"Classifying [{i}/{total}]: {company_name} — "
                    f"Step 1 (sector): {n} nodes, est. {est} tokens"
                )
                sector_result = _classify_one_with_retry(
                    client=client,
                    cfg=cfg,
                    prompt_text=prompt_text,
                    taxonomy_type="sector",
                    allowed_nodes=sector_allowed_nodes,
                    company_name=company_name,
                    company_country=company_country,
                    company_description=company_desc,
                )
                sec_node_id = (
                    _lookup_node_id(taxonomy, "sector", sector_result.node_name)
                    if sector_result.node_name
                    else "00000000-0000-0000-0000-000000000000"
                )
                out_rows.append(
                    _make_row(run_id, company_id, company_name, sector_result, sec_node_id, cfg.model, cfg.prompt_version)
                )

                if sector_result.node_name is None or sector_result.confidence < cfg.confidence_threshold:
                    print(
                        f"  -> Sector unclassifiable (confidence={sector_result.confidence:.2f}), "
                        f"skipping industry"
                    )
                    sector_name = None
                else:
                    sector_name = sector_result.node_name
                    print(f"  -> Sector: {sector_name} ({sector_result.confidence:.2f})")

            # Step 2 — Industry (using only nodes from the matched sector)
            if sector_name is not None and not industry_done:
                filtered_nodes = sector_to_industry_map.get(sector_name, [])
                if not filtered_nodes:
                    print(f"  -> No GICS industry nodes mapped for sector '{sector_name}', skipping industry")
                else:
                    n = len(filtered_nodes)
                    est = _estimate_tokens(n, company_name, company_desc)
                    print(
                        f"Classifying [{i}/{total}]: {company_name} — "
                        f"Step 2 (industry, sector={sector_name}): {n} nodes, est. {est} tokens"
                    )
                    industry_result = _classify_one_with_retry(
                        client=client,
                        cfg=cfg,
                        prompt_text=prompt_text,
                        taxonomy_type="industry",
                        allowed_nodes=filtered_nodes,
                        company_name=company_name,
                        company_country=company_country,
                        company_description=company_desc,
                    )
                    ind_node_id = (
                        _lookup_node_id(taxonomy, "industry", industry_result.node_name)
                        if industry_result.node_name
                        else "00000000-0000-0000-0000-000000000000"
                    )
                    if industry_result.node_name is None:
                        print(f"  -> Industry unclassifiable")
                    elif industry_result.confidence < cfg.confidence_threshold:
                        print(f"  -> Industry low confidence ({industry_result.confidence:.2f}): {industry_result.node_name}")
                    else:
                        print(f"  -> Industry: {industry_result.node_name} ({industry_result.confidence:.2f})")
                    out_rows.append(
                        _make_row(run_id, company_id, company_name, industry_result, ind_node_id, cfg.model, cfg.prompt_version)
                    )

        else:
            # -----------------------------------------------------------------
            # Single-step: sector or geography
            # -----------------------------------------------------------------
            if (company_id, args.taxonomy_type) in already_classified:
                skipped += 1
                continue

            # Pre-classification: instrument rules bypass the API call (sector only)
            if args.taxonomy_type == "sector":
                instrument_match = _check_instrument_rules(company_name)
                if instrument_match is not None:
                    matched_sector, _ = instrument_match
                    print(f"Pre-classified {company_name} as Financials/Capital Markets (instrument rule match)")
                    result = CompanyClassificationOut(
                        taxonomy_type="sector",
                        node_name=matched_sector,
                        confidence=1.0,
                        rationale="Pre-classified by instrument rule — no API call.",
                        assumptions=[],
                    )
                    node_id = _lookup_node_id(taxonomy, "sector", matched_sector)
                    out_rows.append(
                        _make_row(run_id, company_id, company_name, result, node_id, "instrument_rule", cfg.prompt_version)
                    )
                    continue

            n = len(allowed_nodes)
            est = _estimate_tokens(n, company_name, company_desc)
            print(
                f"Classifying [{i}/{total}]: {company_name} — "
                f"{n} nodes, est. {est} tokens"
            )
            result = _classify_one_with_retry(
                client=client,
                cfg=cfg,
                prompt_text=prompt_text,
                taxonomy_type=args.taxonomy_type,
                allowed_nodes=allowed_nodes,
                company_name=company_name,
                company_country=company_country,
                company_description=company_desc,
            )

            if result.node_name is None:
                print(f"  -> Unclassifiable")
            elif result.confidence < cfg.confidence_threshold:
                print(f"  -> Low confidence ({result.confidence:.2f}): {result.node_name}")
            else:
                print(f"  -> {result.node_name} ({result.confidence:.2f})")

            node_id = (
                _lookup_node_id(taxonomy, args.taxonomy_type, result.node_name)
                if result.node_name
                else "00000000-0000-0000-0000-000000000000"
            )
            out_rows.append(
                _make_row(run_id, company_id, company_name, result, node_id, cfg.model, cfg.prompt_version)
            )

        # Flush batch of 10 to DB in non-CSV mode
        if not csv_mode and len(out_rows) % 10 == 0 and out_rows:
            upsert_rows(FactExposureClassification, out_rows[-10:], ["classification_id"])
            total_written += 10
            print(f"Wrote 10 classifications to PostgreSQL (total so far: {total_written})")

    new_df = pd.DataFrame(out_rows) if out_rows else pd.DataFrame()

    # -----------------------------------------------------------------------
    # Write output
    # -----------------------------------------------------------------------
    if csv_mode:
        # Combine with existing CSV data if present
        if existing_df is not None:
            out_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            out_df = new_df
        out_df.to_csv(out_path, index=False)
        print(f"Wrote: {out_path}")
    else:
        # Write any remaining rows not yet flushed by the in-loop batch writer
        remainder = len(out_rows) % 10
        if out_rows and remainder > 0:
            upsert_rows(FactExposureClassification, out_rows[-remainder:], ["classification_id"])
            total_written += remainder
            print(f"Wrote {remainder} classifications to PostgreSQL (total so far: {total_written})")
        if not out_rows:
            print("No new classifications to write.")

        # Update dim_company with sector/industry/node_id from classification results.
        # Only applies to industry-type classifications.
        if out_rows and args.taxonomy_type == "industry":
            industry_out_rows = [r for r in out_rows if r["taxonomy_type"] == "industry"]
            if industry_out_rows:
                _update_dim_company_from_classifications(
                    out_rows=industry_out_rows,
                    tax=tax,
                    taxonomy=taxonomy,
                )

    print(f"Skipped (already classified): {skipped}")
    print(f"New rows: {len(new_df)}")
    print(f"run_id: {run_id}")
    print(f"model: {cfg.model}")


def _update_dim_company_from_classifications(
    out_rows: list[dict],
    tax: pd.DataFrame,
    taxonomy: pd.DataFrame,
) -> None:
    """Update dim_company.primary_sector, primary_industry, and industry_taxonomy_node_id
    for every company that received an industry classification this run.

    Loads the full current dim_company records so that all other columns are preserved
    through the upsert (upsert_rows updates every non-key column).
    """
    # Build lookup: company_id -> taxonomy_node_id for this run's results
    classification_map: dict[str, str] = {}
    for row in out_rows:
        node_id = row["taxonomy_node_id"]
        if node_id == "00000000-0000-0000-0000-000000000000":
            continue  # unclassifiable — don't overwrite anything
        classification_map[str(row["company_id"])] = node_id

    if not classification_map:
        print("No classifiable results to write back to dim_company.")
        return

    # Load full company records so we can do a complete upsert without losing other fields
    all_companies = get_all(DimCompany)
    if all_companies.empty:
        return

    # Build taxonomy lookup: node_id -> node row
    tax_by_id = {str(row["taxonomy_node_id"]): row for _, row in taxonomy.iterrows()}

    company_update_records = []

    for _, company_row in all_companies.iterrows():
        cid = str(company_row["company_id"])
        if cid not in classification_map:
            continue

        node_id = classification_map[cid]
        node = tax_by_id.get(node_id)
        if node is None:
            continue

        # Resolve the parent sector node (level 1 parent of the industry node)
        parent_id = node.get("parent_node_id")
        sector_name: Optional[str] = None
        if parent_id and not pd.isna(parent_id):
            parent_node = tax_by_id.get(str(parent_id))
            if parent_node is not None:
                sector_name = str(parent_node["node_name"])

        # Build a full record — start from the existing company row and patch the three fields
        record = company_row.to_dict()
        record["primary_sector"] = sector_name
        record["primary_industry"] = str(node["node_name"])
        record["industry_taxonomy_node_id"] = node_id
        company_update_records.append(record)

    if company_update_records:
        upsert_rows(DimCompany, company_update_records, ["company_id"])
        print(
            f"Updated {len(company_update_records)} dim_company rows "
            f"(primary_sector, primary_industry, industry_taxonomy_node_id)"
        )
    else:
        print("No dim_company rows needed updating.")


if __name__ == "__main__":
    main()
