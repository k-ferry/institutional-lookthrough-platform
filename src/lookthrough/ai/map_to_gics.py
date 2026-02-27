"""
Map BDC reported_sector descriptions to GICS (Global Industry Classification Standard).

This module uses Claude to intelligently map free-text sector/industry descriptions
from BDC filings to standardized GICS sub-industry codes.

Supports both PostgreSQL and CSV modes:
- Default: Read from PostgreSQL, write to PostgreSQL gics_mapping table,
           and update dim_company.primary_sector / primary_industry
- CSV mode: Read from CSV, write to data/gold/gics_mapping.csv (original behavior)

Usage:
    python -m src.lookthrough.ai.map_to_gics
    python -m src.lookthrough.ai.map_to_gics --limit 50
    python -m src.lookthrough.ai.map_to_gics --batch-size 10
    python -m src.lookthrough.ai.map_to_gics --csv
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

from anthropic import Anthropic, transform_schema

from src.lookthrough.db.repository import (
    _is_csv_mode,
    get_all,
    upsert_rows,
)
from src.lookthrough.db.models import (
    DimCompany,
    FactReportedHolding,
    GICSMapping,
)
from src.lookthrough.taxonomy.gics import get_sub_industry_lookup, get_gics_taxonomy


# ----------------------------
# Schema for structured output
# ----------------------------
class GICSMappingItem(BaseModel):
    """Single mapping from reported_sector to GICS sub_industry."""

    reported_sector: str = Field(..., description="The original reported_sector description")
    gics_sub_industry_code: str = Field(
        ...,
        description="8-digit GICS sub-industry code (e.g., '45102010' for Application Software)",
    )
    confidence: float = Field(
        ...,
        description="Confidence score from 0.0 to 1.0",
    )
    rationale: str = Field(
        ...,
        description="Brief explanation of why this GICS mapping was chosen (1-2 sentences)",
    )


class GICSMappingBatch(BaseModel):
    """Batch response containing multiple GICS mappings."""

    mappings: list[GICSMappingItem] = Field(
        ...,
        description="List of mappings from reported_sector to GICS sub_industry",
    )


@dataclass(frozen=True)
class MapperConfig:
    model: str = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5-20250929")
    max_tokens: int = 4096
    batch_size: int = 20  # Number of descriptions to map per API call


def _repo_root() -> Path:
    # src/lookthrough/ai/map_to_gics.py -> repo root is 4 parents up
    return Path(__file__).resolve().parents[3]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _coerce_gics_codes_to_float(record: dict) -> dict:
    """GICS codes are stored as Float in the DB model (10.0, 15.0, etc.).
    The taxonomy module returns them as strings ('10', '15'). Convert here.
    """
    numeric_fields = [
        "gics_sector_code",
        "gics_industry_group_code",
        "gics_industry_code",
        "gics_sub_industry_code",
    ]
    out = dict(record)
    for field in numeric_fields:
        val = out.get(field)
        if val is not None and str(val).strip() != "":
            try:
                out[field] = float(val)
            except (ValueError, TypeError):
                out[field] = None
        else:
            out[field] = None
    return out


def _build_gics_reference() -> str:
    """Build a compact reference of all GICS sub-industries for the prompt."""
    taxonomy = get_gics_taxonomy()
    lines = []

    # Group by sector for readability
    sectors = [n for n in taxonomy if n["level"] == "sector"]
    for sector in sectors:
        lines.append(f"\n## {sector['code']} - {sector['name']}")

        # Get sub-industries under this sector
        sub_industries = [
            n for n in taxonomy
            if n["level"] == "sub_industry" and n["code"].startswith(sector["code"])
        ]
        for si in sub_industries:
            lines.append(f"  {si['code']}: {si['name']}")

    return "\n".join(lines)


def _build_prompt(descriptions: list[str], gics_reference: str) -> str:
    """Build the prompt for mapping descriptions to GICS."""
    descriptions_json = json.dumps(descriptions, ensure_ascii=False, indent=2)

    return f"""You are an expert financial analyst specializing in industry classification.

Your task is to map company/investment descriptions to the GICS (Global Industry Classification Standard) taxonomy.

## GICS Sub-Industry Reference
{gics_reference}

## Instructions

For each description below, determine the most appropriate GICS sub-industry code (8-digit code).

Consider:
1. The primary business activity described
2. The end market or customer segment
3. The products or services mentioned
4. Industry-specific terminology

If a description is ambiguous or could fit multiple categories:
- Choose the most specific applicable sub-industry
- If it mentions software, prefer IT sub-industries (45xxxxxx) unless clearly healthcare/financial software
- If it mentions services, consider whether it's B2B professional services vs consumer services
- For holding companies or diversified businesses, use Industrial Conglomerates (20105010) or Multi-Sector Holdings (40201020)

If a description is invalid (e.g., just numbers, interest rates, or gibberish), map to the most generic applicable category with low confidence.

## Descriptions to Map
{descriptions_json}

Return a JSON object with a "mappings" array containing one entry per description, in the same order as the input.
Each mapping must include:
- reported_sector: The original description (exact match)
- gics_sub_industry_code: The 8-digit GICS code
- confidence: Float 0.0-1.0 (use lower values for ambiguous mappings)
- rationale: Brief explanation"""


def map_batch(
    client: Anthropic,
    cfg: MapperConfig,
    descriptions: list[str],
    gics_reference: str,
    valid_codes: set[str],
) -> list[dict]:
    """Map a batch of descriptions to GICS using Claude."""
    prompt = _build_prompt(descriptions, gics_reference)

    response = client.messages.create(
        model=cfg.model,
        max_tokens=cfg.max_tokens,
        messages=[{"role": "user", "content": prompt}],
        output_config={
            "format": {
                "type": "json_schema",
                "schema": transform_schema(GICSMappingBatch),
            }
        },
    )

    raw = response.content[0].text
    try:
        parsed = GICSMappingBatch.model_validate_json(raw)
    except ValidationError as e:
        # Return error mappings for all descriptions
        return [
            {
                "reported_sector": desc,
                "gics_sector_code": None,
                "gics_sector_name": None,
                "gics_industry_group_code": None,
                "gics_industry_group_name": None,
                "gics_industry_code": None,
                "gics_industry_name": None,
                "gics_sub_industry_code": None,
                "gics_sub_industry_name": None,
                "confidence": 0.0,
                "rationale": f"ValidationError: {str(e)[:200]}",
            }
            for desc in descriptions
        ]

    # Post-validate and enrich with full hierarchy
    sub_industry_lookup = get_sub_industry_lookup()
    results = []

    for mapping in parsed.mappings:
        code = mapping.gics_sub_industry_code
        confidence = _clamp01(mapping.confidence)

        # Validate code exists
        if code not in valid_codes:
            results.append({
                "reported_sector": mapping.reported_sector,
                "gics_sector_code": None,
                "gics_sector_name": None,
                "gics_industry_group_code": None,
                "gics_industry_group_name": None,
                "gics_industry_code": None,
                "gics_industry_name": None,
                "gics_sub_industry_code": code,
                "gics_sub_industry_name": None,
                "confidence": 0.0,
                "rationale": f"Invalid GICS code returned: {code}",
            })
            continue

        # Enrich with full hierarchy
        hierarchy = sub_industry_lookup[code]
        results.append({
            "reported_sector": mapping.reported_sector,
            **hierarchy,
            "confidence": confidence,
            "rationale": mapping.rationale,
        })

    return results


def _update_dim_company_from_gics(
    gics_results: list[dict],
    already_mapped_df: Optional[pd.DataFrame],
) -> None:
    """Update dim_company.primary_sector and primary_industry for BDC companies
    whose reported_sector was just mapped (or was already mapped).

    Strategy:
    - For each company in dim_company where primary_sector IS NULL,
      look up the company's reported_sector from their holdings,
      then apply the GICS sector_name and industry_name.
    - Does NOT overwrite industry_taxonomy_node_id — that belongs to
      classify_companies.py which maps to the platform's taxonomy nodes.
    """
    # Build a lookup: reported_sector (lowercase) -> mapping dict
    gics_lookup: dict[str, dict] = {}
    for r in gics_results:
        sec = r.get("reported_sector", "")
        if sec:
            gics_lookup[sec.lower()] = r

    # Also include anything already in the DB (passed in as a DataFrame)
    if already_mapped_df is not None and not already_mapped_df.empty:
        for _, row in already_mapped_df.iterrows():
            sec = str(row.get("reported_sector", ""))
            if sec and sec.lower() not in gics_lookup:
                gics_lookup[sec.lower()] = row.to_dict()

    if not gics_lookup:
        return

    # Load holdings to build: company_id -> reported_sector
    all_holdings = get_all(FactReportedHolding)
    if all_holdings.empty:
        return

    company_sector_map: dict[str, str] = {}
    for _, h in all_holdings.iterrows():
        cid = h.get("company_id")
        sec = h.get("reported_sector")
        if cid and not pd.isna(cid) and sec and not pd.isna(sec):
            cleaned = str(sec).strip()
            if cleaned:
                company_sector_map[str(cid)] = cleaned

    # Load full company records — we need all columns for upsert
    all_companies = get_all(DimCompany)
    if all_companies.empty:
        return

    company_update_records = []
    for _, company in all_companies.iterrows():
        # Only update companies that have no primary_sector yet
        if pd.notna(company.get("primary_sector")) and str(company.get("primary_sector", "")).strip():
            continue

        cid = str(company["company_id"])
        reported_sec = company_sector_map.get(cid)
        if not reported_sec:
            continue

        mapping = gics_lookup.get(reported_sec.lower())
        if not mapping:
            continue

        gics_sector = mapping.get("gics_sector_name")
        gics_industry = mapping.get("gics_industry_name")

        if not gics_sector:
            continue  # Invalid/unresolved mapping — skip

        # Build the full record, preserving all existing fields
        record = company.to_dict()
        record["primary_sector"] = gics_sector
        record["primary_industry"] = gics_industry
        # Leave industry_taxonomy_node_id unchanged — classify_companies.py owns that field
        company_update_records.append(record)

    if company_update_records:
        upsert_rows(DimCompany, company_update_records, ["company_id"])
        print(
            f"Updated {len(company_update_records)} dim_company rows "
            f"(primary_sector, primary_industry) from GICS mapping"
        )
    else:
        print("No dim_company rows needed updating from GICS mapping.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Map BDC reported_sector descriptions to GICS codes using Claude."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of unique descriptions to map (default: all)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Number of descriptions per API call (default: 20)",
    )
    parser.add_argument("--csv", action="store_true", help="Use CSV mode instead of PostgreSQL")
    args = parser.parse_args()

    csv_mode = args.csv or _is_csv_mode()

    cfg = MapperConfig(batch_size=args.batch_size)
    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    print(f"Data mode: {'CSV' if csv_mode else 'PostgreSQL'}")

    # -----------------------------------------------------------------------
    # Load unique reported_sector values
    # -----------------------------------------------------------------------
    if csv_mode:
        holdings_path = silver / "fact_reported_holding.csv"
        holdings = _read_csv(holdings_path)
    else:
        holdings = get_all(FactReportedHolding)

    unique_sectors = (
        holdings["reported_sector"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    # Filter out empty strings and literal "nan"
    unique_sectors = [s for s in unique_sectors if s and s.lower() != "nan"]
    print(f"Found {len(unique_sectors)} unique reported_sector values")

    # -----------------------------------------------------------------------
    # Load existing mappings to skip already-mapped descriptions
    # -----------------------------------------------------------------------
    already_mapped: set[str] = set()
    existing_df: Optional[pd.DataFrame] = None

    if csv_mode:
        out_path = gold / "gics_mapping.csv"
        if out_path.exists():
            existing_df = pd.read_csv(out_path)
            already_mapped = set(existing_df["reported_sector"].astype(str).tolist())
            print(f"Loaded {len(existing_df)} existing mappings from {out_path}")
    else:
        existing_db = get_all(GICSMapping)
        if not existing_db.empty:
            existing_df = existing_db
            already_mapped = set(existing_db["reported_sector"].astype(str).tolist())
            print(f"Loaded {len(existing_db)} existing mappings from PostgreSQL")

    # Filter to only unmapped descriptions
    to_map = [s for s in unique_sectors if s not in already_mapped]
    print(f"Descriptions to map: {len(to_map)}")

    if args.limit:
        to_map = to_map[: args.limit]
        print(f"Limited to: {len(to_map)}")

    if not to_map:
        print("No new descriptions to map.")
        # Still attempt to update dim_company from existing mappings
        if not csv_mode and existing_df is not None:
            print("Applying existing GICS mappings to dim_company...")
            _update_dim_company_from_gics([], existing_df)
        return

    # -----------------------------------------------------------------------
    # Build GICS reference and valid codes set
    # -----------------------------------------------------------------------
    gics_reference = _build_gics_reference()
    taxonomy = get_gics_taxonomy()
    valid_codes = {n["code"] for n in taxonomy if n["level"] == "sub_industry"}

    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    all_results: list[dict] = []
    total_batches = (len(to_map) + cfg.batch_size - 1) // cfg.batch_size

    for i in range(0, len(to_map), cfg.batch_size):
        batch = to_map[i : i + cfg.batch_size]
        batch_num = i // cfg.batch_size + 1
        print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} descriptions)...")

        results = map_batch(client, cfg, batch, gics_reference, valid_codes)
        all_results.extend(results)

    new_df = pd.DataFrame(all_results)

    # -----------------------------------------------------------------------
    # Write output
    # -----------------------------------------------------------------------
    if csv_mode:
        # Normalise column order
        cols = [
            "reported_sector",
            "gics_sector_code", "gics_sector_name",
            "gics_industry_group_code", "gics_industry_group_name",
            "gics_industry_code", "gics_industry_name",
            "gics_sub_industry_code", "gics_sub_industry_name",
            "confidence", "rationale",
        ]
        if existing_df is not None:
            for col in cols:
                if col not in existing_df.columns:
                    existing_df[col] = ""
            existing_df = existing_df[cols]
            new_df = new_df[cols]
            out_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            out_df = new_df[cols]

        out_df.to_csv(out_path, index=False)
        print(f"\nWrote: {out_path}")
    else:
        # Convert GICS code strings to floats to match the DB model (Float columns)
        db_records = [_coerce_gics_codes_to_float(r) for r in all_results]

        if db_records:
            upsert_rows(GICSMapping, db_records, ["reported_sector"])
            print(f"\nWrote {len(db_records)} mappings to PostgreSQL:gics_mapping")

        # Update dim_company.primary_sector / primary_industry for BDC companies
        print("Updating dim_company from GICS mappings...")
        _update_dim_company_from_gics(all_results, existing_df)

    # Print summary
    print(f"New mappings: {len(new_df)}")
    print(f"Model: {cfg.model}")

    # Show sample of new mappings
    if len(new_df) > 0 and "gics_sub_industry_name" in new_df.columns:
        print("\nSample mappings:")
        sample = new_df.head(5)
        for _, row in sample.iterrows():
            sector_label = str(row.get("reported_sector", ""))[:50]
            sub_name = str(row.get("gics_sub_industry_name", ""))
            sub_code = str(row.get("gics_sub_industry_code", ""))
            conf = float(row.get("confidence", 0.0))
            print(f"  '{sector_label}' -> {sub_code} ({sub_name}) [{conf:.2f}]")


if __name__ == "__main__":
    main()
