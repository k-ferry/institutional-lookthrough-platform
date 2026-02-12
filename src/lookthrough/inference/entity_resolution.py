"""Entity Resolution Module for Institutional Look-Through Platform.

Matches raw company names in fact_reported_holding to canonical company_id
using dim_company and dim_entity_alias.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def _repo_root() -> Path:
    # src/lookthrough/inference/entity_resolution.py -> repo root is 4 parents up
    return Path(__file__).resolve().parents[3]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def _is_null(value) -> bool:
    """Check if value is null, NaN, or string 'nan'."""
    if value is None:
        return True
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.lower() in ("nan", "none", ""):
        return True
    return False


def resolve_entities() -> pd.DataFrame:
    """
    Resolve raw company names to canonical company_id.

    Resolution strategy:
    1. Exact match (case-insensitive) against dim_company.company_name
    2. Exact match (case-insensitive) against dim_entity_alias.alias_text
    3. If no match found, leave company_id as null

    Returns:
        Updated holdings DataFrame
    """
    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    # Load required data
    holdings = _read_csv(silver / "fact_reported_holding.csv")
    companies = _read_csv(silver / "dim_company.csv")
    aliases = _read_csv(silver / "dim_entity_alias.csv")

    # Build lookup dictionaries (case-insensitive)
    # Direct company name -> company_id
    company_name_to_id: dict[str, str] = {}
    for _, row in companies.iterrows():
        name_lower = str(row["company_name"]).lower().strip()
        company_name_to_id[name_lower] = str(row["company_id"])

    # Alias text -> company_id (only for entity_type='company')
    alias_to_company_id: dict[str, str] = {}
    for _, row in aliases.iterrows():
        if str(row.get("entity_type", "")).lower() == "company":
            alias_lower = str(row["alias_text"]).lower().strip()
            alias_to_company_id[alias_lower] = str(row["entity_id"])

    # Track resolution statistics
    resolved_direct = 0
    resolved_alias = 0
    unresolved = 0
    already_resolved = 0

    # Resolution log entries
    resolution_log: list[dict] = []

    # Process each holding
    for idx, row in holdings.iterrows():
        holding_id = str(row["reported_holding_id"])
        raw_name = str(row.get("raw_company_name", ""))
        raw_name_lower = raw_name.lower().strip()
        current_company_id = row.get("company_id")

        # Skip if already has a valid company_id
        if not _is_null(current_company_id):
            already_resolved += 1
            continue

        matched_company_id: Optional[str] = None
        match_method = "unresolved"
        match_confidence = 0.0

        # Try direct match against company_name
        if raw_name_lower in company_name_to_id:
            matched_company_id = company_name_to_id[raw_name_lower]
            match_method = "direct"
            match_confidence = 1.0
            resolved_direct += 1
        # Try alias match
        elif raw_name_lower in alias_to_company_id:
            matched_company_id = alias_to_company_id[raw_name_lower]
            match_method = "alias"
            match_confidence = 0.9
            resolved_alias += 1
        else:
            unresolved += 1

        # Update holdings DataFrame if matched
        if matched_company_id is not None:
            holdings.at[idx, "company_id"] = matched_company_id

        # Log the resolution attempt
        resolution_log.append({
            "reported_holding_id": holding_id,
            "raw_company_name": raw_name,
            "matched_company_id": matched_company_id,
            "match_method": match_method,
            "match_confidence": match_confidence,
        })

    # Write updated holdings back to silver
    holdings_path = silver / "fact_reported_holding.csv"
    holdings.to_csv(holdings_path, index=False)

    # Write resolution log to gold
    resolution_log_df = pd.DataFrame(resolution_log)
    log_path = gold / "entity_resolution_log.csv"
    resolution_log_df.to_csv(log_path, index=False)

    # Print summary statistics
    total_processed = resolved_direct + resolved_alias + unresolved
    print("Entity Resolution Summary")
    print("=" * 40)
    print(f"Already resolved (skipped):  {already_resolved:,}")
    print(f"Processed (null company_id): {total_processed:,}")
    print("-" * 40)
    print(f"  Resolved by direct match:  {resolved_direct:,}")
    print(f"  Resolved by alias match:   {resolved_alias:,}")
    print(f"  Unresolved:                {unresolved:,}")
    print("-" * 40)
    if total_processed > 0:
        resolution_rate = (resolved_direct + resolved_alias) / total_processed * 100
        print(f"Resolution rate:             {resolution_rate:.1f}%")
    print()
    print(f"Wrote updated holdings: {holdings_path}")
    print(f"Wrote resolution log:   {log_path}")

    return holdings


def main() -> None:
    resolve_entities()


if __name__ == "__main__":
    main()
