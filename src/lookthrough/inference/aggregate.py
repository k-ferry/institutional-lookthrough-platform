"""
V1 Aggregation: Produce fact_aggregation_snapshot from inferred exposures.

Reads:
  - fact_inferred_exposure (Gold)
  - dim_taxonomy_node (Silver)
  - dim_company (Silver)
  - fact_exposure_classification (Gold, optional, AI classifications)

Writes:
  - fact_aggregation_snapshot (Gold)

Aggregates exposures by:
  - taxonomy_type = "sector"    -> level 1 sector nodes
  - taxonomy_type = "industry"  -> level 2 sector nodes (industries)
  - taxonomy_type = "geography" -> level 2 geography nodes (countries)

If AI classifications exist, they override deterministic company lookups for
industry classification, and provide confidence scores for weighted metrics.

Supports both PostgreSQL and CSV modes:
- Default: Read/write from PostgreSQL database
- CSV mode: Set CSV_MODE=1 or use --csv flag for backward compatibility

Run via: python -m src.lookthrough.inference.aggregate
"""
from __future__ import annotations

import argparse
import os
import uuid
from datetime import date as _date_cls
from pathlib import Path

import numpy as np
import pandas as pd

from src.lookthrough.db.repository import (
    _is_csv_mode,
    dataframe_to_records,
    execute_update,
    get_all,
    upsert_rows,
)
from src.lookthrough.db.models import (
    DimCompany,
    DimTaxonomyNode,
    FactAggregationSnapshot,
    FactExposureClassification,
    FactInferredExposure,
    FactReportedHolding,
)

# Stable placeholder for unknown/missing taxonomy classification
UNKNOWN_TAXONOMY_NODE_ID = "00000000-0000-0000-0000-000000000000"


def _repo_root() -> Path:
    """Return repository root (4 levels up from this file)."""
    return Path(__file__).resolve().parents[3]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def _build_taxonomy_lookup(taxonomy_df: pd.DataFrame) -> dict:
    """
    Build lookup tables for taxonomy nodes.

    Returns dict with:
      - node_by_id: taxonomy_node_id -> row dict
      - sector_nodes: set of level 1 sector node IDs
      - industry_nodes: set of level 2 sector node IDs
      - country_nodes: set of level 2 geography node IDs
    """
    node_by_id = {}
    sector_nodes = set()
    industry_nodes = set()
    country_nodes = set()

    for _, row in taxonomy_df.iterrows():
        node_id = str(row["taxonomy_node_id"])
        node_by_id[node_id] = row.to_dict()

        tax_type = str(row.get("taxonomy_type", ""))
        level = int(row.get("level", 0))

        if tax_type == "sector" and level == 1:
            sector_nodes.add(node_id)
        elif tax_type == "sector" and level == 2:
            industry_nodes.add(node_id)
        elif tax_type == "geography" and level == 2:
            country_nodes.add(node_id)

    return {
        "node_by_id": node_by_id,
        "sector_nodes": sector_nodes,
        "industry_nodes": industry_nodes,
        "country_nodes": country_nodes,
    }


def _get_sector_node_id(
    industry_node_id: str | None,
    taxonomy_lookup: dict,
) -> str:
    """
    Given an industry node ID (level 2), return its parent sector node ID (level 1).
    Returns UNKNOWN if not found.
    """
    if not industry_node_id or pd.isna(industry_node_id):
        return UNKNOWN_TAXONOMY_NODE_ID

    node_id = str(industry_node_id)
    node = taxonomy_lookup["node_by_id"].get(node_id)
    if not node:
        return UNKNOWN_TAXONOMY_NODE_ID

    parent_id = node.get("parent_node_id")
    if parent_id and not pd.isna(parent_id):
        return str(parent_id)

    # If no parent, this might already be a level 1 node
    if node_id in taxonomy_lookup["sector_nodes"]:
        return node_id

    return UNKNOWN_TAXONOMY_NODE_ID


def _load_classifications(gold_path: Path, csv_mode: bool = False) -> dict:
    """
    Load AI classifications if available.

    Returns lookup: (company_id, taxonomy_type) -> {taxonomy_node_id, confidence}
    """
    if csv_mode:
        classification_file = gold_path / "fact_exposure_classification.csv"
        if not classification_file.exists():
            return {}
        df = pd.read_csv(classification_file)
    else:
        df = get_all(FactExposureClassification)

    if df.empty:
        return {}

    lookup = {}
    for _, row in df.iterrows():
        company_id = str(row["company_id"]) if pd.notna(row.get("company_id")) else None
        taxonomy_type = str(row["taxonomy_type"]) if pd.notna(row.get("taxonomy_type")) else None
        if company_id and taxonomy_type:
            key = (company_id, taxonomy_type)
            lookup[key] = {
                "taxonomy_node_id": str(row["taxonomy_node_id"]) if pd.notna(row.get("taxonomy_node_id")) else UNKNOWN_TAXONOMY_NODE_ID,
                "confidence": float(row["confidence"]) if pd.notna(row.get("confidence")) else 0.0,
            }
    return lookup


def _load_reported_sector_lookup(silver_path: Path, csv_mode: bool = False) -> dict:
    """
    Load reported_sector values from holdings to use as fallback classification.

    Returns lookup: company_id -> reported_sector (string)
    """
    if csv_mode:
        holdings_file = silver_path / "fact_reported_holding.csv"
        if not holdings_file.exists():
            return {}
        df = pd.read_csv(holdings_file)
    else:
        df = get_all(FactReportedHolding)

    if df.empty:
        return {}

    lookup = {}

    # For each company_id, collect reported_sector values
    # If multiple holdings have different sectors, we take the most common one
    for _, row in df.iterrows():
        company_id = row.get("company_id")
        reported_sector = row.get("reported_sector")

        if not company_id or pd.isna(company_id):
            continue
        if not reported_sector or pd.isna(reported_sector):
            continue

        company_id = str(company_id)
        reported_sector = str(reported_sector).strip()
        if not reported_sector:
            continue

        # Store the first non-empty reported_sector per company
        if company_id not in lookup:
            lookup[company_id] = reported_sector

    return lookup


def _build_reported_sector_to_taxonomy_lookup(taxonomy_df: pd.DataFrame) -> dict:
    """
    Build lookup from reported_sector names to taxonomy_node_id.

    Maps sector/industry names (case-insensitive) to their taxonomy_node_id.
    Prioritizes exact matches, handles both sector (level 1) and industry (level 2) nodes.

    Returns lookup: sector_name_lower -> taxonomy_node_id
    """
    lookup = {}
    if taxonomy_df.empty:
        return lookup

    for _, row in taxonomy_df.iterrows():
        node_name = row.get("node_name")
        if not node_name or pd.isna(node_name):
            continue

        node_id = str(row["taxonomy_node_id"])
        taxonomy_type = str(row.get("taxonomy_type", ""))
        level = int(row.get("level", 0)) if pd.notna(row.get("level")) else 0

        # Only map sector (level 1) and industry (level 2) nodes
        if taxonomy_type == "sector" and level in (1, 2):
            name_lower = str(node_name).strip().lower()
            # Don't overwrite existing entries (first wins)
            if name_lower not in lookup:
                lookup[name_lower] = node_id

    return lookup


def _run_aggregation(
    exposures_df: pd.DataFrame,
    group_cols: list[str],
    taxonomy_type_configs: list[tuple],
) -> pd.DataFrame:
    """
    Aggregate resolved exposures by group columns and taxonomy types.

    Args:
        exposures_df: DataFrame with taxonomy resolution columns appended.
        group_cols: Columns to group by (e.g. ["run_id", "portfolio_id", "as_of_date"]).
        taxonomy_type_configs: List of (taxonomy_type, node_col, conf_col) tuples.

    Returns:
        DataFrame of aggregation rows (coverage_pct is np.nan, filled by caller).
    """
    aggregation_rows = []
    for taxonomy_type, node_col, conf_col in taxonomy_type_configs:
        grouped = exposures_df.groupby(group_cols + [node_col])
        for keys, group in grouped:
            *group_vals, taxonomy_node_id = keys
            group_dict = dict(zip(group_cols, group_vals))
            total_exposure_value_usd = group["exposure_value_usd"].sum()
            confidence_weighted_exposure = (
                group["exposure_value_usd"] * group[conf_col]
            ).sum()
            aggregation_rows.append(
                {
                    **group_dict,
                    "taxonomy_type": taxonomy_type,
                    "taxonomy_node_id": taxonomy_node_id,
                    "total_exposure_value_usd": total_exposure_value_usd,
                    "total_exposure_p10": np.nan,
                    "total_exposure_p90": np.nan,
                    "coverage_pct": np.nan,
                    "confidence_weighted_exposure": confidence_weighted_exposure,
                }
            )
    return pd.DataFrame(aggregation_rows) if aggregation_rows else pd.DataFrame()


def _compute_coverage_pct(result: pd.DataFrame, group_keys: list[str]) -> pd.DataFrame:
    """
    Compute coverage_pct for an aggregation result DataFrame.

    coverage_pct = sum(exposure for known nodes) / sum(all exposure) per group.

    Args:
        result: Aggregation DataFrame (must have taxonomy_node_id, total_exposure_value_usd).
        group_keys: Columns defining the coverage group.

    Returns:
        DataFrame with coverage_pct column populated.
    """
    if len(result) == 0:
        return result
    result = result.copy()
    portfolio_totals = result.groupby(group_keys)["total_exposure_value_usd"].transform(
        "sum"
    )
    is_known = result["taxonomy_node_id"] != UNKNOWN_TAXONOMY_NODE_ID
    result["_known_exposure"] = result["total_exposure_value_usd"].where(is_known, 0.0)
    known_totals = result.groupby(group_keys)["_known_exposure"].transform("sum")
    result["coverage_pct"] = (known_totals / portfolio_totals).fillna(0.0)
    return result.drop(columns=["_known_exposure"])


def aggregate_exposures_v1(csv_mode: bool = False) -> pd.DataFrame:
    """
    V1 aggregation: group inferred exposures by taxonomy buckets.

    Produces one row per (run_id, portfolio_id, as_of_date, taxonomy_type, taxonomy_node_id).

    Classification priority:
    1. AI classifications from fact_exposure_classification (highest priority)
    2. reported_sector from holdings (confidence 0.75)
    3. Deterministic lookup from dim_company (confidence 1.0)
    4. Unknown (confidence 0.0)

    Args:
        csv_mode: If True, use CSV files instead of database
    """
    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    # Load inputs from DB or CSV
    if csv_mode:
        exposures = _read_csv(gold / "fact_inferred_exposure.csv")
        companies = _read_csv(silver / "dim_company.csv")
        taxonomy = _read_csv(silver / "dim_taxonomy_node.csv")
    else:
        exposures = get_all(FactInferredExposure)
        companies = get_all(DimCompany)
        taxonomy = get_all(DimTaxonomyNode)

    taxonomy_lookup = _build_taxonomy_lookup(taxonomy)

    # Load AI classifications if available
    classification_lookup = _load_classifications(gold, csv_mode=csv_mode)
    use_ai_classifications = len(classification_lookup) > 0
    if use_ai_classifications:
        print(f"Using AI classifications: {len(classification_lookup)} entries")

    # Load reported_sector from holdings as fallback classification source
    reported_sector_lookup = _load_reported_sector_lookup(silver, csv_mode=csv_mode)
    if reported_sector_lookup:
        print(f"Using reported_sector fallback: {len(reported_sector_lookup)} companies")

    # Build lookup from reported_sector names to taxonomy_node_id
    sector_name_to_node = _build_reported_sector_to_taxonomy_lookup(taxonomy)

    # Build company lookup: company_id -> {industry_taxonomy_node_id, country_taxonomy_node_id}
    company_lookup = {}
    for _, row in companies.iterrows():
        cid = str(row["company_id"])
        company_lookup[cid] = {
            "industry_taxonomy_node_id": row.get("industry_taxonomy_node_id"),
            "country_taxonomy_node_id": row.get("country_taxonomy_node_id"),
        }

    # Confidence for reported_sector classifications (structured data from filing)
    REPORTED_SECTOR_CONFIDENCE = 0.75

    # For each exposure, determine taxonomy node IDs for sector, industry, geography
    def resolve_taxonomy(row) -> dict:
        company_id = row.get("company_id")
        if not company_id or pd.isna(company_id) or str(company_id) == "nan":
            return {
                "sector_node_id": UNKNOWN_TAXONOMY_NODE_ID,
                "industry_node_id": UNKNOWN_TAXONOMY_NODE_ID,
                "geography_node_id": UNKNOWN_TAXONOMY_NODE_ID,
                "industry_confidence": 0.0,
                "sector_confidence": 0.0,
                "geography_confidence": 0.0,
            }

        company_id = str(company_id)
        company = company_lookup.get(company_id, {})

        industry_resolved = UNKNOWN_TAXONOMY_NODE_ID
        industry_confidence = 0.0

        # Priority 1: Check for AI classification for industry
        ai_industry = classification_lookup.get((company_id, "industry"))
        if ai_industry:
            industry_resolved = ai_industry["taxonomy_node_id"]
            industry_confidence = ai_industry["confidence"]
        else:
            # Priority 2: Check for reported_sector from holdings
            reported_sector = reported_sector_lookup.get(company_id)
            if reported_sector:
                # Map reported_sector name to taxonomy_node_id
                sector_lower = reported_sector.lower()
                if sector_lower in sector_name_to_node:
                    industry_resolved = sector_name_to_node[sector_lower]
                    industry_confidence = REPORTED_SECTOR_CONFIDENCE

            # Priority 3: Fallback to deterministic lookup from dim_company
            if industry_resolved == UNKNOWN_TAXONOMY_NODE_ID:
                industry_node_id = company.get("industry_taxonomy_node_id")
                if industry_node_id and not pd.isna(industry_node_id):
                    industry_resolved = str(industry_node_id)
                    industry_confidence = 1.0  # Deterministic = full confidence

        # Sector node ID (level 1) - get parent of industry
        sector_resolved = _get_sector_node_id(industry_resolved, taxonomy_lookup)
        # Sector confidence inherits from industry (same source)
        sector_confidence = industry_confidence if sector_resolved != UNKNOWN_TAXONOMY_NODE_ID else 0.0

        # Geography node ID (country, level 2) - no AI classification yet
        country_node_id = company.get("country_taxonomy_node_id")
        if country_node_id and not pd.isna(country_node_id):
            geography_resolved = str(country_node_id)
            geography_confidence = 1.0  # Deterministic
        else:
            geography_resolved = UNKNOWN_TAXONOMY_NODE_ID
            geography_confidence = 0.0

        return {
            "sector_node_id": sector_resolved,
            "industry_node_id": industry_resolved,
            "geography_node_id": geography_resolved,
            "industry_confidence": industry_confidence,
            "sector_confidence": sector_confidence,
            "geography_confidence": geography_confidence,
        }

    # Apply taxonomy resolution
    resolved = exposures.apply(resolve_taxonomy, axis=1, result_type="expand")
    exposures = pd.concat([exposures, resolved], axis=1)

    # Taxonomy type config shared by both portfolio- and fund-level aggregations
    taxonomy_type_configs = [
        ("sector", "sector_node_id", "sector_confidence"),
        ("industry", "industry_node_id", "industry_confidence"),
        ("geography", "geography_node_id", "geography_confidence"),
    ]

    # Portfolio-level aggregation (fund_id='')
    port_group_cols = ["run_id", "portfolio_id", "as_of_date"]
    port_result = _run_aggregation(exposures, port_group_cols, taxonomy_type_configs)
    port_result = _compute_coverage_pct(
        port_result, ["run_id", "portfolio_id", "as_of_date", "taxonomy_type"]
    )

    # Sort deterministically for reproducibility
    port_result = port_result.sort_values(
        ["run_id", "portfolio_id", "as_of_date", "taxonomy_type", "taxonomy_node_id"]
    ).reset_index(drop=True)

    # Write output
    if csv_mode:
        # CSV mode: overwrite (no time-series without DB)
        csv_out = port_result.copy()
        csv_out["fund_id"] = ""
        csv_out["snapshot_id"] = ""
        csv_out["snapshot_date"] = ""
        csv_out["is_latest"] = True
        out_path = gold / "fact_aggregation_snapshot.csv"
        csv_out.to_csv(out_path, index=False)
        print(f"Wrote: {out_path}")
        print(f"Rows: {len(port_result)}")
    else:
        snapshot_id = str(uuid.uuid4())
        snapshot_date = _date_cls.today().isoformat()

        # 1. Mark all existing rows stale
        execute_update(
            "UPDATE fact_aggregation_snapshot SET is_latest = FALSE WHERE is_latest = TRUE"
        )

        # 2. Stamp portfolio-level rows and set fund_id=''
        port_result["fund_id"] = ""
        port_result["snapshot_id"] = snapshot_id
        port_result["snapshot_date"] = snapshot_date
        port_result["is_latest"] = True

        # 3. Fund-level aggregation
        fund_group_cols = ["run_id", "portfolio_id", "fund_id", "as_of_date"]
        fund_result = _run_aggregation(exposures, fund_group_cols, taxonomy_type_configs)
        fund_result = _compute_coverage_pct(
            fund_result,
            ["run_id", "portfolio_id", "fund_id", "as_of_date", "taxonomy_type"],
        )
        fund_result["snapshot_id"] = snapshot_id
        fund_result["snapshot_date"] = snapshot_date
        fund_result["is_latest"] = True

        # 4. Combine, deduplicate, and upsert
        all_records = pd.concat([port_result, fund_result], ignore_index=True)
        pk_cols = ["snapshot_id", "portfolio_id", "fund_id", "taxonomy_type", "taxonomy_node_id"]
        before = len(all_records)
        all_records = all_records.drop_duplicates(subset=pk_cols, keep="last")
        dropped = before - len(all_records)
        if dropped:
            print(f"Dropped {dropped} duplicate rows before insert")
        upsert_rows(
            FactAggregationSnapshot,
            dataframe_to_records(all_records),
            pk_cols,
        )

        # 5. Cleanup: remove snapshots older than 24 months
        today = _date_cls.today()
        try:
            cutoff = today.replace(year=today.year - 2).isoformat()
        except ValueError:  # Feb 29 leap-year edge case
            cutoff = today.replace(year=today.year - 2, day=28).isoformat()
        n_deleted = execute_update(
            "DELETE FROM fact_aggregation_snapshot WHERE snapshot_date < :cutoff",
            {"cutoff": cutoff},
        )
        if n_deleted:
            print(f"Cleaned up {n_deleted} rows older than 24 months")

        print(
            f"Appended snapshot {snapshot_id} for {snapshot_date}. "
            f"Total rows: {len(all_records)}"
        )
        print(f"Rows (portfolio-level): {len(port_result)}")

    return port_result


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate exposures")
    parser.add_argument("--csv", action="store_true", help="Use CSV mode instead of PostgreSQL")
    args = parser.parse_args()

    # Check CSV mode from args or environment
    csv_mode = args.csv or _is_csv_mode()
    print(f"Data mode: {'CSV' if csv_mode else 'PostgreSQL'}")

    aggregate_exposures_v1(csv_mode=csv_mode)


if __name__ == "__main__":
    main()
