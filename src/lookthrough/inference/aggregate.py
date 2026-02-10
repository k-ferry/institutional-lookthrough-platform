"""
V1 Aggregation: Produce fact_aggregation_snapshot from inferred exposures.

Reads:
  - data/gold/fact_inferred_exposure.csv
  - data/silver/dim_taxonomy_node.csv
  - data/silver/dim_company.csv
  - data/gold/fact_exposure_classification.csv (optional, AI classifications)

Writes:
  - data/gold/fact_aggregation_snapshot.csv

Aggregates exposures by:
  - taxonomy_type = "sector"    -> level 1 sector nodes
  - taxonomy_type = "industry"  -> level 2 sector nodes (industries)
  - taxonomy_type = "geography" -> level 2 geography nodes (countries)

If AI classifications exist, they override deterministic company lookups for
industry classification, and provide confidence scores for weighted metrics.

Run via: python -m src.lookthrough.inference.aggregate
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

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


def _load_classifications(gold_path: Path) -> dict:
    """
    Load AI classifications if available.

    Returns lookup: (company_id, taxonomy_type) -> {taxonomy_node_id, confidence}
    """
    classification_file = gold_path / "fact_exposure_classification.csv"
    if not classification_file.exists():
        return {}

    df = pd.read_csv(classification_file)
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


def aggregate_exposures_v1() -> pd.DataFrame:
    """
    V1 aggregation: group inferred exposures by taxonomy buckets.

    Produces one row per (run_id, portfolio_id, as_of_date, taxonomy_type, taxonomy_node_id).
    """
    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    # Load inputs
    exposures = _read_csv(gold / "fact_inferred_exposure.csv")
    companies = _read_csv(silver / "dim_company.csv")
    taxonomy = _read_csv(silver / "dim_taxonomy_node.csv")

    taxonomy_lookup = _build_taxonomy_lookup(taxonomy)

    # Load AI classifications if available
    classification_lookup = _load_classifications(gold)
    use_ai_classifications = len(classification_lookup) > 0
    if use_ai_classifications:
        print(f"Using AI classifications: {len(classification_lookup)} entries")

    # Build company lookup: company_id -> {industry_taxonomy_node_id, country_taxonomy_node_id}
    company_lookup = {}
    for _, row in companies.iterrows():
        cid = str(row["company_id"])
        company_lookup[cid] = {
            "industry_taxonomy_node_id": row.get("industry_taxonomy_node_id"),
            "country_taxonomy_node_id": row.get("country_taxonomy_node_id"),
        }

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

        # Check for AI classification for industry
        ai_industry = classification_lookup.get((company_id, "industry"))
        if ai_industry:
            industry_resolved = ai_industry["taxonomy_node_id"]
            industry_confidence = ai_industry["confidence"]
        else:
            # Fallback to deterministic lookup
            industry_node_id = company.get("industry_taxonomy_node_id")
            if industry_node_id and not pd.isna(industry_node_id):
                industry_resolved = str(industry_node_id)
                industry_confidence = 1.0  # Deterministic = full confidence
            else:
                industry_resolved = UNKNOWN_TAXONOMY_NODE_ID
                industry_confidence = 0.0

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

    # Prepare aggregation output
    aggregation_rows = []

    # Group keys
    group_cols = ["run_id", "portfolio_id", "as_of_date"]

    # Aggregate for each taxonomy type
    taxonomy_type_configs = [
        ("sector", "sector_node_id", "sector_confidence"),
        ("industry", "industry_node_id", "industry_confidence"),
        ("geography", "geography_node_id", "geography_confidence"),
    ]

    for taxonomy_type, node_col, conf_col in taxonomy_type_configs:
        # Group by run_id, portfolio_id, as_of_date, taxonomy_node_id
        grouped = exposures.groupby(group_cols + [node_col])

        for keys, group in grouped:
            run_id, portfolio_id, as_of_date, taxonomy_node_id = keys

            total_exposure_value_usd = group["exposure_value_usd"].sum()

            # confidence_weighted_exposure = sum(exposure_value_usd * confidence)
            confidence_weighted_exposure = (
                group["exposure_value_usd"] * group[conf_col]
            ).sum()

            aggregation_rows.append(
                {
                    "run_id": run_id,
                    "portfolio_id": portfolio_id,
                    "as_of_date": as_of_date,
                    "taxonomy_type": taxonomy_type,
                    "taxonomy_node_id": taxonomy_node_id,
                    "total_exposure_value_usd": total_exposure_value_usd,
                    "total_exposure_p10": np.nan,  # Not computed in V1
                    "total_exposure_p90": np.nan,  # Not computed in V1
                    "coverage_pct": np.nan,  # Computed below at portfolio level
                    "confidence_weighted_exposure": confidence_weighted_exposure,
                }
            )

    # Compute coverage_pct at portfolio level per (run_id, portfolio_id, as_of_date, taxonomy_type)
    # coverage_pct = sum(exposure where known) / sum(all exposure)
    result = pd.DataFrame(aggregation_rows)
    if len(result) > 0:
        # Calculate total exposure per (run_id, portfolio_id, as_of_date, taxonomy_type)
        portfolio_totals = result.groupby(
            ["run_id", "portfolio_id", "as_of_date", "taxonomy_type"]
        )["total_exposure_value_usd"].transform("sum")

        # Known exposure = total where taxonomy_node_id != UNKNOWN
        is_known = result["taxonomy_node_id"] != UNKNOWN_TAXONOMY_NODE_ID
        result["_known_exposure"] = result["total_exposure_value_usd"].where(is_known, 0.0)

        known_totals = result.groupby(
            ["run_id", "portfolio_id", "as_of_date", "taxonomy_type"]
        )["_known_exposure"].transform("sum")

        result["coverage_pct"] = (known_totals / portfolio_totals).fillna(0.0)
        result = result.drop(columns=["_known_exposure"])

    # Sort deterministically for reproducibility
    result = result.sort_values(
        ["run_id", "portfolio_id", "as_of_date", "taxonomy_type", "taxonomy_node_id"]
    ).reset_index(drop=True)

    # Write output
    out_path = gold / "fact_aggregation_snapshot.csv"
    result.to_csv(out_path, index=False)

    print(f"Wrote: {out_path}")
    print(f"Rows: {len(result)}")

    return result


def main() -> None:
    aggregate_exposures_v1()


if __name__ == "__main__":
    main()
