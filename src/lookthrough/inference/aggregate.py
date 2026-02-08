"""
V1 Aggregation: Produce fact_aggregation_snapshot from inferred exposures.

Reads:
  - data/gold/fact_inferred_exposure.csv
  - data/silver/dim_taxonomy_node.csv
  - data/silver/dim_company.csv

Writes:
  - data/gold/fact_aggregation_snapshot.csv

Aggregates exposures by:
  - taxonomy_type = "sector"    -> level 1 sector nodes
  - taxonomy_type = "industry"  -> level 2 sector nodes (industries)
  - taxonomy_type = "geography" -> level 2 geography nodes (countries)

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
            }

        company_id = str(company_id)
        company = company_lookup.get(company_id, {})

        industry_node_id = company.get("industry_taxonomy_node_id")
        country_node_id = company.get("country_taxonomy_node_id")

        # Industry node ID (level 2 sector) - use directly or Unknown
        if industry_node_id and not pd.isna(industry_node_id):
            industry_resolved = str(industry_node_id)
        else:
            industry_resolved = UNKNOWN_TAXONOMY_NODE_ID

        # Sector node ID (level 1) - get parent of industry
        sector_resolved = _get_sector_node_id(industry_node_id, taxonomy_lookup)

        # Geography node ID (country, level 2)
        if country_node_id and not pd.isna(country_node_id):
            geography_resolved = str(country_node_id)
        else:
            geography_resolved = UNKNOWN_TAXONOMY_NODE_ID

        return {
            "sector_node_id": sector_resolved,
            "industry_node_id": industry_resolved,
            "geography_node_id": geography_resolved,
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
        ("sector", "sector_node_id"),
        ("industry", "industry_node_id"),
        ("geography", "geography_node_id"),
    ]

    for taxonomy_type, node_col in taxonomy_type_configs:
        # Group by run_id, portfolio_id, as_of_date, taxonomy_node_id
        grouped = exposures.groupby(group_cols + [node_col])

        for keys, group in grouped:
            run_id, portfolio_id, as_of_date, taxonomy_node_id = keys

            total_exposure_value_usd = group["exposure_value_usd"].sum()

            # Count total exposures vs those with known classification
            total_count = len(group)
            known_count = (group[node_col] != UNKNOWN_TAXONOMY_NODE_ID).sum()

            # Simple coverage_pct: fraction of exposures with known classification
            # (For a more sophisticated metric, weight by exposure_value_usd)
            coverage_pct = known_count / total_count if total_count > 0 else 0.0

            # confidence_weighted_exposure: simple V1 = total for known, 0 for unknown
            if taxonomy_node_id != UNKNOWN_TAXONOMY_NODE_ID:
                confidence_weighted_exposure = total_exposure_value_usd
            else:
                confidence_weighted_exposure = 0.0

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
                    "coverage_pct": coverage_pct,
                    "confidence_weighted_exposure": confidence_weighted_exposure,
                }
            )

    result = pd.DataFrame(aggregation_rows)

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
