"""Generate historical (synthetic) snapshots for time-series trend analysis.

Loads the latest portfolio-level and fund-level snapshots from the DB,
applies backward drift to simulate 8 quarters of historical data
(Q1 2023 – Q4 2024), and inserts the records with is_latest=False.

Prerequisites:
    Run 'python -m src.lookthrough.inference.aggregate' at least once to
    create the current snapshot before running this script.

Usage:
    python -m src.lookthrough.synthetic.generate_historical_snapshots
    python -m src.lookthrough.synthetic.generate_historical_snapshots --dry-run
"""
from __future__ import annotations

import argparse
import hashlib
import uuid

import numpy as np
import pandas as pd

from src.lookthrough.db.models import FactAggregationSnapshot
from src.lookthrough.db.repository import (
    bulk_insert,
    execute_query,
)

# Quarter-end dates to seed (oldest first, Q1 2023 → Q4 2024)
QUARTER_DATES = [
    "2023-01-01",
    "2023-04-01",
    "2023-07-01",
    "2023-10-01",
    "2024-01-01",
    "2024-04-01",
    "2024-07-01",
    "2024-10-01",
]

# Drift rates per quarter going *backward* from current snapshot.
# Negative = allocation grew over time (was smaller in 2023 → story: tech/health grew).
# Positive = allocation shrank over time (was larger in 2023 → story: energy/utilities declined).
#
# Magnitudes are intentionally large (5–15% per quarter total across 7 steps) so the
# trend is clearly visible on a chart without needing many snapshots.
#
# Matching is substring-based (case-insensitive), first match wins.
SECTOR_DRIFT_TABLE: list[tuple[str, float]] = [
    # Trending UP  — subtract going backward so 2023 values are lower
    ("technology",             -0.030),
    ("information technology", -0.030),
    ("healthcare",             -0.025),
    ("health care",            -0.025),
    ("communication",          -0.020),
    # Relatively STABLE — small signed drift
    ("financials",             +0.010),
    ("finance",                +0.010),
    ("industrials",            -0.010),
    # Trending DOWN — add going backward so 2023 values are higher
    ("energy",                 +0.030),
    ("utilities",              +0.025),
    ("real estate",            +0.020),
]


def _drift_rate(node_name: str, node_id: str, taxonomy_type: str) -> float:
    """Return backward-drift rate per quarter for a taxonomy node.

    For sector taxonomy: uses SECTOR_DRIFT_TABLE (first substring match wins),
    with a deterministic hash fallback for unlisted sectors.
    For geography/industry: smaller hash-based drift so those charts show
    moderate movement without overwhelming the sector story.
    """
    if taxonomy_type == "sector":
        name_lower = str(node_name).lower()
        for keyword, rate in SECTOR_DRIFT_TABLE:
            if keyword in name_lower:
                return rate
        # Unlisted sectors: deterministic ±5–10% total drift
        h = int(hashlib.md5(node_id.encode()).hexdigest()[:6], 16)
        magnitude = 0.008 + (h % 1000) / 50_000   # 0.008 – 0.028
        sign = 1 if h % 2 == 0 else -1
        return sign * magnitude
    else:
        # Geography / industry: moderate drift so trend is visible but secondary
        h = int(hashlib.md5(node_id.encode()).hexdigest()[:6], 16)
        magnitude = 0.005 + (h % 1000) / 100_000  # 0.005 – 0.015
        sign = 1 if h % 2 == 0 else -1
        return sign * magnitude


def _apply_drift(group_df: pd.DataFrame, n_steps: int) -> pd.DataFrame:
    """Adjust a group's exposure values by n_steps quarters of backward drift.

    Fractions are shifted by drift_rate * n_steps, clipped to a floor of 0.001
    to avoid zero allocations, then renormalized so absolute USD totals are
    preserved.
    """
    group_df = group_df.copy()
    total = group_df["total_exposure_value_usd"].sum()
    if total <= 0:
        return group_df

    fracs = group_df["total_exposure_value_usd"] / total
    drift_rates = group_df.apply(
        lambda r: _drift_rate(r["node_name"], r["taxonomy_node_id"], r["taxonomy_type"]),
        axis=1,
    )
    new_fracs = (fracs + drift_rates * n_steps).clip(lower=0.001)
    new_fracs = new_fracs / new_fracs.sum()  # renormalize

    new_values = new_fracs * total
    # Scale confidence_weighted_exposure proportionally
    orig = group_df["total_exposure_value_usd"].replace(0, np.nan)
    scale = new_values / orig
    group_df["total_exposure_value_usd"] = new_values.values
    cwe = group_df["confidence_weighted_exposure"].fillna(0) * scale.fillna(1)
    group_df["confidence_weighted_exposure"] = cwe.values
    return group_df


def _build_record(
    row: pd.Series,
    snap_id: str,
    fund_id: str,
    qdate: str,
) -> dict:
    cwe = row.get("confidence_weighted_exposure")
    cvg = row.get("coverage_pct")
    return {
        "snapshot_id": snap_id,
        "portfolio_id": str(row["portfolio_id"]),
        "fund_id": fund_id,
        "taxonomy_type": str(row["taxonomy_type"]),
        "taxonomy_node_id": str(row["taxonomy_node_id"]),
        "run_id": snap_id,
        "as_of_date": qdate,
        "snapshot_date": qdate,
        "is_latest": False,
        "total_exposure_value_usd": float(row["total_exposure_value_usd"]),
        "total_exposure_p10": None,
        "total_exposure_p90": None,
        "coverage_pct": float(cvg) if cvg is not None and not pd.isna(cvg) else None,
        "confidence_weighted_exposure": (
            float(cwe) if cwe is not None and not pd.isna(cwe) else None
        ),
    }


def generate_historical_snapshots(dry_run: bool = False) -> int:
    """Seed 8 quarters of historical snapshot data.

    Returns:
        Total number of records generated.
    """
    # Load latest portfolio-level snapshot (all taxonomy types)
    port_df = execute_query(
        """
        SELECT fas.portfolio_id,
               fas.fund_id,
               fas.run_id,
               fas.as_of_date,
               fas.taxonomy_type,
               fas.taxonomy_node_id,
               fas.total_exposure_value_usd,
               fas.coverage_pct,
               fas.confidence_weighted_exposure,
               dtn.node_name
        FROM fact_aggregation_snapshot fas
        JOIN dim_taxonomy_node dtn
          ON fas.taxonomy_node_id = dtn.taxonomy_node_id
        WHERE fas.fund_id = '' AND fas.is_latest = TRUE
        """
    )

    if port_df.empty:
        print(
            "ERROR: No portfolio-level snapshot found (fund_id='', is_latest=TRUE).\n"
            "Run 'python -m src.lookthrough.inference.aggregate' first."
        )
        return 0

    # Load latest fund-level snapshot
    fund_df = execute_query(
        """
        SELECT fas.portfolio_id,
               fas.fund_id,
               fas.run_id,
               fas.as_of_date,
               fas.taxonomy_type,
               fas.taxonomy_node_id,
               fas.total_exposure_value_usd,
               fas.coverage_pct,
               fas.confidence_weighted_exposure,
               dtn.node_name
        FROM fact_aggregation_snapshot fas
        JOIN dim_taxonomy_node dtn
          ON fas.taxonomy_node_id = dtn.taxonomy_node_id
        WHERE fas.fund_id != '' AND fas.is_latest = TRUE
        """
    )

    all_records: list[dict] = []

    port_group_keys = ["portfolio_id", "taxonomy_type"]
    fund_group_keys = ["portfolio_id", "fund_id", "taxonomy_type"]

    for i, qdate in enumerate(QUARTER_DATES):
        # Q4 2024 (last in list) = 0 steps back; Q1 2023 = 7 steps back
        n_steps = len(QUARTER_DATES) - 1 - i
        snap_id = str(uuid.uuid4())

        # Portfolio-level
        for _, group in port_df.groupby(port_group_keys):
            adj = _apply_drift(group, n_steps)
            for _, row in adj.iterrows():
                all_records.append(_build_record(row, snap_id, "", qdate))

        # Fund-level
        if not fund_df.empty:
            for _, group in fund_df.groupby(fund_group_keys):
                adj = _apply_drift(group, n_steps)
                fid = str(adj["fund_id"].iloc[0])
                for _, row in adj.iterrows():
                    all_records.append(_build_record(row, snap_id, fid, qdate))

    n = len(all_records)
    print(f"Generated {n} historical rows across {len(QUARTER_DATES)} quarters")

    if dry_run:
        print("[DRY RUN] No records written to database.")
    else:
        if all_records:
            bulk_insert(FactAggregationSnapshot, all_records)
            print(f"Seeded {n} historical snapshot rows.")

    return n


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed historical snapshot data for time-series trend analysis"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without writing to database",
    )
    args = parser.parse_args()
    generate_historical_snapshots(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
