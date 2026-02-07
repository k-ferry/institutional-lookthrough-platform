from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
import uuid

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class InferenceConfig:
    """
    V1 inference assumptions (deterministic baseline).

    portfolio_total_value_usd:
        Total portfolio value used to translate fund weights into dollar exposure.
        In real usage, this would come from portfolio accounting/positions.
    fund_weight_method:
        How to allocate portfolio across funds for V1.
        - "equal": equal weight across funds present in the quarter
    scale_exposure_to_nav:
        If true, normalize holdings weights so they sum to 1.0 per fund report.
        If false, allow gross sums != 1.0 (useful for leverage / net short cases later).
    """
    portfolio_total_value_usd: float = 100_000_000.0
    fund_weight_method: str = "equal"
    scale_exposure_to_nav: bool = True


def _repo_root() -> Path:
    # src/lookthrough/inference/exposure.py -> repo root is 4 parents up
    return Path(__file__).resolve().parents[3]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def _safe_float(x) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def _estimate_fund_nav(
    holdings: pd.DataFrame,
    coverage_estimate: Optional[float],
) -> Tuple[float, float]:
    """
    Estimate fund NAV for a fund report.

    Returns:
        nav_estimate_usd, covered_value_usd

    Logic (V1):
    - covered_value_usd = sum(reported_value_usd where present) +
                         (sum(reported_pct_nav where present) * nav_estimate_guess if needed)
    - If reported_value_usd is present, that dominates.
    - If only pct_nav is present, we approximate NAV using pct sums if possible.

    Note: In real systems NAV comes directly from statements.
    """
    # Covered value from reported_value_usd
    values = holdings["reported_value_usd"].apply(_safe_float)
    covered_value_usd = float(np.nansum([v for v in values if v is not None]))

    # If we have no values but we have pct_nav, approximate NAV from pct_nav totals.
    pct = holdings["reported_pct_nav"].apply(_safe_float)
    pct_vals = [p for p in pct if p is not None]
    pct_sum = float(np.nansum(pct_vals)) if pct_vals else 0.0

    nav_estimate = None

    # If we have dollar values and coverage estimate, NAV ≈ covered_value / coverage
    if covered_value_usd > 0 and coverage_estimate and coverage_estimate > 0:
        nav_estimate = covered_value_usd / coverage_estimate

    # If no value coverage, but pct_sum exists, NAV is not identifiable from pct alone.
    # We'll fallback to NAV=covered_value if any, else NAV=1 to avoid divide-by-zero.
    if nav_estimate is None:
        nav_estimate = covered_value_usd if covered_value_usd > 0 else 1.0

    return float(nav_estimate), float(covered_value_usd)


def infer_exposures_v1(cfg: InferenceConfig) -> pd.DataFrame:
    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    # Required inputs
    portfolio = _read_csv(silver / "dim_portfolio.csv")
    fund_reports = _read_csv(silver / "fact_fund_report.csv")
    holdings = _read_csv(silver / "fact_reported_holding.csv")

    # Minimal required columns checks (fail fast)
    for col in ["portfolio_id"]:
        if col not in portfolio.columns:
            raise ValueError(f"dim_portfolio missing column: {col}")

    for col in ["fund_report_id", "fund_id", "report_period_end"]:
        if col not in fund_reports.columns:
            raise ValueError(f"fact_fund_report missing column: {col}")

    for col in ["fund_report_id", "raw_company_name"]:
        if col not in holdings.columns:
            raise ValueError(f"fact_reported_holding missing column: {col}")

    # Use first (and only) portfolio in V1
    portfolio_id = str(portfolio.loc[0, "portfolio_id"])

    # Determine fund weights per quarter (V1: equal weight across funds reporting that quarter)
    # We infer quarters from fund_reports.report_period_end
    fund_reports["report_period_end"] = pd.to_datetime(fund_reports["report_period_end"]).dt.date

    exposures_out = []

    run_id = str(uuid.uuid4())
    method = "deterministic_v1"
    exposure_type = "lookthrough"

    # Build per-quarter fund sets
    for as_of_date, fr_q in fund_reports.groupby("report_period_end"):
        fund_ids = fr_q["fund_id"].astype(str).unique().tolist()
        if len(fund_ids) == 0:
            continue

        if cfg.fund_weight_method != "equal":
            raise ValueError(f"Unsupported fund_weight_method in V1: {cfg.fund_weight_method}")

        fund_weight = 1.0 / len(fund_ids)
        fund_alloc_value = cfg.portfolio_total_value_usd * fund_weight

        # For each fund report in that quarter
        for _, fr in fr_q.iterrows():
            fund_report_id = str(fr["fund_report_id"])
            fund_id = str(fr["fund_id"])
            coverage_est = _safe_float(fr["coverage_estimate"]) if "coverage_estimate" in fr_q.columns else None

            h = holdings[holdings["fund_report_id"].astype(str) == fund_report_id].copy()

            # If company_id exists, prefer it; else we keep raw name (company_id will be null)
            if "company_id" in h.columns:
                h["company_id"] = h["company_id"].astype(str)
            else:
                h["company_id"] = None

            # Ensure numeric columns exist
            if "reported_value_usd" not in h.columns:
                h["reported_value_usd"] = np.nan
            if "reported_pct_nav" not in h.columns:
                h["reported_pct_nav"] = np.nan

            nav_est, covered_value_usd = _estimate_fund_nav(h, coverage_estimate=coverage_est)

            # Compute holding value: prefer reported_value_usd; else use pct_nav * nav_est
            def holding_value(row) -> float:
                v = _safe_float(row.get("reported_value_usd", None))
                if v is not None and v > 0:
                    return float(v)
                p = _safe_float(row.get("reported_pct_nav", None))
                if p is not None and p > 0:
                    return float(p) * float(nav_est)
                return 0.0

            h["holding_value_usd"] = h.apply(holding_value, axis=1)

            # If scale_exposure_to_nav=True, normalize by sum of holding_value_usd (covered holdings),
            # otherwise normalize by nav_est (allows gross exposure concepts later).
            denom = float(h["holding_value_usd"].sum())
            if cfg.scale_exposure_to_nav:
                denom = denom if denom > 0 else 1.0
            else:
                denom = nav_est if nav_est > 0 else 1.0

            h["holding_weight"] = h["holding_value_usd"] / denom

            # Translate to portfolio dollar exposure using fund_alloc_value
            h["exposure_value_usd"] = h["holding_weight"] * fund_alloc_value
            h["exposure_weight"] = h["exposure_value_usd"] / cfg.portfolio_total_value_usd

            for _, row in h.iterrows():
                exposures_out.append(
                    {
                        "exposure_id": str(uuid.uuid4()),
                        "run_id": run_id,
                        "portfolio_id": portfolio_id,
                        "fund_id": fund_id,
                        "company_id": row.get("company_id") if row.get("company_id") not in ["nan", "None"] else None,
                        "raw_company_name": row.get("raw_company_name"),
                        "as_of_date": str(as_of_date),
                        "exposure_value_usd": float(row["exposure_value_usd"]),
                        "exposure_weight": float(row["exposure_weight"]),
                        "exposure_type": exposure_type,
                        "method": method,
                    }
                )

    exposures_df = pd.DataFrame(exposures_out)

    # Write output
    out_path = gold / "fact_inferred_exposure.csv"
    exposures_df.to_csv(out_path, index=False)

    print("Wrote:", out_path)
    print("Rows:", len(exposures_df))
    print("run_id:", run_id)

    return exposures_df


def main() -> None:
    cfg = InferenceConfig()
    infer_exposures_v1(cfg)


if __name__ == "__main__":
    main()
