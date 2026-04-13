"""LP exposure scaling — compute Northbridge Endowment Fund's scaled exposure.

Uses a position-percentage approach:
  holding_pct_of_fund = holding_value / total_fund_value
  scaled_value        = lp_nav × holding_pct_of_fund

This is more robust than an ownership-% approach because it never requires
knowing total fund size accurately, and works correctly even when holdings
are a partial snapshot of the full portfolio.

LP NAV sources (per fund+quarter):
  - PDF funds (source=pdf_document):   fact_fund_report.nav_usd
                                        — quarter is skipped if nav_usd is NULL
  - Named LP positions (LP_POSITIONS): linear interpolation across 8 quarters
  - All other funds:                   synthetic proxy = 5% × SUM(holdings)

Denominator (total_fund_value):
  - pdf_document funds with total_net_assets_usd: use that (more accurate)
  - All others: SUM(reported_value_usd) for that fund+quarter

The ownership_pct column in fact_lp_scaled_exposure stores holding_pct_of_fund
(the holding's share of the fund), not the LP's ownership % of the fund.
fact_lp_position.ownership_pct = lp_nav / total_fund_value is retained for
reference but is no longer the basis of scaling.

Usage:
    python -m src.lookthrough.inference.scale_exposure
"""

from __future__ import annotations

import hashlib
import logging
import uuid

import pandas as pd
from sqlalchemy import text as sa_text

from src.lookthrough.db.engine import get_session_context
from src.lookthrough.db.models import FactLpPosition, FactLpScaledExposure
from src.lookthrough.db.repository import execute_query, upsert_rows

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LP_NAME = "Northbridge Endowment Fund"
SYNTHETIC_OWNERSHIP_PCT = 0.05
MAX_INTERP_QUARTERS = 8

# Northbridge's LP positions in BDC / 13F / ETF funds.
# nav_usd  = approximate entry NAV (earliest quarter)
# end_nav  = approximate exit / latest NAV (most recent quarter)
# Interpolated linearly across up to 8 quarters of available data.
LP_POSITIONS: dict[str, dict] = {
    "ARCC Capital":              {"nav_usd": 62_000_000, "end_nav": 69_000_000},
    "MAIN Capital":              {"nav_usd": 40_000_000, "end_nav": 45_000_000},
    "OBDC Capital":              {"nav_usd": 34_000_000, "end_nav": 38_000_000},
    "Brightline Innovation ETF": {"nav_usd": 11_000_000, "end_nav": 14_000_000},
    "Vertex Macro Fund":         {"nav_usd": 13_000_000, "end_nav": 15_000_000},
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_uuid(seed: str) -> str:
    """Deterministic UUID from an arbitrary seed string."""
    digest = hashlib.md5(seed.encode()).hexdigest()
    return str(uuid.UUID(digest))


def _interpolate_nav(cfg: dict, idx: int, n_quarters: int) -> float:
    """
    Linear interpolation from cfg["nav_usd"] to cfg["end_nav"].

    idx         — 0-based position of this quarter in the fund's sorted date list
    n_quarters  — total number of quarters available (capped at MAX_INTERP_QUARTERS)
    """
    start = cfg["nav_usd"]
    end = cfg["end_nav"]
    steps = min(n_quarters, MAX_INTERP_QUARTERS) - 1
    if steps <= 0:
        return float(start)
    t = min(idx, steps) / steps
    return start + t * (end - start)


def _clean(val) -> float | None:
    """Convert NaN / None to None, otherwise return float."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _run_migrations() -> None:
    """Create LP scaling tables if they don't exist. Safe to run repeatedly."""
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS fact_lp_position (
            position_id          VARCHAR(36)  PRIMARY KEY,
            lp_name              VARCHAR(255),
            fund_id              VARCHAR(36),
            as_of_date           VARCHAR(20),
            nav_usd              FLOAT,
            total_fund_nav_usd   FLOAT,
            ownership_pct        FLOAT,
            source               VARCHAR(50)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_lp_scaled_exposure (
            scaled_exposure_id   VARCHAR(36)  PRIMARY KEY,
            fund_id              VARCHAR(36),
            company_id           VARCHAR(36),
            fund_report_id       VARCHAR(36),
            reported_holding_id  VARCHAR(100),
            as_of_date           VARCHAR(20),
            raw_value_usd        FLOAT,
            ownership_pct        FLOAT,
            scaled_value_usd     FLOAT,
            lp_name              VARCHAR(255) DEFAULT 'Northbridge Endowment Fund',
            source               VARCHAR(50)
        )
        """,
    ]
    with get_session_context() as session:
        for stmt in stmts:
            session.execute(sa_text(stmt))
        session.commit()
    logger.info("LP scaling tables verified.")


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_lp_scaled_exposure(db_mode: bool = True) -> None:
    """
    Compute Northbridge's LP-scaled exposure using a position-percentage approach.

    For each holding in each fund+quarter:
      holding_pct_of_fund = holding_value / total_fund_value
      scaled_value        = lp_nav × holding_pct_of_fund

    Writes:
      - fact_lp_position        — one row per (fund, quarter)
      - fact_lp_scaled_exposure — one row per holding with scaled_value_usd
    """
    _run_migrations()

    # ------------------------------------------------------------------
    # 1. Load all positive-value holdings with their fund / report context
    # ------------------------------------------------------------------
    holdings_df = execute_query(
        """
        SELECT
            frh.reported_holding_id,
            frh.fund_report_id,
            ffr.fund_id,
            frh.company_id,
            frh.raw_company_name,
            frh.reported_value_usd,
            COALESCE(frh.as_of_date, ffr.report_period_end) AS as_of_date,
            frh.source,
            df.fund_name,
            df.fund_type,
            df.source AS fund_source
        FROM  fact_reported_holding  frh
        JOIN  fact_fund_report       ffr ON frh.fund_report_id = ffr.fund_report_id
        JOIN  dim_fund               df  ON ffr.fund_id = df.fund_id
        WHERE frh.reported_value_usd IS NOT NULL
          AND frh.reported_value_usd > 0
        """
    )

    if holdings_df.empty:
        logger.warning("No holdings found — nothing to scale.")
        return

    logger.info("Loaded %d holdings across %d funds.",
                len(holdings_df), holdings_df["fund_id"].nunique())

    # ------------------------------------------------------------------
    # 2. Load fact_fund_report aggregated by (fund_id, period_end).
    #    MAX() recovers both nav_usd and total_net_assets_usd even when
    #    two doc types share the same fund_report_id and overwrite each other.
    # ------------------------------------------------------------------
    fund_reports_df = execute_query(
        """
        SELECT
            fund_id,
            report_period_end          AS as_of_date,
            MAX(nav_usd)               AS nav_usd,
            MAX(total_net_assets_usd)  AS total_net_assets_usd
        FROM  fact_fund_report
        GROUP BY fund_id, report_period_end
        """
    )

    fund_report_lookup: dict[tuple[str, str], dict] = {
        (str(r.fund_id), str(r.as_of_date)): {
            "nav_usd":             _clean(r.nav_usd),
            "total_net_assets_usd": _clean(r.total_net_assets_usd),
        }
        for r in fund_reports_df.itertuples(index=False)
    }

    # ------------------------------------------------------------------
    # 3. Process each fund
    # ------------------------------------------------------------------
    position_records: list[dict] = []
    scaled_records: list[dict] = []

    for fund_id, fund_group in holdings_df.groupby("fund_id"):
        fund_id = str(fund_id)
        fund_name: str = fund_group["fund_name"].iloc[0]
        fund_source: str = str(fund_group["fund_source"].iloc[0] or "")

        # Sort all quarters for this fund (needed for interpolation index)
        dates = sorted(fund_group["as_of_date"].dropna().unique())
        n_quarters = len(dates)

        # Per-fund accumulators for the summary log line
        fund_navs: list[float] = []
        fund_denominators: list[float] = []
        # Best sample = (company_name, holding_pct, scaled_value) — largest scaled value
        best_sample: tuple[str, float, float] | None = None

        for date_idx, as_of_date in enumerate(dates):
            date_holdings = fund_group[fund_group["as_of_date"] == as_of_date]
            total_holdings_value: float = float(date_holdings["reported_value_usd"].sum())

            # ---- Step 1: Determine LP NAV for this fund+quarter ----
            #
            # Priority order:
            #   1. LP_POSITIONS config (explicit NAV range) — takes precedence over
            #      dim_fund.source because BDC parsers may write the fund's own total
            #      assets into fact_fund_report.nav_usd, which is not Northbridge's NAV.
            #   2. PDF path (source=pdf_document, not in LP_POSITIONS) — uses
            #      fact_fund_report.nav_usd from LP statement ingestion.
            #   3. Synthetic proxy — 5% × holdings sum for everything else.

            if fund_name in LP_POSITIONS:
                # Named BDC / 13F / ETF position — interpolate from config
                lp_nav = _interpolate_nav(LP_POSITIONS[fund_name], date_idx, n_quarters)
                pos_source = "config"

            elif fund_source == "pdf_document":
                # Private PE/VC/credit/hedge fund — nav_usd from LP statement
                report = fund_report_lookup.get((fund_id, as_of_date), {})
                lp_nav = report.get("nav_usd")
                if not lp_nav:
                    logger.warning(
                        "  Skipping %s %s — nav_usd is NULL "
                        "(ingest LP statement PDF to populate)",
                        fund_name, as_of_date,
                    )
                    continue
                pos_source = "pdf_document"

            else:
                # Synthetic / unknown — proxy NAV = 5% of holdings sum
                lp_nav = SYNTHETIC_OWNERSHIP_PCT * total_holdings_value
                pos_source = "hardcoded"

            # ---- Step 2: Determine denominator (total_fund_value) ----
            #
            # PDF private funds: prefer total_net_assets_usd (full fund size from
            # balance sheet) over the sum of Schedule-of-Investments holdings, which
            # may not cover 100% of assets. LP_POSITIONS and synthetic funds always
            # use the holdings sum — their denominators don't come from fact_fund_report.
            if fund_source == "pdf_document" and fund_name not in LP_POSITIONS:
                report = fund_report_lookup.get((fund_id, as_of_date), {})
                total_fund_value = (
                    report.get("total_net_assets_usd") or total_holdings_value
                )
            else:
                total_fund_value = total_holdings_value

            if not total_fund_value or total_fund_value <= 0:
                logger.warning(
                    "  Skipping %s %s — zero total fund value", fund_name, as_of_date
                )
                continue

            # ---- Step 3: Upsert fact_lp_position ----
            # ownership_pct here = lp_nav / total (kept for reference only)
            ref_ownership_pct = lp_nav / total_fund_value
            position_records.append({
                "position_id":       _make_uuid(f"lp_pos_{fund_id}_{as_of_date}"),
                "lp_name":           LP_NAME,
                "fund_id":           fund_id,
                "as_of_date":        as_of_date,
                "nav_usd":           lp_nav,
                "total_fund_nav_usd": total_fund_value,
                "ownership_pct":     ref_ownership_pct,
                "source":            pos_source,
            })

            fund_navs.append(lp_nav)
            fund_denominators.append(total_fund_value)

            # ---- Step 4: Build fact_lp_scaled_exposure rows ----
            for _, holding in date_holdings.iterrows():
                raw_value = _clean(holding["reported_value_usd"]) or 0.0
                holding_pct = raw_value / total_fund_value      # % of this fund
                scaled_value = lp_nav * holding_pct             # Northbridge exposure

                company_id = holding["company_id"]
                if not isinstance(company_id, str) and pd.isna(company_id):
                    company_id = None

                scaled_records.append({
                    "scaled_exposure_id":  _make_uuid(
                        f"lp_scaled_{holding['reported_holding_id']}"
                    ),
                    "fund_id":             fund_id,
                    "company_id":          company_id or None,
                    "fund_report_id":      str(holding["fund_report_id"]),
                    "reported_holding_id": str(holding["reported_holding_id"]),
                    "as_of_date":          as_of_date,
                    "raw_value_usd":       raw_value,
                    # ownership_pct now stores holding_pct_of_fund, not LP ownership %
                    "ownership_pct":       holding_pct,
                    "scaled_value_usd":    scaled_value,
                    "lp_name":             LP_NAME,
                    "source":              str(holding["source"] or fund_source),
                })

                # Track largest-exposure holding as a sample for the log
                if best_sample is None or scaled_value > best_sample[2]:
                    cname = str(holding.get("raw_company_name") or "").strip()
                    best_sample = (cname or "unknown", holding_pct, scaled_value)

        # ---- Per-fund log line ----
        if fund_navs:
            nav_lo = min(fund_navs) / 1e6
            nav_hi = max(fund_navs) / 1e6
            nav_range = f"${nav_lo:.0f}M" if nav_lo == nav_hi else f"${nav_lo:.0f}M–${nav_hi:.0f}M"
            avg_denom = sum(fund_denominators) / len(fund_denominators)
            sample_str = ""
            if best_sample:
                cname, pct, val = best_sample
                sample_str = (
                    f", sample: {cname[:25]} {pct * 100:.2f}% of fund "
                    f"→ ${val / 1_000:.0f}K Northbridge exposure"
                )
            logger.info(
                "  %-40s  %d qtrs, LP NAV %s, avg holding pct denominator $%.1fB%s",
                fund_name, len(fund_navs), nav_range, avg_denom / 1e9, sample_str,
            )

    # ------------------------------------------------------------------
    # 4. Upsert everything
    # ------------------------------------------------------------------
    if position_records:
        upsert_rows(FactLpPosition, position_records, ["position_id"])
    if scaled_records:
        upsert_rows(FactLpScaledExposure, scaled_records, ["scaled_exposure_id"])

    # ------------------------------------------------------------------
    # 5. Grand summary
    # ------------------------------------------------------------------
    n_funds = holdings_df["fund_id"].nunique()
    total_scaled = sum(r["scaled_value_usd"] for r in scaled_records)
    logger.info(
        "LP scaling complete — %d funds, %d fund-quarter positions, "
        "%d scaled holdings, total Northbridge exposure $%.1fM",
        n_funds,
        len(position_records),
        len(scaled_records),
        total_scaled / 1e6,
    )


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    compute_lp_scaled_exposure()
