"""Dashboard API endpoints returning aggregated portfolio statistics."""

import csv
import io
import math
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import distinct, func
from sqlalchemy.orm import Session

from src.lookthrough.auth.dependencies import get_current_user, get_db
from src.lookthrough.db.models import (
    DimCompany,
    DimFund,
    DimTaxonomyNode,
    EntityResolutionLog,
    FactAggregationSnapshot,
    FactAuditEvent,
    FactExposureClassification,
    FactFundReport,
    FactLpScaledExposure,
    FactReportedHolding,
    User,
)

LP_NAME = "Northbridge Endowment Fund"


def _latest_per_fund_sq(db: Session):
    """Subquery: (fund_id, max_date) — each fund's latest scaled as_of_date."""
    return (
        db.query(
            FactLpScaledExposure.fund_id.label("fund_id"),
            func.max(FactLpScaledExposure.as_of_date).label("max_date"),
        )
        .filter(FactLpScaledExposure.lp_name == LP_NAME)
        .group_by(FactLpScaledExposure.fund_id)
        .subquery()
    )


def _scaled_totals_by_fund(db: Session) -> dict[str, float]:
    """Return {fund_id: sum(scaled_value_usd)} for each fund at its latest as_of_date."""
    latest_sq = _latest_per_fund_sq(db)
    rows = (
        db.query(
            FactLpScaledExposure.fund_id,
            func.sum(FactLpScaledExposure.scaled_value_usd).label("total"),
        )
        .join(
            latest_sq,
            (FactLpScaledExposure.fund_id == latest_sq.c.fund_id)
            & (FactLpScaledExposure.as_of_date == latest_sq.c.max_date),
        )
        .filter(FactLpScaledExposure.lp_name == LP_NAME)
        .group_by(FactLpScaledExposure.fund_id)
        .all()
    )
    return {str(r.fund_id): float(r.total) if r.total else 0.0 for r in rows}

UNKNOWN_NODE_ID = "00000000-0000-0000-0000-000000000000"

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


# ---------------------------------------------------------------------------
# GET /api/dashboard/stats
# ---------------------------------------------------------------------------


@router.get("/stats")
def get_stats(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Return top-level portfolio statistics with source breakdown."""
    total_holdings = (
        db.query(func.count(FactReportedHolding.reported_holding_id)).scalar() or 0
    )
    # Sum each fund's scaled exposure at its own latest as_of_date (avoids global-max skew)
    total_exposure = sum(_scaled_totals_by_fund(db).values())
    fund_count = db.query(func.count(DimFund.fund_id)).scalar() or 0
    company_count = db.query(func.count(DimCompany.company_id)).scalar() or 0
    quarter_count = (
        db.query(func.count(distinct(FactReportedHolding.as_of_date)))
        .filter(FactReportedHolding.as_of_date.isnot(None))
        .scalar()
        or 0
    )

    # Classification coverage: companies with a non-null primary_sector
    classified = (
        db.query(func.count(DimCompany.company_id))
        .filter(DimCompany.primary_sector.isnot(None))
        .scalar()
        or 0
    )
    classification_coverage_pct = (
        round(classified / company_count * 100, 1) if company_count > 0 else 0.0
    )

    # Per-source breakdown
    source_rows = (
        db.query(
            FactReportedHolding.source,
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.count(distinct(FactFundReport.fund_id)).label("fund_count"),
            func.max(FactReportedHolding.as_of_date).label("latest_as_of_date"),
        )
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .filter(FactReportedHolding.source.isnot(None))
        .group_by(FactReportedHolding.source)
        .all()
    )
    source_breakdown = [
        {
            "source": r.source,
            "holding_count": r.holding_count,
            "fund_count": r.fund_count,
            "latest_as_of_date": r.latest_as_of_date,
        }
        for r in source_rows
    ]

    return {
        "total_exposure_usd": float(total_exposure),
        "lp_name": LP_NAME,
        "view_mode": "scaled",
        "fund_count": fund_count,
        "company_count": company_count,
        "quarter_count": quarter_count,
        "classification_coverage_pct": classification_coverage_pct,
        "source_breakdown": source_breakdown,
        # Backward-compat aliases
        "total_holdings": total_holdings,
        "total_aum": float(total_exposure),
        "total_companies": company_count,
        "total_funds": fund_count,
        "data_sources": len(source_breakdown),
    }


# ---------------------------------------------------------------------------
# GET /api/dashboard/sector-breakdown
# ---------------------------------------------------------------------------


@router.get("/sector-breakdown")
def get_sector_breakdown(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Return top 10 sectors by holding count, with total value where available."""
    total_holdings_count = (
        db.query(func.count(FactReportedHolding.reported_holding_id)).scalar() or 1
    )

    rows = (
        db.query(
            func.coalesce(DimCompany.primary_sector, "Unclassified").label("sector"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .group_by(func.coalesce(DimCompany.primary_sector, "Unclassified"))
        .order_by(func.count(FactReportedHolding.reported_holding_id).desc())
        .limit(10)
        .all()
    )

    sectors = [
        {
            "sector": row.sector,
            "total_value": float(row.total_value) if row.total_value is not None else None,
            "holding_count": row.holding_count,
            "percentage": round(row.holding_count / total_holdings_count * 100, 2),
        }
        for row in rows
    ]

    return {"sectors": sectors}


# ---------------------------------------------------------------------------
# GET /api/dashboard/fund-breakdown
# ---------------------------------------------------------------------------


@router.get("/fund-breakdown")
def get_fund_breakdown(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Return per-fund summary with holding count and AUM."""
    total_holdings_count = (
        db.query(func.count(FactReportedHolding.reported_holding_id)).scalar() or 1
    )

    rows = (
        db.query(
            DimFund.fund_id,
            DimFund.fund_name,
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
        )
        .join(FactFundReport, FactFundReport.fund_id == DimFund.fund_id)
        .join(
            FactReportedHolding,
            FactReportedHolding.fund_report_id == FactFundReport.fund_report_id,
        )
        .group_by(DimFund.fund_id, DimFund.fund_name)
        .order_by(func.count(FactReportedHolding.reported_holding_id).desc())
        .all()
    )

    funds = [
        {
            "fund_id": row.fund_id,
            "fund_name": row.fund_name,
            "holding_count": row.holding_count,
            "total_value": float(row.total_value) if row.total_value is not None else None,
            "percentage_of_portfolio": round(
                row.holding_count / total_holdings_count * 100, 2
            ),
        }
        for row in rows
    ]

    return {"funds": funds}


# ---------------------------------------------------------------------------
# GET /api/dashboard/geography-breakdown
# GET /api/dashboard/geography-breakdown/fund/{fund_id}
# ---------------------------------------------------------------------------

def _geography_rows(db: Session, fund_id: str | None = None) -> dict:
    """Shared query for geography breakdown, optionally filtered to one fund."""
    country_col = func.coalesce(
        DimCompany.primary_country,
        FactReportedHolding.reported_country,
        "Unknown",
    )

    total_holdings = db.query(
        func.count(FactReportedHolding.reported_holding_id)
    )
    q = (
        db.query(
            country_col.label("geography"),
            func.count(distinct(DimCompany.company_id)).label("company_count"),
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
    )
    if fund_id:
        q = q.join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        q = q.filter(FactFundReport.fund_id == fund_id)
        total_holdings = total_holdings.join(
            FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id
        ).filter(FactFundReport.fund_id == fund_id)

    total = total_holdings.scalar() or 1
    rows = (
        q.group_by(country_col)
        .order_by(func.count(FactReportedHolding.reported_holding_id).desc())
        .all()
    )

    geographies = [
        {
            "geography": row.geography,
            "company_count": row.company_count,
            "holding_count": row.holding_count,
            "total_value": float(row.total_value) if row.total_value is not None else None,
            "percentage": round(row.holding_count / total * 100, 2),
        }
        for row in rows
    ]
    return {"geographies": geographies, "total_holdings": total}


@router.get("/geography-breakdown")
def get_geography_breakdown(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Return exposure breakdown by country/geography across all holdings."""
    return _geography_rows(db)


@router.get("/geography-breakdown/fund/{fund_id}")
def get_geography_breakdown_by_fund(
    fund_id: str,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Return exposure breakdown by country/geography for a specific fund."""
    return _geography_rows(db, fund_id=fund_id)


# ===========================================================================
# Exposure Trend  —  GET /api/dashboard/exposure-trend
#                    GET /api/dashboard/exposure-trend/fund/{fund_id}
# ===========================================================================


def _date_to_quarter(date_str: str) -> str:
    """Convert a YYYY-MM-DD string to a human-readable quarter label."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    q = (d.month - 1) // 3 + 1
    return f"Q{q} {d.year}"


def _build_trend_response(
    db: Session,
    dimension_type: str,
    periods: int,
    fund_id: str,
) -> dict:
    """Shared query logic for portfolio- and fund-scoped trend endpoints."""
    # Fetch last `periods` distinct snapshot dates for this fund_id / taxonomy_type
    date_rows = (
        db.query(FactAggregationSnapshot.snapshot_date)
        .filter(
            FactAggregationSnapshot.fund_id == fund_id,
            FactAggregationSnapshot.taxonomy_type == dimension_type,
        )
        .distinct()
        .order_by(FactAggregationSnapshot.snapshot_date.desc())
        .limit(periods)
        .all()
    )
    date_list = sorted([r[0] for r in date_rows])

    if not date_list:
        return {"dates": [], "series": []}

    # Aggregate exposure by (snapshot_date, node_name) across the chosen dates
    rows = (
        db.query(
            FactAggregationSnapshot.snapshot_date,
            DimTaxonomyNode.node_name,
            func.sum(FactAggregationSnapshot.total_exposure_value_usd).label("value"),
        )
        .join(
            DimTaxonomyNode,
            FactAggregationSnapshot.taxonomy_node_id == DimTaxonomyNode.taxonomy_node_id,
        )
        .filter(
            FactAggregationSnapshot.fund_id == fund_id,
            FactAggregationSnapshot.taxonomy_type == dimension_type,
            FactAggregationSnapshot.taxonomy_node_id != UNKNOWN_NODE_ID,
            FactAggregationSnapshot.snapshot_date.in_(date_list),
        )
        .group_by(
            FactAggregationSnapshot.snapshot_date,
            DimTaxonomyNode.node_name,
        )
        .order_by(FactAggregationSnapshot.snapshot_date)
        .all()
    )

    # Build lookup: date_totals and name -> {date: value}
    date_totals: dict[str, float] = {}
    name_date_value: dict[str, dict[str, float]] = {}
    for snap_date, name, value in rows:
        v = float(value) if value is not None else 0.0
        date_totals[snap_date] = date_totals.get(snap_date, 0.0) + v
        if name not in name_date_value:
            name_date_value[name] = {}
        name_date_value[name][snap_date] = v

    # Compute per-name average % across all dates
    name_avg: dict[str, float] = {}
    for name, date_vals in name_date_value.items():
        pcts = []
        for d in date_list:
            total = date_totals.get(d, 1.0) or 1.0
            pcts.append(date_vals.get(d, 0.0) / total * 100)
        name_avg[name] = sum(pcts) / len(pcts) if pcts else 0.0

    # Top 6 by average allocation; remainder → "Other"
    sorted_names = sorted(name_avg, key=lambda n: name_avg[n], reverse=True)
    top6 = sorted_names[:6]
    others = sorted_names[6:]

    series: list[dict] = []
    for name in top6:
        data = []
        for d in date_list:
            total = date_totals.get(d, 1.0) or 1.0
            pct = name_date_value.get(name, {}).get(d, 0.0) / total * 100
            data.append(round(pct, 2))
        series.append({"name": name, "data": data})

    if others:
        other_data = []
        for d in date_list:
            total = date_totals.get(d, 1.0) or 1.0
            other_val = sum(
                name_date_value.get(n, {}).get(d, 0.0) for n in others
            )
            other_data.append(round(other_val / total * 100, 2))
        series.append({"name": "Other", "data": other_data})

    quarters = [_date_to_quarter(d) for d in date_list]
    return {"dates": quarters, "series": series}


def _build_portfolio_trend_response(
    db: Session,
    dimension_type: str,
    periods: int,
) -> dict:
    """Portfolio-level trend using Northbridge's scaled exposure from fact_lp_scaled_exposure."""
    # Last `periods` distinct as_of_dates that have scaled data
    # Date list: distinct latest-dates across all funds (per-fund, not global max)
    latest_sq = _latest_per_fund_sq(db)
    date_rows = (
        db.query(latest_sq.c.max_date)
        .distinct()
        .order_by(latest_sq.c.max_date.desc())
        .limit(periods)
        .all()
    )
    date_list = sorted([r[0] for r in date_rows])

    if not date_list:
        return {"dates": [], "series": []}

    if dimension_type == "geography":
        dim_col = func.coalesce(
            DimCompany.primary_country,
            FactReportedHolding.reported_country,
            "Unknown",
        )
    else:  # sector (default)
        dim_col = func.coalesce(DimCompany.primary_sector, "Unclassified")

    # Only include each fund's contribution at its own latest date
    rows = (
        db.query(
            FactLpScaledExposure.as_of_date,
            dim_col.label("dim_name"),
            func.sum(FactLpScaledExposure.scaled_value_usd).label("value"),
        )
        .join(
            latest_sq,
            (FactLpScaledExposure.fund_id == latest_sq.c.fund_id)
            & (FactLpScaledExposure.as_of_date == latest_sq.c.max_date),
        )
        .join(FactReportedHolding, FactLpScaledExposure.reported_holding_id == FactReportedHolding.reported_holding_id)
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .filter(
            FactLpScaledExposure.lp_name == LP_NAME,
            FactLpScaledExposure.as_of_date.in_(date_list),
        )
        .group_by(FactLpScaledExposure.as_of_date, dim_col)
        .order_by(FactLpScaledExposure.as_of_date)
        .all()
    )

    date_totals: dict[str, float] = {}
    name_date_value: dict[str, dict[str, float]] = {}
    for as_of_date, name, value in rows:
        v = float(value) if value else 0.0
        date_totals[as_of_date] = date_totals.get(as_of_date, 0.0) + v
        if name not in name_date_value:
            name_date_value[name] = {}
        name_date_value[name][as_of_date] = v

    name_avg: dict[str, float] = {}
    for name, date_vals in name_date_value.items():
        pcts = []
        for d in date_list:
            total = date_totals.get(d, 1.0) or 1.0
            pcts.append(date_vals.get(d, 0.0) / total * 100)
        name_avg[name] = sum(pcts) / len(pcts) if pcts else 0.0

    sorted_names = sorted(name_avg, key=lambda n: name_avg[n], reverse=True)
    top6 = sorted_names[:6]
    others = sorted_names[6:]

    series: list[dict] = []
    for name in top6:
        data = []
        for d in date_list:
            total = date_totals.get(d, 1.0) or 1.0
            pct = name_date_value.get(name, {}).get(d, 0.0) / total * 100
            data.append(round(pct, 2))
        series.append({"name": name, "data": data})

    if others:
        other_data = []
        for d in date_list:
            total = date_totals.get(d, 1.0) or 1.0
            other_val = sum(name_date_value.get(n, {}).get(d, 0.0) for n in others)
            other_data.append(round(other_val / total * 100, 2))
        series.append({"name": "Other", "data": other_data})

    quarters = [_date_to_quarter(d) for d in date_list]
    return {"dates": quarters, "series": series}


@router.get("/exposure-trend")
def get_exposure_trend(
    dimension_type: str = "sector",
    periods: int = 8,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Portfolio-level exposure trend — aggregates across ALL sources via raw holdings."""
    return _build_portfolio_trend_response(db, dimension_type, periods)


@router.get("/exposure-trend/fund/{fund_id}")
def get_exposure_trend_by_fund(
    fund_id: str,
    dimension_type: str = "sector",
    periods: int = 8,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Fund-scoped exposure trend over time."""
    return _build_trend_response(db, dimension_type, periods, fund_id=fund_id)


# ---------------------------------------------------------------------------
# GET /api/dashboard/funds-summary
# ---------------------------------------------------------------------------


@router.get("/funds-summary")
def get_funds_summary(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> list:
    """All funds with latest-quarter stats — ordered by total exposure DESC."""
    latest_period_sq = (
        db.query(
            FactFundReport.fund_id.label("fund_id"),
            func.max(FactFundReport.report_period_end).label("max_period"),
        )
        .group_by(FactFundReport.fund_id)
        .subquery()
    )

    rows = (
        db.query(
            DimFund.fund_id,
            DimFund.fund_name,
            DimFund.fund_type,
            DimFund.source,
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.count(
                distinct(func.coalesce(DimCompany.primary_sector, "Unclassified"))
            ).label("sector_count"),
            latest_period_sq.c.max_period.label("latest_as_of_date"),
        )
        .join(latest_period_sq, latest_period_sq.c.fund_id == DimFund.fund_id)
        .join(
            FactFundReport,
            (FactFundReport.fund_id == DimFund.fund_id)
            & (FactFundReport.report_period_end == latest_period_sq.c.max_period),
        )
        .join(FactReportedHolding, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .group_by(
            DimFund.fund_id,
            DimFund.fund_name,
            DimFund.fund_type,
            DimFund.source,
            latest_period_sq.c.max_period,
        )
        .all()
    )

    scaled_by_fund = _scaled_totals_by_fund(db)

    return sorted(
        [
            {
                "fund_id": row.fund_id,
                "fund_name": row.fund_name,
                "fund_type": row.fund_type,
                "source": row.source,
                "holding_count": row.holding_count,
                "total_exposure_usd": scaled_by_fund.get(str(row.fund_id)) or None,
                "sector_count": row.sector_count or 0,
                "latest_as_of_date": row.latest_as_of_date,
            }
            for row in rows
        ],
        key=lambda r: r["total_exposure_usd"] or 0.0,
        reverse=True,
    )


# ===========================================================================
# Funds router  —  GET /api/funds, GET /api/funds/{fund_id}
# ===========================================================================

funds_router = APIRouter(prefix="/api/funds", tags=["funds"])


# ---------------------------------------------------------------------------
# GET /api/funds
# ---------------------------------------------------------------------------


@funds_router.get("")
def list_funds(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> list:
    """List all funds with holding count (latest quarter) and metadata."""
    # Subquery: latest report_period_end per fund
    latest_period_sq = (
        db.query(
            FactFundReport.fund_id.label("fund_id"),
            func.max(FactFundReport.report_period_end).label("max_period"),
        )
        .group_by(FactFundReport.fund_id)
        .subquery()
    )

    rows = (
        db.query(
            DimFund.fund_id,
            DimFund.fund_name,
            DimFund.manager_name,
            DimFund.fund_type,
            DimFund.strategy,
            DimFund.vintage_year,
            DimFund.base_currency,
            DimFund.source,
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
            latest_period_sq.c.max_period.label("latest_as_of_date"),
        )
        .join(latest_period_sq, latest_period_sq.c.fund_id == DimFund.fund_id)
        .join(
            FactFundReport,
            (FactFundReport.fund_id == DimFund.fund_id)
            & (FactFundReport.report_period_end == latest_period_sq.c.max_period),
        )
        .join(
            FactReportedHolding,
            FactReportedHolding.fund_report_id == FactFundReport.fund_report_id,
        )
        .group_by(
            DimFund.fund_id,
            DimFund.fund_name,
            DimFund.manager_name,
            DimFund.fund_type,
            DimFund.strategy,
            DimFund.vintage_year,
            DimFund.base_currency,
            DimFund.source,
            latest_period_sq.c.max_period,
        )
        .order_by(func.count(FactReportedHolding.reported_holding_id).desc())
        .all()
    )

    return [
        {
            "fund_id": row.fund_id,
            "fund_name": row.fund_name,
            "manager_name": row.manager_name,
            "fund_type": row.fund_type,
            "strategy": row.strategy,
            "source": row.source,
            "vintage_year": int(row.vintage_year) if row.vintage_year is not None else None,
            "holding_count": row.holding_count,
            "total_value": float(row.total_value) if row.total_value is not None else None,
            "latest_as_of_date": row.latest_as_of_date,
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# GET /api/funds/allocation
# ---------------------------------------------------------------------------


@funds_router.get("/allocation")
def get_fund_allocation(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Portfolio allocation by fund type and individual fund (latest quarter)."""
    latest_period_sq = (
        db.query(
            FactFundReport.fund_id.label("fund_id"),
            func.max(FactFundReport.report_period_end).label("max_period"),
        )
        .group_by(FactFundReport.fund_id)
        .subquery()
    )

    fund_rows = (
        db.query(
            DimFund.fund_id,
            DimFund.fund_name,
            DimFund.fund_type,
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            latest_period_sq.c.max_period.label("latest_as_of_date"),
        )
        .join(latest_period_sq, latest_period_sq.c.fund_id == DimFund.fund_id)
        .join(
            FactFundReport,
            (FactFundReport.fund_id == DimFund.fund_id)
            & (FactFundReport.report_period_end == latest_period_sq.c.max_period),
        )
        .join(FactReportedHolding, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .group_by(
            DimFund.fund_id,
            DimFund.fund_name,
            DimFund.fund_type,
            latest_period_sq.c.max_period,
        )
        .all()
    )

    scaled_by_fund = _scaled_totals_by_fund(db)
    total_portfolio = sum(scaled_by_fund.values())

    # Per-fund, per-sector totals (latest quarter, scaled) — used for top_sectors
    sector_rows = (
        db.query(
            FactFundReport.fund_id.label("fund_id"),
            func.coalesce(DimCompany.primary_sector, "Unclassified").label("sector"),
            func.sum(func.coalesce(FactLpScaledExposure.scaled_value_usd, FactReportedHolding.reported_value_usd)).label("sector_value"),
        )
        .join(FactReportedHolding, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .outerjoin(FactLpScaledExposure, FactReportedHolding.reported_holding_id == FactLpScaledExposure.reported_holding_id)
        .join(
            latest_period_sq,
            (latest_period_sq.c.fund_id == FactFundReport.fund_id)
            & (latest_period_sq.c.max_period == FactFundReport.report_period_end),
        )
        .group_by(
            FactFundReport.fund_id,
            func.coalesce(DimCompany.primary_sector, "Unclassified"),
        )
        .all()
    )

    # Build top-3 sectors per fund (by value, excluding Unclassified)
    from collections import defaultdict as _defaultdict
    fund_sector_values: dict = _defaultdict(list)
    for row in sector_rows:
        fund_sector_values[row.fund_id].append(
            (row.sector, float(row.sector_value) if row.sector_value else 0.0)
        )
    fund_top_sectors: dict = {}
    for fid, pairs in fund_sector_values.items():
        pairs.sort(key=lambda x: -x[1])
        fund_top_sectors[fid] = [s for s, _ in pairs if s != "Unclassified"][:3]

    fund_rows_sorted = sorted(
        fund_rows, key=lambda r: scaled_by_fund.get(str(r.fund_id), 0.0), reverse=True
    )

    by_fund = [
        {
            "fund_id": r.fund_id,
            "fund_name": r.fund_name,
            "fund_type": r.fund_type,
            "total_exposure_usd": scaled_by_fund.get(str(r.fund_id)) or None,
            "pct_of_portfolio": (
                round(scaled_by_fund.get(str(r.fund_id), 0.0) / total_portfolio * 100, 2)
                if total_portfolio > 0
                else 0.0
            ),
            "holding_count": r.holding_count,
            "top_sectors": fund_top_sectors.get(r.fund_id, []),
            "latest_as_of_date": r.latest_as_of_date,
        }
        for r in fund_rows_sorted
    ]

    # Aggregate by fund_type
    type_totals: dict = {}
    type_counts: dict = {}
    for f in by_fund:
        t = f["fund_type"] or "Other"
        type_totals[t] = type_totals.get(t, 0.0) + (f["total_exposure_usd"] or 0.0)
        type_counts[t] = type_counts.get(t, 0) + 1

    by_type = [
        {
            "fund_type": t,
            "total_exposure_usd": round(v, 2),
            "pct_of_portfolio": round(v / total_portfolio * 100, 2) if total_portfolio > 0 else 0.0,
            "fund_count": type_counts[t],
        }
        for t, v in sorted(type_totals.items(), key=lambda x: -x[1])
    ]

    return {
        "total_portfolio_exposure": total_portfolio,
        "by_type": by_type,
        "by_fund": by_fund,
    }


# ---------------------------------------------------------------------------
# GET /api/funds/{fund_id}/export — CSV
# ---------------------------------------------------------------------------

_FUND_EXPORT_COLUMNS = [
    "Company Name", "Fund", "Sector", "Industry", "Country",
    "Reported Value (USD)", "% NAV", "As of Date", "Source",
    "Extraction Confidence", "Match Method", "Match Confidence",
]


@funds_router.get("/{fund_id}/export")
def export_fund_holdings_csv(
    fund_id: str,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """Export all holdings for a single fund as a CSV download."""
    fund = db.query(DimFund).filter(DimFund.fund_id == fund_id).first()
    if fund is None:
        raise HTTPException(status_code=404, detail="Fund not found")

    company_name_col = func.coalesce(DimCompany.company_name, FactReportedHolding.raw_company_name)
    sector_col = func.coalesce(DimCompany.primary_sector, FactReportedHolding.reported_sector)
    country_col = func.coalesce(DimCompany.primary_country, FactReportedHolding.reported_country)

    rows = (
        db.query(
            company_name_col.label("company_name"),
            DimFund.fund_name,
            sector_col.label("sector"),
            DimCompany.primary_industry.label("industry"),
            country_col.label("country"),
            FactReportedHolding.reported_value_usd,
            FactReportedHolding.extraction_confidence,
            FactReportedHolding.as_of_date,
            FactReportedHolding.source,
            EntityResolutionLog.match_method,
            EntityResolutionLog.match_confidence,
        )
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .join(DimFund, FactFundReport.fund_id == DimFund.fund_id)
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .outerjoin(
            EntityResolutionLog,
            FactReportedHolding.reported_holding_id == EntityResolutionLog.reported_holding_id,
        )
        .filter(DimFund.fund_id == fund_id)
        .all()
    )

    fund_total = sum(
        float(r.reported_value_usd) for r in rows if r.reported_value_usd is not None
    )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=_FUND_EXPORT_COLUMNS)
    writer.writeheader()
    for row in rows:
        value = float(row.reported_value_usd) if row.reported_value_usd is not None else None
        pct_nav = round(value / fund_total * 100, 4) if (value is not None and fund_total > 0) else None
        writer.writerow({
            "Company Name": row.company_name or "",
            "Fund": row.fund_name or "",
            "Sector": row.sector or "",
            "Industry": row.industry or "",
            "Country": row.country or "",
            "Reported Value (USD)": value if value is not None else "",
            "% NAV": pct_nav if pct_nav is not None else "",
            "As of Date": row.as_of_date or "",
            "Source": row.source or "",
            "Extraction Confidence": (
                float(row.extraction_confidence) if row.extraction_confidence is not None else ""
            ),
            "Match Method": row.match_method or "",
            "Match Confidence": (
                float(row.match_confidence) if row.match_confidence is not None else ""
            ),
        })

    safe_name = fund.fund_name.replace(" ", "_").replace("/", "-")[:30]
    filename = f"lookthrough_{safe_name}_{date.today().isoformat()}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# GET /api/funds/{fund_id}/holdings — paginated, latest quarter
# ---------------------------------------------------------------------------


@funds_router.get("/{fund_id}/holdings")
def list_fund_holdings(
    fund_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = Query(None),
    sort_by: str = Query("reported_value_usd"),
    sort_dir: str = Query("desc"),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Paginated holdings for a fund, scoped to the latest quarter."""
    fund = db.query(DimFund).filter(DimFund.fund_id == fund_id).first()
    if fund is None:
        raise HTTPException(status_code=404, detail="Fund not found")

    latest_period = (
        db.query(func.max(FactFundReport.report_period_end))
        .filter(FactFundReport.fund_id == fund_id)
        .scalar()
    )
    if latest_period is None:
        return {"items": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 1}

    # Fund total from scaled exposure (latest scaled date for this fund)
    latest_scaled_date_fh = (
        db.query(func.max(FactLpScaledExposure.as_of_date))
        .filter(FactLpScaledExposure.fund_id == fund_id, FactLpScaledExposure.lp_name == LP_NAME)
        .scalar()
    )
    fund_scaled_total = (
        db.query(func.sum(FactLpScaledExposure.scaled_value_usd))
        .filter(
            FactLpScaledExposure.fund_id == fund_id,
            FactLpScaledExposure.lp_name == LP_NAME,
            FactLpScaledExposure.as_of_date == latest_scaled_date_fh,
        )
        .scalar()
    ) if latest_scaled_date_fh else None
    fund_total_float = float(fund_scaled_total) if fund_scaled_total else 0.0

    company_name_col = func.coalesce(DimCompany.company_name, FactReportedHolding.raw_company_name)
    sector_col = func.coalesce(DimCompany.primary_sector, "Unclassified")
    country_col = func.coalesce(DimCompany.primary_country, FactReportedHolding.reported_country, "Unknown")
    effective_value_col = func.coalesce(FactLpScaledExposure.scaled_value_usd, FactReportedHolding.reported_value_usd)

    q = (
        db.query(
            FactReportedHolding.reported_holding_id,
            FactReportedHolding.company_id,
            company_name_col.label("company_name"),
            sector_col.label("sector"),
            DimCompany.primary_industry.label("industry"),
            country_col.label("country"),
            FactReportedHolding.reported_value_usd,
            FactLpScaledExposure.scaled_value_usd,
            FactReportedHolding.as_of_date,
            FactReportedHolding.source,
        )
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .outerjoin(FactLpScaledExposure, FactReportedHolding.reported_holding_id == FactLpScaledExposure.reported_holding_id)
        .filter(
            FactFundReport.fund_id == fund_id,
            FactFundReport.report_period_end == latest_period,
        )
    )

    if search:
        q = q.filter(company_name_col.ilike(f"%{search}%"))

    sort_col_map = {
        "reported_value_usd": effective_value_col,
        "company_name": company_name_col,
        "sector": sector_col,
    }
    col = sort_col_map.get(sort_by, effective_value_col)
    q = q.order_by(col.desc().nullslast() if sort_dir != "asc" else col.asc().nullslast())

    total = q.count()
    total_pages = max(1, (total + page_size - 1) // page_size)
    rows = q.offset((page - 1) * page_size).limit(page_size).all()

    items = [
        {
            "holding_id": row.reported_holding_id,
            "company_id": row.company_id,
            "company_name": row.company_name,
            "sector": row.sector or "Unclassified",
            "industry": row.industry or "Unclassified",
            "country": row.country or "Unknown",
            "reported_value_usd": float(
                row.scaled_value_usd if row.scaled_value_usd is not None else row.reported_value_usd
            ) if (row.scaled_value_usd is not None or row.reported_value_usd is not None) else None,
            "raw_value_usd": float(row.reported_value_usd) if row.reported_value_usd is not None else None,
            "pct_of_fund": (
                round(
                    float(row.scaled_value_usd if row.scaled_value_usd is not None else row.reported_value_usd)
                    / fund_total_float * 100,
                    2,
                )
                if (row.scaled_value_usd is not None or row.reported_value_usd is not None) and fund_total_float > 0
                else None
            ),
            "as_of_date": row.as_of_date,
            "source": row.source,
        }
        for row in rows
    ]

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


# ---------------------------------------------------------------------------
# GET /api/funds/{fund_id}/exposure-trend
# ---------------------------------------------------------------------------


@funds_router.get("/{fund_id}/exposure-trend")
def get_fund_exposure_trend(
    fund_id: str,
    dimension_type: str = Query("sector"),
    periods: int = Query(8),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Fund-scoped exposure trend over time (sector or geography)."""
    return _build_trend_response(db, dimension_type, periods, fund_id=fund_id)


# ---------------------------------------------------------------------------
# GET /api/funds/{fund_id}
# ---------------------------------------------------------------------------


@funds_router.get("/{fund_id}")
def get_fund_detail(
    fund_id: str,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Full fund profile: metadata, stats, sector/industry/geography breakdowns, top 15 holdings."""
    fund = db.query(DimFund).filter(DimFund.fund_id == fund_id).first()
    if fund is None:
        raise HTTPException(status_code=404, detail="Fund not found")

    # Latest report period for this fund
    latest_period = (
        db.query(func.max(FactFundReport.report_period_end))
        .filter(FactFundReport.fund_id == fund_id)
        .scalar()
    )

    # Total distinct quarters for this fund (used by frontend to show BDC banner)
    quarter_count = (
        db.query(func.count(distinct(FactFundReport.report_period_end)))
        .filter(FactFundReport.fund_id == fund_id)
        .scalar()
        or 0
    )

    # ---- Stats scoped to latest quarter ----
    holding_count_q = (
        db.query(func.count(FactReportedHolding.reported_holding_id))
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .filter(FactFundReport.fund_id == fund_id)
    )
    if latest_period:
        holding_count_q = holding_count_q.filter(FactFundReport.report_period_end == latest_period)
    holding_count = holding_count_q.scalar() or 0

    # Total exposure from Northbridge's scaled values at latest scaled date for this fund
    latest_scaled_date = (
        db.query(func.max(FactLpScaledExposure.as_of_date))
        .filter(FactLpScaledExposure.fund_id == fund_id, FactLpScaledExposure.lp_name == LP_NAME)
        .scalar()
    )
    scaled_total = (
        db.query(func.sum(FactLpScaledExposure.scaled_value_usd))
        .filter(
            FactLpScaledExposure.fund_id == fund_id,
            FactLpScaledExposure.lp_name == LP_NAME,
            FactLpScaledExposure.as_of_date == latest_scaled_date,
        )
        .scalar()
    ) if latest_scaled_date else None
    total_exposure = float(scaled_total) if scaled_total else 0.0

    # ---- Sector breakdown by value, latest quarter (scaled) ----
    _scaled_val = func.coalesce(FactLpScaledExposure.scaled_value_usd, FactReportedHolding.reported_value_usd)
    sector_q = (
        db.query(
            func.coalesce(DimCompany.primary_sector, "Unclassified").label("sector"),
            func.sum(_scaled_val).label("value_usd"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .outerjoin(FactLpScaledExposure, FactReportedHolding.reported_holding_id == FactLpScaledExposure.reported_holding_id)
        .filter(FactFundReport.fund_id == fund_id)
    )
    if latest_period:
        sector_q = sector_q.filter(FactFundReport.report_period_end == latest_period)
    sector_rows = (
        sector_q
        .group_by(func.coalesce(DimCompany.primary_sector, "Unclassified"))
        .order_by(func.sum(_scaled_val).desc().nullslast())
        .all()
    )

    sector_breakdown = [
        {
            "sector": row.sector,
            "value_usd": float(row.value_usd) if row.value_usd else 0.0,
            "pct": (
                round(float(row.value_usd) / total_exposure * 100, 2)
                if row.value_usd and total_exposure > 0
                else 0.0
            ),
        }
        for row in sector_rows
    ]

    # ---- Industry breakdown (top 15 by value), latest quarter (scaled) ----
    industry_q = (
        db.query(
            func.coalesce(DimCompany.primary_industry, "Unclassified").label("industry"),
            func.coalesce(DimCompany.primary_sector, "Unclassified").label("sector"),
            func.sum(_scaled_val).label("value_usd"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .outerjoin(FactLpScaledExposure, FactReportedHolding.reported_holding_id == FactLpScaledExposure.reported_holding_id)
        .filter(FactFundReport.fund_id == fund_id)
    )
    if latest_period:
        industry_q = industry_q.filter(FactFundReport.report_period_end == latest_period)
    industry_rows = (
        industry_q
        .group_by(
            func.coalesce(DimCompany.primary_industry, "Unclassified"),
            func.coalesce(DimCompany.primary_sector, "Unclassified"),
        )
        .order_by(func.sum(_scaled_val).desc().nullslast())
        .limit(15)
        .all()
    )

    industry_breakdown = [
        {
            "industry": row.industry,
            "sector": row.sector,
            "value_usd": float(row.value_usd) if row.value_usd else 0.0,
            "pct": (
                round(float(row.value_usd) / total_exposure * 100, 2)
                if row.value_usd and total_exposure > 0
                else 0.0
            ),
        }
        for row in industry_rows
    ]

    # ---- Geography breakdown by value, latest quarter (scaled) ----
    country_col = func.coalesce(
        DimCompany.primary_country,
        FactReportedHolding.reported_country,
        "Unknown",
    )
    geo_q = (
        db.query(
            country_col.label("country"),
            func.sum(_scaled_val).label("value_usd"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .outerjoin(FactLpScaledExposure, FactReportedHolding.reported_holding_id == FactLpScaledExposure.reported_holding_id)
        .filter(FactFundReport.fund_id == fund_id)
    )
    if latest_period:
        geo_q = geo_q.filter(FactFundReport.report_period_end == latest_period)
    geo_rows = (
        geo_q
        .group_by(country_col)
        .order_by(func.sum(_scaled_val).desc().nullslast())
        .all()
    )

    geography_breakdown = [
        {
            "country": row.country,
            "value_usd": float(row.value_usd) if row.value_usd else 0.0,
            "pct": (
                round(float(row.value_usd) / total_exposure * 100, 2)
                if row.value_usd and total_exposure > 0
                else 0.0
            ),
        }
        for row in geo_rows
    ]

    # ---- Top 15 holdings by value, latest quarter (scaled) ----
    company_name_col = func.coalesce(DimCompany.company_name, FactReportedHolding.raw_company_name)
    top_q = (
        db.query(
            FactReportedHolding.reported_holding_id,
            FactReportedHolding.company_id,
            company_name_col.label("company_name"),
            func.coalesce(DimCompany.primary_sector, "Unclassified").label("sector"),
            func.coalesce(DimCompany.primary_industry, "Unclassified").label("industry"),
            FactReportedHolding.reported_value_usd,
            FactLpScaledExposure.scaled_value_usd,
            FactReportedHolding.as_of_date,
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .outerjoin(FactLpScaledExposure, FactReportedHolding.reported_holding_id == FactLpScaledExposure.reported_holding_id)
        .filter(FactFundReport.fund_id == fund_id)
    )
    if latest_period:
        top_q = top_q.filter(FactFundReport.report_period_end == latest_period)
    top_rows = (
        top_q
        .order_by(func.coalesce(FactLpScaledExposure.scaled_value_usd, FactReportedHolding.reported_value_usd).desc().nullslast())
        .limit(15)
        .all()
    )

    top_holdings = [
        {
            "company_id": row.company_id,
            "company_name": row.company_name,
            "sector": row.sector,
            "industry": row.industry,
            "reported_value_usd": float(row.scaled_value_usd if row.scaled_value_usd is not None else row.reported_value_usd),
            "raw_value_usd": float(row.reported_value_usd) if row.reported_value_usd is not None else None,
            "pct_of_fund": (
                round(float(row.scaled_value_usd if row.scaled_value_usd is not None else row.reported_value_usd) / total_exposure * 100, 2)
                if (row.scaled_value_usd is not None or row.reported_value_usd is not None) and total_exposure > 0
                else None
            ),
            "as_of_date": row.as_of_date,
        }
        for row in top_rows
        if row.scaled_value_usd is not None or row.reported_value_usd is not None
    ]

    return {
        "fund_id": fund.fund_id,
        "fund_name": fund.fund_name,
        "manager_name": fund.manager_name,
        "fund_type": fund.fund_type,
        "strategy": fund.strategy,
        "source": fund.source,
        "vintage_year": (
            int(fund.vintage_year)
            if fund.vintage_year is not None and not math.isnan(fund.vintage_year)
            else None
        ),
        "base_currency": fund.base_currency,
        "latest_as_of_date": latest_period,
        "quarter_count": quarter_count,
        "holding_count": holding_count,
        "total_exposure_usd": total_exposure if total_exposure > 0 else None,
        "sector_breakdown": sector_breakdown,
        "industry_breakdown": industry_breakdown,
        "geography_breakdown": geography_breakdown,
        "top_holdings": top_holdings,
    }


# ===========================================================================
# Companies router  —  GET /api/companies/{company_id}
# ===========================================================================

companies_router = APIRouter(prefix="/api/companies", tags=["companies"])


@companies_router.get("/{company_id}")
def get_company_detail(
    company_id: str,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Full company profile: metadata, classification, resolution, fund exposure, holdings, audit."""

    # ---- Company metadata ----
    company = db.query(DimCompany).filter(DimCompany.company_id == company_id).first()
    if company is None:
        raise HTTPException(status_code=404, detail="Company not found")

    # ---- AI classification (most recent sector classification) ----
    classification_row = (
        db.query(FactExposureClassification)
        .filter(
            FactExposureClassification.company_id == company_id,
            FactExposureClassification.taxonomy_type == "sector",
        )
        .order_by(FactExposureClassification.run_id.desc())
        .first()
    )
    classification = None
    if classification_row:
        classification = {
            "taxonomy_node_id": classification_row.taxonomy_node_id,
            "confidence": float(classification_row.confidence) if classification_row.confidence is not None else None,
            "rationale": classification_row.rationale,
            "model": classification_row.model,
            "prompt_version": classification_row.prompt_version,
        }

    # ---- Entity resolution log ----
    holding_id_rows = (
        db.query(FactReportedHolding.reported_holding_id)
        .filter(FactReportedHolding.company_id == company_id)
        .all()
    )
    holding_ids = [r.reported_holding_id for r in holding_id_rows]
    resolution_rows = []
    if holding_ids:
        resolution_rows = (
            db.query(EntityResolutionLog)
            .filter(EntityResolutionLog.reported_holding_id.in_(holding_ids))
            .all()
        )
    raw_names = sorted({r.raw_company_name for r in resolution_rows if r.raw_company_name})
    methods = sorted({r.match_method for r in resolution_rows if r.match_method})
    confidences = [r.match_confidence for r in resolution_rows if r.match_confidence is not None]
    avg_confidence = sum(confidences) / len(confidences) if confidences else None
    resolution = {
        "raw_names": raw_names,
        "match_methods": methods,
        "avg_confidence": round(avg_confidence, 4) if avg_confidence is not None else None,
        "resolution_count": len(resolution_rows),
    }

    # ---- Fund exposure ----
    fund_rows = (
        db.query(
            DimFund.fund_id,
            DimFund.fund_name,
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
            func.max(FactReportedHolding.as_of_date).label("most_recent_date"),
        )
        .select_from(FactReportedHolding)
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .join(DimFund, FactFundReport.fund_id == DimFund.fund_id)
        .filter(FactReportedHolding.company_id == company_id)
        .group_by(DimFund.fund_id, DimFund.fund_name)
        .order_by(func.sum(FactReportedHolding.reported_value_usd).desc())
        .all()
    )
    fund_exposure = [
        {
            "fund_id": r.fund_id,
            "fund_name": r.fund_name,
            "holding_count": r.holding_count,
            "total_value": float(r.total_value) if r.total_value is not None else None,
            "most_recent_date": r.most_recent_date,
        }
        for r in fund_rows
    ]

    # ---- Holdings ----
    holding_rows = (
        db.query(
            FactReportedHolding.reported_holding_id,
            DimFund.fund_id,
            DimFund.fund_name,
            FactReportedHolding.reported_value_usd,
            FactReportedHolding.reported_pct_nav,
            FactReportedHolding.reported_sector,
            FactReportedHolding.as_of_date,
            FactReportedHolding.source,
            FactReportedHolding.extraction_confidence,
        )
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .join(DimFund, FactFundReport.fund_id == DimFund.fund_id)
        .filter(FactReportedHolding.company_id == company_id)
        .order_by(FactReportedHolding.as_of_date.desc())
        .all()
    )
    holdings = [
        {
            "reported_holding_id": r.reported_holding_id,
            "fund_id": r.fund_id,
            "fund_name": r.fund_name,
            "reported_value_usd": float(r.reported_value_usd) if r.reported_value_usd is not None else None,
            "reported_pct_nav": float(r.reported_pct_nav) if r.reported_pct_nav is not None else None,
            "reported_sector": r.reported_sector,
            "as_of_date": r.as_of_date,
            "source": r.source,
            "extraction_confidence": float(r.extraction_confidence) if r.extraction_confidence is not None else None,
        }
        for r in holding_rows
    ]

    # ---- Audit events (last 10) ----
    audit_rows = (
        db.query(FactAuditEvent)
        .filter(FactAuditEvent.entity_id == company_id)
        .order_by(FactAuditEvent.event_time.desc())
        .limit(10)
        .all()
    )
    audit_events = [
        {
            "audit_event_id": r.audit_event_id,
            "event_time": r.event_time,
            "actor_type": r.actor_type,
            "actor_id": r.actor_id,
            "action": r.action,
            "entity_type": r.entity_type,
            "payload_json": r.payload_json,
        }
        for r in audit_rows
    ]

    return {
        "company_id": company.company_id,
        "company_name": company.company_name,
        "primary_sector": company.primary_sector,
        "primary_industry": company.primary_industry,
        "primary_country": company.primary_country,
        "website": company.website,
        "source": company.source,
        "created_at": company.created_at,
        "classification": classification,
        "resolution": resolution,
        "fund_exposure": fund_exposure,
        "holdings": holdings,
        "audit_events": audit_events,
    }
