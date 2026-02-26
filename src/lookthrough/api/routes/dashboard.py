"""Dashboard API endpoints returning aggregated portfolio statistics."""

from fastapi import APIRouter, Depends
from sqlalchemy import distinct, func
from sqlalchemy.orm import Session

from src.lookthrough.auth.dependencies import get_current_user, get_db
from src.lookthrough.db.models import (
    DimCompany,
    DimFund,
    FactFundReport,
    FactReportedHolding,
    User,
)

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


# ---------------------------------------------------------------------------
# GET /api/dashboard/stats
# ---------------------------------------------------------------------------


@router.get("/stats")
def get_stats(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Return top-level portfolio statistics."""
    total_holdings = (
        db.query(func.count(FactReportedHolding.reported_holding_id)).scalar() or 0
    )
    total_companies = db.query(func.count(DimCompany.company_id)).scalar() or 0
    total_funds = db.query(func.count(DimFund.fund_id)).scalar() or 0
    total_aum = (
        db.query(func.sum(FactReportedHolding.reported_value_usd)).scalar() or 0.0
    )
    data_sources = (
        db.query(func.count(distinct(FactReportedHolding.source))).scalar() or 0
    )

    return {
        "total_holdings": total_holdings,
        "total_companies": total_companies,
        "total_funds": total_funds,
        "total_aum": float(total_aum),
        "data_sources": data_sources,
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
# ---------------------------------------------------------------------------


@router.get("/geography-breakdown")
def get_geography_breakdown(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Return exposure breakdown by country/geography."""
    rows = (
        db.query(
            func.coalesce(DimCompany.primary_country, "Unknown").label("geography"),
            func.count(distinct(DimCompany.company_id)).label("company_count"),
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .group_by(func.coalesce(DimCompany.primary_country, "Unknown"))
        .order_by(func.sum(FactReportedHolding.reported_value_usd).desc())
        .all()
    )

    geographies = [
        {
            "geography": row.geography,
            "company_count": row.company_count,
            "holding_count": row.holding_count,
            "total_value": float(row.total_value or 0),
        }
        for row in rows
    ]

    return {"geographies": geographies}
