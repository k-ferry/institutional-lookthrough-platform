"""GICS taxonomy drill-down API endpoints."""

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import distinct, func
from sqlalchemy.orm import Session

from src.lookthrough.auth.dependencies import get_current_user, get_db
from src.lookthrough.db.models import (
    DimCompany,
    DimFund,
    FactFundReport,
    FactLpScaledExposure,
    FactReportedHolding,
    GICSMapping,
    User,
)

gics_router = APIRouter(prefix="/api/gics", tags=["gics"])

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


def _sector_col():
    return func.coalesce(
        DimCompany.primary_sector,
        FactReportedHolding.reported_sector,
        "Unclassified",
    )


def _company_name_col():
    return func.coalesce(DimCompany.company_name, FactReportedHolding.raw_company_name)


# ---------------------------------------------------------------------------
# GET /api/gics/sectors
# ---------------------------------------------------------------------------


@gics_router.get("/sectors")
def get_gics_sectors(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """All GICS sectors with holding counts, company counts, AUM, and percentages."""
    sector_col = _sector_col()
    latest_sq = _latest_per_fund_sq(db)

    rows = (
        db.query(
            sector_col.label("sector"),
            func.count(FactLpScaledExposure.scaled_exposure_id).label("holding_count"),
            func.count(distinct(FactReportedHolding.company_id)).label("company_count"),
            func.sum(FactLpScaledExposure.scaled_value_usd).label("total_value"),
        )
        .select_from(FactLpScaledExposure)
        .join(
            latest_sq,
            (FactLpScaledExposure.fund_id == latest_sq.c.fund_id)
            & (FactLpScaledExposure.as_of_date == latest_sq.c.max_date),
        )
        .join(FactReportedHolding, FactLpScaledExposure.reported_holding_id == FactReportedHolding.reported_holding_id)
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .filter(FactLpScaledExposure.lp_name == LP_NAME)
        .group_by(sector_col)
        .order_by(func.sum(FactLpScaledExposure.scaled_value_usd).desc())
        .all()
    )

    TOTAL_PORTFOLIO = 713_300_000
    total_holdings = sum(r.holding_count for r in rows)

    return {
        "sectors": [
            {
                "sector": row.sector,
                "holding_count": row.holding_count,
                "company_count": row.company_count,
                "total_value": float(row.total_value) if row.total_value is not None else None,
                "percentage": round(float(row.total_value) / TOTAL_PORTFOLIO * 100, 2) if row.total_value else 0.0,
            }
            for row in rows
        ],
        "total_holdings": total_holdings,
    }


# ---------------------------------------------------------------------------
# GET /api/gics/sector/{sector_name}
# ---------------------------------------------------------------------------


@gics_router.get("/sector/{sector_name}")
def get_sector_detail(
    sector_name: str,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Industry groups within a GICS sector, plus sector summary and top 10 companies."""
    sector_col = _sector_col()
    company_name_col = _company_name_col()
    ig_col = func.coalesce(GICSMapping.gics_industry_group_name, "Unknown")
    latest_sq = _latest_per_fund_sq(db)

    def base_joins(qry):
        return (
            qry
            .select_from(FactLpScaledExposure)
            .join(
                latest_sq,
                (FactLpScaledExposure.fund_id == latest_sq.c.fund_id)
                & (FactLpScaledExposure.as_of_date == latest_sq.c.max_date),
            )
            .join(FactReportedHolding, FactLpScaledExposure.reported_holding_id == FactReportedHolding.reported_holding_id)
            .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
            .outerjoin(GICSMapping, FactReportedHolding.reported_sector == GICSMapping.reported_sector)
            .filter(FactLpScaledExposure.lp_name == LP_NAME, sector_col == sector_name)
        )

    # Sector totals
    totals = base_joins(
        db.query(
            func.count(FactLpScaledExposure.scaled_exposure_id).label("holding_count"),
            func.count(distinct(FactReportedHolding.company_id)).label("company_count"),
            func.sum(FactLpScaledExposure.scaled_value_usd).label("total_value"),
        )
    ).one()
    holding_total = totals.holding_count or 1

    # Industry groups
    ig_rows = (
        base_joins(
            db.query(
                ig_col.label("industry_group"),
                func.count(FactLpScaledExposure.scaled_exposure_id).label("holding_count"),
                func.count(distinct(FactReportedHolding.company_id)).label("company_count"),
                func.sum(FactLpScaledExposure.scaled_value_usd).label("total_value"),
            )
        )
        .group_by(ig_col)
        .order_by(func.sum(FactLpScaledExposure.scaled_value_usd).desc())
        .all()
    )

    # Top 10 companies by scaled value
    top_rows = (
        base_joins(
            db.query(
                FactReportedHolding.company_id,
                company_name_col.label("company_name"),
                func.count(FactLpScaledExposure.scaled_exposure_id).label("holding_count"),
                func.sum(FactLpScaledExposure.scaled_value_usd).label("total_value"),
            )
        )
        .group_by(FactReportedHolding.company_id, company_name_col)
        .order_by(func.coalesce(func.sum(FactLpScaledExposure.scaled_value_usd), 0).desc())
        .limit(10)
        .all()
    )

    return {
        "sector": sector_name,
        "holding_count": totals.holding_count or 0,
        "company_count": totals.company_count or 0,
        "total_value": float(totals.total_value) if totals.total_value is not None else None,
        "industry_groups": [
            {
                "industry_group": row.industry_group,
                "holding_count": row.holding_count,
                "company_count": row.company_count,
                "total_value": float(row.total_value) if row.total_value is not None else None,
                "percentage_of_sector": round(row.holding_count / holding_total * 100, 2),
            }
            for row in ig_rows
        ],
        "top_companies": [
            {
                "company_id": row.company_id,
                "company_name": row.company_name,
                "holding_count": row.holding_count,
                "total_value": float(row.total_value) if row.total_value is not None else None,
            }
            for row in top_rows
        ],
    }


# ---------------------------------------------------------------------------
# GET /api/gics/industry/{industry_name}
# ---------------------------------------------------------------------------


@gics_router.get("/industry/{industry_name}")
def get_industry_detail(
    industry_name: str,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """GICS Industries within an Industry Group, plus top 10 companies."""
    company_name_col = _company_name_col()
    ig_col = func.coalesce(GICSMapping.gics_industry_group_name, "Unknown")
    industry_col = func.coalesce(GICSMapping.gics_industry_name, "Unknown")
    latest_sq = _latest_per_fund_sq(db)

    def base_joins(qry):
        return (
            qry
            .select_from(FactLpScaledExposure)
            .join(
                latest_sq,
                (FactLpScaledExposure.fund_id == latest_sq.c.fund_id)
                & (FactLpScaledExposure.as_of_date == latest_sq.c.max_date),
            )
            .join(FactReportedHolding, FactLpScaledExposure.reported_holding_id == FactReportedHolding.reported_holding_id)
            .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
            .outerjoin(GICSMapping, FactReportedHolding.reported_sector == GICSMapping.reported_sector)
            .filter(FactLpScaledExposure.lp_name == LP_NAME, ig_col == industry_name)
        )

    # Industry group totals
    totals = base_joins(
        db.query(
            func.count(FactLpScaledExposure.scaled_exposure_id).label("holding_count"),
            func.count(distinct(FactReportedHolding.company_id)).label("company_count"),
            func.sum(FactLpScaledExposure.scaled_value_usd).label("total_value"),
        )
    ).one()
    holding_total = totals.holding_count or 1

    # Industries within the group
    industry_rows = (
        base_joins(
            db.query(
                industry_col.label("industry"),
                func.count(FactLpScaledExposure.scaled_exposure_id).label("holding_count"),
                func.count(distinct(FactReportedHolding.company_id)).label("company_count"),
                func.sum(FactLpScaledExposure.scaled_value_usd).label("total_value"),
            )
        )
        .group_by(industry_col)
        .order_by(func.sum(FactLpScaledExposure.scaled_value_usd).desc())
        .all()
    )

    # Top 10 companies
    top_rows = (
        base_joins(
            db.query(
                FactReportedHolding.company_id,
                company_name_col.label("company_name"),
                func.count(FactLpScaledExposure.scaled_exposure_id).label("holding_count"),
                func.sum(FactLpScaledExposure.scaled_value_usd).label("total_value"),
            )
        )
        .group_by(FactReportedHolding.company_id, company_name_col)
        .order_by(func.coalesce(func.sum(FactLpScaledExposure.scaled_value_usd), 0).desc())
        .limit(10)
        .all()
    )

    return {
        "industry_group": industry_name,
        "holding_count": totals.holding_count or 0,
        "company_count": totals.company_count or 0,
        "total_value": float(totals.total_value) if totals.total_value is not None else None,
        "sub_industries": [
            {
                "industry": row.industry,
                "holding_count": row.holding_count,
                "company_count": row.company_count,
                "total_value": float(row.total_value) if row.total_value is not None else None,
                "percentage_of_group": round(row.holding_count / holding_total * 100, 2),
            }
            for row in industry_rows
        ],
        "top_companies": [
            {
                "company_id": row.company_id,
                "company_name": row.company_name,
                "holding_count": row.holding_count,
                "total_value": float(row.total_value) if row.total_value is not None else None,
            }
            for row in top_rows
        ],
    }


# ---------------------------------------------------------------------------
# GET /api/gics/holdings
# ---------------------------------------------------------------------------


@gics_router.get("/holdings")
def get_gics_holdings(
    sector: Optional[str] = Query(None),
    industry: Optional[str] = Query(None),
    sub_industry: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Paginated holdings filtered to any GICS taxonomy level, using scaled exposure values."""
    sector_col = _sector_col()
    company_name_col = _company_name_col()
    ig_col = func.coalesce(GICSMapping.gics_industry_group_name, "Unknown")
    industry_col = func.coalesce(GICSMapping.gics_industry_name, "Unknown")
    latest_sq = _latest_per_fund_sq(db)

    q = (
        db.query(
            FactReportedHolding.reported_holding_id,
            FactReportedHolding.company_id,
            company_name_col.label("company_name"),
            DimFund.fund_id,
            DimFund.fund_name,
            FactLpScaledExposure.scaled_value_usd,
            FactReportedHolding.reported_value_usd,
            FactReportedHolding.reported_sector,
            FactReportedHolding.as_of_date,
            FactReportedHolding.source,
        )
        .select_from(FactLpScaledExposure)
        .join(
            latest_sq,
            (FactLpScaledExposure.fund_id == latest_sq.c.fund_id)
            & (FactLpScaledExposure.as_of_date == latest_sq.c.max_date),
        )
        .join(FactReportedHolding, FactLpScaledExposure.reported_holding_id == FactReportedHolding.reported_holding_id)
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .outerjoin(GICSMapping, FactReportedHolding.reported_sector == GICSMapping.reported_sector)
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .join(DimFund, FactFundReport.fund_id == DimFund.fund_id)
        .filter(FactLpScaledExposure.lp_name == LP_NAME)
    )

    if sector:
        q = q.filter(sector_col == sector)
    if industry:
        q = q.filter(ig_col == industry)
    if sub_industry:
        q = q.filter(industry_col == sub_industry)

    total = q.count()
    total_pages = max(1, (total + page_size - 1) // page_size)
    rows = (
        q.order_by(func.coalesce(FactLpScaledExposure.scaled_value_usd, 0).desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return {
        "items": [
            {
                "holding_id": r.reported_holding_id,
                "company_id": r.company_id,
                "company_name": r.company_name,
                "fund_id": r.fund_id,
                "fund_name": r.fund_name,
                "reported_value_usd": (
                    float(r.scaled_value_usd) if r.scaled_value_usd is not None
                    else float(r.reported_value_usd) if r.reported_value_usd is not None
                    else None
                ),
                "reported_sector": r.reported_sector,
                "as_of_date": r.as_of_date,
                "source": r.source,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }
