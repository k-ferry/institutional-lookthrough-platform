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
    FactReportedHolding,
    GICSMapping,
    User,
)

gics_router = APIRouter(prefix="/api/gics", tags=["gics"])


# ---------------------------------------------------------------------------
# Shared column expressions
# ---------------------------------------------------------------------------

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
    total = db.query(func.count(FactReportedHolding.reported_holding_id)).scalar() or 1

    rows = (
        db.query(
            sector_col.label("sector"),
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.count(distinct(FactReportedHolding.company_id)).label("company_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .group_by(sector_col)
        .order_by(func.count(FactReportedHolding.reported_holding_id).desc())
        .all()
    )

    return {
        "sectors": [
            {
                "sector": row.sector,
                "holding_count": row.holding_count,
                "company_count": row.company_count,
                "total_value": float(row.total_value) if row.total_value is not None else None,
                "percentage": round(row.holding_count / total * 100, 2),
            }
            for row in rows
        ],
        "total_holdings": total,
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

    # Sector totals
    totals = (
        db.query(
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.count(distinct(FactReportedHolding.company_id)).label("company_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .filter(sector_col == sector_name)
        .one()
    )
    holding_total = totals.holding_count or 1

    # Industry groups
    ig_rows = (
        db.query(
            ig_col.label("industry_group"),
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.count(distinct(FactReportedHolding.company_id)).label("company_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .outerjoin(GICSMapping, FactReportedHolding.reported_sector == GICSMapping.reported_sector)
        .filter(sector_col == sector_name)
        .group_by(ig_col)
        .order_by(func.count(FactReportedHolding.reported_holding_id).desc())
        .all()
    )

    # Top 10 companies by value
    top_rows = (
        db.query(
            FactReportedHolding.company_id,
            company_name_col.label("company_name"),
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .filter(sector_col == sector_name)
        .group_by(FactReportedHolding.company_id, company_name_col)
        .order_by(func.coalesce(func.sum(FactReportedHolding.reported_value_usd), 0).desc())
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

    # Industry group totals
    totals = (
        db.query(
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.count(distinct(FactReportedHolding.company_id)).label("company_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .outerjoin(GICSMapping, FactReportedHolding.reported_sector == GICSMapping.reported_sector)
        .filter(ig_col == industry_name)
        .one()
    )
    holding_total = totals.holding_count or 1

    # Industries within the group
    industry_rows = (
        db.query(
            industry_col.label("industry"),
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.count(distinct(FactReportedHolding.company_id)).label("company_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .outerjoin(GICSMapping, FactReportedHolding.reported_sector == GICSMapping.reported_sector)
        .filter(ig_col == industry_name)
        .group_by(industry_col)
        .order_by(func.count(FactReportedHolding.reported_holding_id).desc())
        .all()
    )

    # Top 10 companies
    top_rows = (
        db.query(
            FactReportedHolding.company_id,
            company_name_col.label("company_name"),
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .outerjoin(GICSMapping, FactReportedHolding.reported_sector == GICSMapping.reported_sector)
        .filter(ig_col == industry_name)
        .group_by(FactReportedHolding.company_id, company_name_col)
        .order_by(func.coalesce(func.sum(FactReportedHolding.reported_value_usd), 0).desc())
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
    """Paginated holdings filtered to any GICS taxonomy level."""
    sector_col = _sector_col()
    company_name_col = _company_name_col()
    ig_col = func.coalesce(GICSMapping.gics_industry_group_name, "Unknown")
    industry_col = func.coalesce(GICSMapping.gics_industry_name, "Unknown")

    q = (
        db.query(
            FactReportedHolding.reported_holding_id,
            FactReportedHolding.company_id,
            company_name_col.label("company_name"),
            DimFund.fund_id,
            DimFund.fund_name,
            FactReportedHolding.reported_value_usd,
            FactReportedHolding.reported_sector,
            FactReportedHolding.as_of_date,
            FactReportedHolding.source,
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .outerjoin(GICSMapping, FactReportedHolding.reported_sector == GICSMapping.reported_sector)
        .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
        .join(DimFund, FactFundReport.fund_id == DimFund.fund_id)
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
        q.order_by(func.coalesce(FactReportedHolding.reported_value_usd, 0).desc())
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
                "reported_value_usd": float(r.reported_value_usd) if r.reported_value_usd is not None else None,
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
