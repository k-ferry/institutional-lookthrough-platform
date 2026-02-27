"""Holdings API endpoints — paginated list and filter options."""

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.lookthrough.auth.dependencies import get_current_user, get_db
from src.lookthrough.db.models import (
    DimCompany,
    DimFund,
    FactFundReport,
    FactReportedHolding,
    User,
)

router = APIRouter(prefix="/api/holdings", tags=["holdings"])


# ---------------------------------------------------------------------------
# GET /api/holdings
# ---------------------------------------------------------------------------


@router.get("")
def list_holdings(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = Query(None),
    fund_id: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    has_value: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Return a paginated list of holdings with optional filters.

    Joins fact_reported_holding → fact_fund_report → dim_fund and
    optionally dim_company to resolve canonical names and sectors.
    """
    company_name_col = func.coalesce(
        DimCompany.company_name, FactReportedHolding.raw_company_name
    )
    sector_col = func.coalesce(
        DimCompany.primary_sector, FactReportedHolding.reported_sector
    )
    country_col = func.coalesce(
        DimCompany.primary_country, FactReportedHolding.reported_country
    )

    query = (
        db.query(
            FactReportedHolding.reported_holding_id,
            company_name_col.label("company_name"),
            DimFund.fund_id,
            DimFund.fund_name,
            sector_col.label("sector"),
            country_col.label("country"),
            FactReportedHolding.reported_value_usd,
            FactReportedHolding.as_of_date,
            FactReportedHolding.source,
        )
        .join(
            FactFundReport,
            FactReportedHolding.fund_report_id == FactFundReport.fund_report_id,
        )
        .join(DimFund, FactFundReport.fund_id == DimFund.fund_id)
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
    )

    if search:
        query = query.filter(company_name_col.ilike(f"%{search}%"))
    if fund_id:
        query = query.filter(DimFund.fund_id == fund_id)
    if sector:
        query = query.filter(sector_col == sector)
    if has_value:
        query = query.filter(FactReportedHolding.reported_value_usd.isnot(None))

    total = query.count()
    total_pages = max(1, (total + page_size - 1) // page_size)
    offset = (page - 1) * page_size
    rows = query.offset(offset).limit(page_size).all()

    items = [
        {
            "holding_id": row.reported_holding_id,
            "company_name": row.company_name,
            "fund_id": row.fund_id,
            "fund_name": row.fund_name,
            "sector": row.sector,
            "country": row.country,
            "reported_value": (
                float(row.reported_value_usd)
                if row.reported_value_usd is not None
                else None
            ),
            "date_reported": row.as_of_date,
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
# GET /api/holdings/filters
# ---------------------------------------------------------------------------


@router.get("/filters")
def get_filter_options(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Return available filter options for the holdings explorer.

    Returns funds with holdings, distinct coalesced sectors, and the count
    of holdings that have a reported value.
    """
    # Funds that actually have holdings (deduplicated by fund_id)
    fund_rows = (
        db.query(DimFund.fund_id, DimFund.fund_name)
        .join(FactFundReport, FactFundReport.fund_id == DimFund.fund_id)
        .join(
            FactReportedHolding,
            FactReportedHolding.fund_report_id == FactFundReport.fund_report_id,
        )
        .distinct()
        .order_by(DimFund.fund_name)
        .all()
    )
    funds = [{"id": r.fund_id, "name": r.fund_name} for r in fund_rows]

    # Distinct coalesced sectors present in holdings
    sector_col = func.coalesce(
        DimCompany.primary_sector, FactReportedHolding.reported_sector
    )
    sector_rows = (
        db.query(sector_col.label("sector"))
        .select_from(FactReportedHolding)
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .filter(sector_col.isnot(None))
        .distinct()
        .order_by(sector_col)
        .all()
    )
    sectors = [r.sector for r in sector_rows]

    # Count of holdings that have a non-null reported value
    has_value_count = (
        db.query(func.count(FactReportedHolding.reported_holding_id))
        .filter(FactReportedHolding.reported_value_usd.isnot(None))
        .scalar()
        or 0
    )

    return {
        "funds": funds,
        "sectors": sectors,
        "has_value_count": has_value_count,
    }
