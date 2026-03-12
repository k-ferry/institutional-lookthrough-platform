"""Holdings API endpoints — paginated list, filter options, and exports."""

import csv
import io
from collections import defaultdict
from datetime import date
from typing import Optional

import openpyxl
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from openpyxl.styles import Alignment, Font, PatternFill
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.lookthrough.auth.dependencies import get_current_user, get_db
from src.lookthrough.db.models import (
    DimCompany,
    DimFund,
    EntityResolutionLog,
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
            FactReportedHolding.company_id,
            company_name_col.label("company_name"),
            DimFund.fund_id,
            DimFund.fund_name,
            sector_col.label("sector"),
            country_col.label("country"),
            FactReportedHolding.reported_value_usd,
            FactReportedHolding.extraction_confidence,
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
            "company_id": row.company_id,
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
            "extraction_confidence": (
                float(row.extraction_confidence)
                if row.extraction_confidence is not None
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


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

EXPORT_COLUMNS = [
    "Company Name",
    "Fund",
    "Sector",
    "Industry",
    "Country",
    "Reported Value (USD)",
    "% NAV",
    "As of Date",
    "Source",
    "Extraction Confidence",
    "Match Method",
    "Match Confidence",
]

_HEADER_FILL = PatternFill("solid", fgColor="0F2B5B")
_HEADER_FONT = Font(color="FFFFFF", bold=True)


def _build_export_query(
    db: Session,
    search: Optional[str],
    fund_id: Optional[str],
    sector: Optional[str],
    has_value: Optional[bool],
):
    """Un-paginated query for export — same filters as list_holdings, plus entity resolution."""
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
            company_name_col.label("company_name"),
            DimFund.fund_id,
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
    )

    if search:
        query = query.filter(company_name_col.ilike(f"%{search}%"))
    if fund_id:
        query = query.filter(DimFund.fund_id == fund_id)
    if sector:
        query = query.filter(sector_col == sector)
    if has_value:
        query = query.filter(FactReportedHolding.reported_value_usd.isnot(None))

    return query


def _rows_to_dicts(rows) -> list[dict]:
    """Convert query rows to export dicts, computing % NAV from fund-level totals."""
    fund_totals: dict[str, float] = defaultdict(float)
    for row in rows:
        if row.reported_value_usd is not None:
            fund_totals[row.fund_id] += float(row.reported_value_usd)

    result = []
    for row in rows:
        value = float(row.reported_value_usd) if row.reported_value_usd is not None else None
        ft = fund_totals.get(row.fund_id, 0.0)
        pct_nav = round(value / ft * 100, 4) if (value is not None and ft > 0) else None
        result.append({
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
    return result


# ---------------------------------------------------------------------------
# GET /api/holdings/export — CSV
# ---------------------------------------------------------------------------


@router.get("/export")
def export_holdings_csv(
    search: Optional[str] = Query(None),
    fund_id: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    has_value: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """Export all holdings matching current filters as a CSV download."""
    rows = _build_export_query(db, search, fund_id, sector, has_value).all()
    dicts = _rows_to_dicts(rows)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=EXPORT_COLUMNS)
    writer.writeheader()
    writer.writerows(dicts)

    filename = f"lookthrough_holdings_{date.today().isoformat()}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# GET /api/holdings/export/excel — XLSX
# ---------------------------------------------------------------------------


@router.get("/export/excel")
def export_holdings_excel(
    search: Optional[str] = Query(None),
    fund_id: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    has_value: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
):
    """Export all holdings matching current filters as an Excel (.xlsx) download."""
    rows = _build_export_query(db, search, fund_id, sector, has_value).all()
    dicts = _rows_to_dicts(rows)

    wb = openpyxl.Workbook()

    # --- Sheet 1: Holdings ---
    ws = wb.active
    ws.title = "Holdings"
    ws.append(EXPORT_COLUMNS)
    for cell in ws[1]:
        cell.fill = _HEADER_FILL
        cell.font = _HEADER_FONT
        cell.alignment = Alignment(horizontal="center")

    for row_dict in dicts:
        ws.append([row_dict[col] for col in EXPORT_COLUMNS])

    for col in ws.columns:
        max_len = max((len(str(cell.value or "")) for cell in col), default=8)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)

    # --- Sheet 2: Summary ---
    ws2 = wb.create_sheet("Summary")

    def _styled_header(sheet, values):
        sheet.append(values)
        for cell in sheet[sheet.max_row]:
            cell.fill = _HEADER_FILL
            cell.font = _HEADER_FONT

    _NA = "N/A - value data not available for all holdings"
    total_aum = sum(
        float(r.reported_value_usd) for r in rows if r.reported_value_usd is not None
    )
    _styled_header(ws2, ["Metric", "Value"])
    ws2.append(["Total Holdings", len(rows)])
    ws2.append(["Total AUM (USD)", round(total_aum, 2) if total_aum is not None and total_aum > 0 else _NA])
    ws2.append(["Export Date", date.today().isoformat()])
    ws2.append([])

    fund_agg: dict[str, dict] = defaultdict(lambda: {"count": 0, "aum": 0.0, "has_value": False})
    for row in rows:
        fund_agg[row.fund_name]["count"] += 1
        if row.reported_value_usd is not None:
            fund_agg[row.fund_name]["aum"] += float(row.reported_value_usd)
            fund_agg[row.fund_name]["has_value"] = True
    _styled_header(ws2, ["Fund", "Holdings", "AUM (USD)"])
    for name, stats in sorted(fund_agg.items(), key=lambda x: x[1]["aum"], reverse=True):
        aum_cell = round(stats["aum"], 2) if stats["has_value"] else _NA
        ws2.append([name, stats["count"], aum_cell])
    ws2.append([])

    sector_agg: dict[str, dict] = defaultdict(lambda: {"count": 0, "aum": 0.0, "has_value": False})
    for row in rows:
        s = row.sector or "Unclassified"
        sector_agg[s]["count"] += 1
        if row.reported_value_usd is not None:
            sector_agg[s]["aum"] += float(row.reported_value_usd)
            sector_agg[s]["has_value"] = True
    _styled_header(ws2, ["Sector", "Holdings", "AUM (USD)"])
    for name, stats in sorted(sector_agg.items(), key=lambda x: x[1]["aum"], reverse=True):
        aum_cell = round(stats["aum"], 2) if stats["has_value"] else _NA
        ws2.append([name, stats["count"], aum_cell])

    for col in ws2.columns:
        max_len = max((len(str(cell.value or "")) for cell in col), default=8)
        ws2.column_dimensions[col[0].column_letter].width = min(max_len + 4, 40)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    filename = f"lookthrough_holdings_{date.today().isoformat()}.xlsx"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
