"""Dashboard API endpoints returning aggregated portfolio statistics."""

import csv
import io
import math
from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException
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
    FactReportedHolding,
    User,
)

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


@router.get("/exposure-trend")
def get_exposure_trend(
    dimension_type: str = "sector",
    periods: int = 8,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Portfolio-level exposure trend over time.

    Returns percentage allocations per taxonomy node across historical snapshots.
    dimension_type: 'sector' | 'geography' | 'industry'
    periods: number of most-recent snapshots to include (default 8)
    """
    return _build_trend_response(db, dimension_type, periods, fund_id="")


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
    """List all funds with holding count and total value."""
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
        )
        .join(FactFundReport, FactFundReport.fund_id == DimFund.fund_id)
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
            "vintage_year": int(row.vintage_year) if row.vintage_year is not None else None,
            "holding_count": row.holding_count,
            "total_value": float(row.total_value) if row.total_value is not None else None,
        }
        for row in rows
    ]


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
# GET /api/funds/{fund_id}
# ---------------------------------------------------------------------------


@funds_router.get("/{fund_id}")
def get_fund_detail(
    fund_id: str,
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Full fund profile: metadata, stats, sector breakdown, and top 10 holdings."""
    fund = db.query(DimFund).filter(DimFund.fund_id == fund_id).first()
    if fund is None:
        raise HTTPException(status_code=404, detail="Fund not found")

    # ---- Stats scoped to this fund ----
    stats = (
        db.query(
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
            func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
            func.count(distinct(FactReportedHolding.company_id)).label("unique_companies"),
            func.max(FactReportedHolding.as_of_date).label("as_of_date"),
        )
        .join(
            FactFundReport,
            FactReportedHolding.fund_report_id == FactFundReport.fund_report_id,
        )
        .filter(FactFundReport.fund_id == fund_id)
        .one()
    )

    holding_count = stats.holding_count or 0
    total_value = float(stats.total_value) if stats.total_value is not None else None

    # ---- Sector breakdown for this fund (top 10 by holding count) ----
    sector_rows = (
        db.query(
            func.coalesce(DimCompany.primary_sector, "Unclassified").label("sector"),
            func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .join(
            FactFundReport,
            FactReportedHolding.fund_report_id == FactFundReport.fund_report_id,
        )
        .filter(FactFundReport.fund_id == fund_id)
        .group_by(func.coalesce(DimCompany.primary_sector, "Unclassified"))
        .order_by(func.count(FactReportedHolding.reported_holding_id).desc())
        .limit(10)
        .all()
    )

    sector_breakdown = [
        {
            "sector": row.sector,
            "holding_count": row.holding_count,
            "percentage": round(row.holding_count / holding_count * 100, 2) if holding_count else 0,
        }
        for row in sector_rows
    ]

    # ---- Top 10 holdings by reported value ----
    top_holding_rows = (
        db.query(
            FactReportedHolding.company_id,
            func.coalesce(
                DimCompany.company_name, FactReportedHolding.raw_company_name
            ).label("company_name"),
            FactReportedHolding.reported_value_usd,
            FactReportedHolding.reported_sector,
        )
        .outerjoin(DimCompany, FactReportedHolding.company_id == DimCompany.company_id)
        .join(
            FactFundReport,
            FactReportedHolding.fund_report_id == FactFundReport.fund_report_id,
        )
        .filter(
            FactFundReport.fund_id == fund_id,
            FactReportedHolding.reported_value_usd.isnot(None),
        )
        .order_by(FactReportedHolding.reported_value_usd.desc())
        .limit(10)
        .all()
    )

    top_holdings = [
        {
            "company_id": row.company_id,
            "company_name": row.company_name,
            "reported_value_usd": float(row.reported_value_usd),
            "reported_sector": row.reported_sector,
            "pct_nav": (
                round(float(row.reported_value_usd) / total_value * 100, 2)
                if total_value
                else None
            ),
        }
        for row in top_holding_rows
    ]

    return {
        "fund_id": fund.fund_id,
        "fund_name": fund.fund_name,
        "manager_name": fund.manager_name,
        "fund_type": fund.fund_type,
        "strategy": fund.strategy,
        "vintage_year": int(fund.vintage_year) if (fund.vintage_year is not None and not math.isnan(fund.vintage_year)) else None,
        "base_currency": fund.base_currency,
        "source": fund.source,
        "holding_count": holding_count,
        "total_value": total_value,
        "unique_companies": stats.unique_companies or 0,
        "as_of_date": stats.as_of_date,
        "sector_breakdown": sector_breakdown,
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
    # entity_resolution_log is keyed by reported_holding_id, not company_id.
    # Step 1: collect all holding IDs for this company.
    holding_id_rows = (
        db.query(FactReportedHolding.reported_holding_id)
        .filter(FactReportedHolding.company_id == company_id)
        .all()
    )
    holding_ids = [r.reported_holding_id for r in holding_id_rows]
    # Step 2: look up resolution records by those holding IDs.
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
