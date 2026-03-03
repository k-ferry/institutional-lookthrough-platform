"""Dashboard API endpoints returning aggregated portfolio statistics."""

import math

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import distinct, func
from sqlalchemy.orm import Session

from src.lookthrough.auth.dependencies import get_current_user, get_db
from src.lookthrough.db.models import (
    DimCompany,
    DimFund,
    EntityResolutionLog,
    FactAuditEvent,
    FactExposureClassification,
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
    resolution_rows = (
        db.query(EntityResolutionLog)
        .filter(EntityResolutionLog.matched_company_id == company_id)
        .all()
    )
    raw_names = list({r.raw_company_name for r in resolution_rows if r.raw_company_name})
    methods = list({r.match_method for r in resolution_rows if r.match_method})
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
