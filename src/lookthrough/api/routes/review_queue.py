"""Review queue API endpoints for approving, rejecting, and dismissing flagged items."""

import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import case, func
from sqlalchemy.orm import Session

from src.lookthrough.auth.dependencies import get_current_user, get_db
from src.lookthrough.db.models import (
    DimCompany,
    DimFund,
    DimTaxonomyNode,
    EntityResolutionLog,
    FactAuditEvent,
    FactExposureClassification,
    FactFundReport,
    FactReportedHolding,
    FactReviewQueueItem,
    User,
)

router = APIRouter(prefix="/api/review-queue", tags=["review-queue"])

VALID_STATUSES = {"pending", "approved", "rejected", "dismissed"}


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------


class StatusUpdateRequest(BaseModel):
    status: str
    reviewer_notes: Optional[str] = None


class BulkStatusUpdateRequest(BaseModel):
    item_ids: list[str]
    status: str
    reviewer_notes: Optional[str] = None


class ResearchRequest(BaseModel):
    company_name: str
    company_id: Optional[str] = None
    raw_company_name: Optional[str] = None
    reported_sector: Optional[str] = None
    provider: str = "claude"  # "claude" | "openai" | "ollama"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _item_to_dict(
    item: FactReviewQueueItem,
    company_name: Optional[str],
    primary_sector: Optional[str],
    *,
    reported_sector: Optional[str] = None,
    ai_classification: Optional[dict] = None,
    match_method: Optional[str] = None,
    match_confidence: Optional[float] = None,
    fund_names: Optional[list] = None,
    holding_count: Optional[int] = None,
    reported_value_usd: Optional[float] = None,
) -> dict:
    return {
        "queue_item_id": item.queue_item_id,
        "company_id": item.company_id,
        "company_name": company_name or item.raw_company_name,
        "raw_company_name": item.raw_company_name,
        "reason": item.reason,
        "priority": item.priority,
        "status": item.status,
        "created_at": item.created_at,
        "reviewer_notes": item.reviewer_notes,
        "resolved_at": item.resolved_at,
        "resolved_by": item.resolved_by,
        "primary_sector": primary_sector,
        # Enriched fields
        "reported_sector": reported_sector,
        "ai_classification": ai_classification,
        "match_method": match_method,
        "match_confidence": match_confidence,
        "fund_names": fund_names or [],
        "holding_count": holding_count,
        "reported_value_usd": reported_value_usd,
    }


# Module-level SQLAlchemy expression for priority sort order (high=1, medium=2, low=3)
_PRIORITY_ORDER = case(
    (FactReviewQueueItem.priority == "high", 1),
    (FactReviewQueueItem.priority == "medium", 2),
    (FactReviewQueueItem.priority == "low", 3),
    else_=4,
)


# ---------------------------------------------------------------------------
# GET /api/review-queue
# ---------------------------------------------------------------------------


@router.get("")
def list_queue_items(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    status: str = Query(default="pending"),
    priority: str = Query(default="all"),
    reason: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Paginated list of review queue items with optional filters.

    status: pending | approved | rejected | dismissed | all  (default: pending)
    priority: high | medium | low | all  (default: all)
    reason: exact match on reason field (optional)

    Each item includes per-company enrichment: reported_sector, ai_classification,
    match_method/confidence from entity resolution, fund_names, holding_count,
    and total reported_value_usd.
    """
    q = (
        db.query(
            FactReviewQueueItem,
            DimCompany.company_name,
            DimCompany.primary_sector,
        )
        .outerjoin(DimCompany, FactReviewQueueItem.company_id == DimCompany.company_id)
    )

    if status != "all":
        q = q.filter(FactReviewQueueItem.status == status)
    if priority != "all":
        q = q.filter(FactReviewQueueItem.priority == priority)
    if reason:
        q = q.filter(FactReviewQueueItem.reason == reason)

    total = q.count()

    rows = (
        q.order_by(_PRIORITY_ORDER, FactReviewQueueItem.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    # Global status counts (unfiltered) — used for tab badges in the UI
    status_count_rows = (
        db.query(
            FactReviewQueueItem.status,
            func.count(FactReviewQueueItem.queue_item_id).label("cnt"),
        )
        .group_by(FactReviewQueueItem.status)
        .all()
    )
    counts = {"pending": 0, "approved": 0, "rejected": 0, "dismissed": 0}
    for row in status_count_rows:
        if row.status in counts:
            counts[row.status] = row.cnt

    # -----------------------------------------------------------------------
    # Per-company enrichment — 5 batched secondary queries for the current page
    # -----------------------------------------------------------------------
    company_ids = [item.company_id for item, _, _ in rows if item.company_id is not None]

    holdings_map: dict[str, dict] = {}
    sector_map: dict[str, str] = {}
    funds_map: dict[str, list] = {}
    classif_map: dict[str, dict] = {}
    er_map: dict[str, dict] = {}

    if company_ids:
        # Holdings count and total value per company
        for r in (
            db.query(
                FactReportedHolding.company_id,
                func.count(FactReportedHolding.reported_holding_id).label("holding_count"),
                func.sum(FactReportedHolding.reported_value_usd).label("total_value"),
            )
            .filter(FactReportedHolding.company_id.in_(company_ids))
            .group_by(FactReportedHolding.company_id)
            .all()
        ):
            holdings_map[str(r.company_id)] = {
                "holding_count": r.holding_count,
                "reported_value_usd": float(r.total_value) if r.total_value is not None else None,
            }

        # Most common reported_sector per company
        sector_best: dict[str, tuple] = {}
        for r in (
            db.query(
                FactReportedHolding.company_id,
                FactReportedHolding.reported_sector,
                func.count(FactReportedHolding.reported_holding_id).label("cnt"),
            )
            .filter(
                FactReportedHolding.company_id.in_(company_ids),
                FactReportedHolding.reported_sector.isnot(None),
            )
            .group_by(FactReportedHolding.company_id, FactReportedHolding.reported_sector)
            .all()
        ):
            cid = str(r.company_id)
            if cid not in sector_best or r.cnt > sector_best[cid][1]:
                sector_best[cid] = (r.reported_sector, r.cnt)
        sector_map = {cid: v[0] for cid, v in sector_best.items()}

        # Fund names per company (distinct)
        for r in (
            db.query(FactReportedHolding.company_id, DimFund.fund_name)
            .distinct()
            .join(FactFundReport, FactReportedHolding.fund_report_id == FactFundReport.fund_report_id)
            .join(DimFund, FactFundReport.fund_id == DimFund.fund_id)
            .filter(FactReportedHolding.company_id.in_(company_ids))
            .all()
        ):
            funds_map.setdefault(str(r.company_id), []).append(r.fund_name)

        # Best AI industry classification per company (highest confidence first)
        for classif, node_name in (
            db.query(FactExposureClassification, DimTaxonomyNode.node_name)
            .outerjoin(
                DimTaxonomyNode,
                FactExposureClassification.taxonomy_node_id == DimTaxonomyNode.taxonomy_node_id,
            )
            .filter(
                FactExposureClassification.company_id.in_(company_ids),
                FactExposureClassification.taxonomy_type == "industry",
            )
            .order_by(FactExposureClassification.confidence.desc())
            .all()
        ):
            cid = str(classif.company_id)
            if cid not in classif_map:
                classif_map[cid] = {
                    "taxonomy_node_id": classif.taxonomy_node_id,
                    "node_name": node_name,
                    "confidence": float(classif.confidence) if classif.confidence is not None else None,
                    "rationale": classif.rationale,
                    "model": classif.model,
                }

        # Entity resolution: first known match method per company
        for r in (
            db.query(
                EntityResolutionLog.matched_company_id,
                EntityResolutionLog.match_method,
                EntityResolutionLog.match_confidence,
            )
            .filter(
                EntityResolutionLog.matched_company_id.in_(company_ids),
                EntityResolutionLog.match_method.isnot(None),
            )
            .all()
        ):
            cid = str(r.matched_company_id)
            if cid not in er_map:
                er_map[cid] = {
                    "match_method": r.match_method,
                    "match_confidence": (
                        float(r.match_confidence) if r.match_confidence is not None else None
                    ),
                }

    # Build final item list with enrichment merged in
    items = []
    for item, company_name, primary_sector in rows:
        cid = str(item.company_id) if item.company_id else None
        h = holdings_map.get(cid, {}) if cid else {}
        er = er_map.get(cid, {}) if cid else {}
        items.append(
            _item_to_dict(
                item,
                company_name,
                primary_sector,
                reported_sector=sector_map.get(cid) if cid else None,
                ai_classification=classif_map.get(cid) if cid else None,
                match_method=er.get("match_method"),
                match_confidence=er.get("match_confidence"),
                fund_names=funds_map.get(cid, []) if cid else [],
                holding_count=h.get("holding_count"),
                reported_value_usd=h.get("reported_value_usd"),
            )
        )

    return {"items": items, "total": total, "counts": counts}


# ---------------------------------------------------------------------------
# GET /api/review-queue/stats
# ---------------------------------------------------------------------------


@router.get("/stats")
def get_queue_stats(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Summary counts for the review queue header."""
    total = db.query(func.count(FactReviewQueueItem.queue_item_id)).scalar() or 0

    status_rows = (
        db.query(
            FactReviewQueueItem.status,
            func.count(FactReviewQueueItem.queue_item_id).label("cnt"),
        )
        .group_by(FactReviewQueueItem.status)
        .all()
    )
    by_status = {"pending": 0, "approved": 0, "rejected": 0, "dismissed": 0}
    for row in status_rows:
        if row.status in by_status:
            by_status[row.status] = row.cnt

    high_priority = (
        db.query(func.count(FactReviewQueueItem.queue_item_id))
        .filter(
            FactReviewQueueItem.priority == "high",
            FactReviewQueueItem.status == "pending",
        )
        .scalar() or 0
    )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    approved_today = (
        db.query(func.count(FactReviewQueueItem.queue_item_id))
        .filter(
            FactReviewQueueItem.status == "approved",
            FactReviewQueueItem.resolved_at.like(f"{today}%"),
        )
        .scalar() or 0
    )

    reason_rows = (
        db.query(
            FactReviewQueueItem.reason,
            func.count(FactReviewQueueItem.queue_item_id).label("cnt"),
        )
        .filter(FactReviewQueueItem.status == "pending")
        .group_by(FactReviewQueueItem.reason)
        .all()
    )
    by_reason = {
        "unresolved_entity": 0,
        "low_confidence_classification": 0,
        "unclassifiable_company": 0,
        "large_unknown_exposure": 0,
    }
    for row in reason_rows:
        if row.reason in by_reason:
            by_reason[row.reason] = row.cnt

    return {
        "total": total,
        "pending": by_status["pending"],
        "high_priority": high_priority,
        "approved_today": approved_today,
        "by_reason": by_reason,
    }


# ---------------------------------------------------------------------------
# PATCH /api/review-queue/bulk  (must be defined before /{item_id})
# ---------------------------------------------------------------------------


@router.patch("/bulk")
def bulk_update_queue_items(
    body: BulkStatusUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    """Bulk update status for multiple queue items.

    Returns {updated: int} with the count of rows changed.
    """
    if body.status not in VALID_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {sorted(VALID_STATUSES)}",
        )
    if not body.item_ids:
        return {"updated": 0}

    now = datetime.now(timezone.utc).isoformat()
    updated = (
        db.query(FactReviewQueueItem)
        .filter(FactReviewQueueItem.queue_item_id.in_(body.item_ids))
        .update(
            {
                "status": body.status,
                "reviewer_notes": body.reviewer_notes,
                "resolved_at": now,
                "resolved_by": current_user.email,
            },
            synchronize_session=False,
        )
    )
    db.commit()
    return {"updated": updated}


# ---------------------------------------------------------------------------
# POST /api/review-queue/research
# ---------------------------------------------------------------------------


@router.post("/research")
def research_company(
    body: ResearchRequest,
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Research a company using a selected LLM to aid manual classification.

    Returns {response, provider, model, duration_ms} on success or {error} on failure.
    Provider must be one of: claude, openai, ollama.
    """
    if body.provider == "ollama":
        prompt = (
            f"You are a financial analyst. For this company: {body.company_name} "
            f"(reported sector: {body.reported_sector or 'unknown'}), briefly state: "
            "1) what the company likely does, 2) the most likely GICS sector, "
            "3) confidence level. Be concise, 3-5 sentences max."
        )
    else:
        prompt = (
            "You are a financial analyst assistant. Research this company and provide a brief classification summary.\n"
            f"Company name: {body.company_name}\n"
            f"Raw name from filing: {body.raw_company_name or 'N/A'}\n"
            f"Reported sector: {body.reported_sector or 'Not provided'}\n\n"
            "Please provide:\n"
            "1. What this company does (2-3 sentences)\n"
            "2. Most likely GICS sector and industry classification with reasoning\n"
            "3. Confidence level (high/medium/low) and why\n"
            "4. Any notes that would help an analyst classify this company\n\n"
            "Be concise. If you don't have reliable information about this specific company, "
            "say so clearly rather than guessing."
        )

    start = time.monotonic()

    if body.provider == "claude":
        try:
            import anthropic
        except ImportError:
            return {"error": "Anthropic SDK not installed. Run: pip install anthropic"}
        try:
            client = anthropic.Anthropic()
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            text = "".join(
                block.text for block in response.content if hasattr(block, "text")
            )
            duration_ms = int((time.monotonic() - start) * 1000)
            return {
                "response": text,
                "provider": "claude",
                "model": "claude-sonnet-4-20250514",
                "duration_ms": duration_ms,
            }
        except Exception as e:
            return {"error": str(e)}

    elif body.provider == "openai":
        try:
            from openai import OpenAI
        except ImportError:
            return {"error": "OpenAI SDK not installed. Run: pip install openai"}
        try:
            client = OpenAI()
            response = client.chat.completions.create(
                model="gpt-4o",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.choices[0].message.content or ""
            duration_ms = int((time.monotonic() - start) * 1000)
            return {
                "response": text,
                "provider": "openai",
                "model": "gpt-4o",
                "duration_ms": duration_ms,
            }
        except Exception as e:
            return {"error": str(e)}

    elif body.provider == "ollama":
        try:
            import requests as _requests
        except ImportError:
            return {"error": "requests library not installed. Run: pip install requests"}
        try:
            resp = _requests.post(
                "http://localhost:11434/api/chat",
                json={
                    "model": "llama3.1",
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                },
                timeout=300,
            )
            resp.raise_for_status()
            data = resp.json()
            text = data.get("message", {}).get("content", "")
            duration_ms = int((time.monotonic() - start) * 1000)
            return {
                "response": text,
                "provider": "ollama",
                "model": "llama3.1",
                "duration_ms": duration_ms,
            }
        except _requests.exceptions.ConnectionError:
            return {"error": "Cannot connect to Ollama. Is it running at localhost:11434?"}
        except Exception as e:
            return {"error": str(e)}

    else:
        return {"error": f"Unknown provider '{body.provider}'. Must be: claude, openai, or ollama."}


# ---------------------------------------------------------------------------
# PATCH /api/review-queue/{item_id}
# ---------------------------------------------------------------------------


@router.patch("/{item_id}")
def update_queue_item(
    item_id: str,
    body: StatusUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    """Update status and optional reviewer notes for a single queue item.

    Sets resolved_at to the current UTC timestamp and resolved_by to the
    current user's email. Returns the updated item.
    """
    if body.status not in VALID_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {sorted(VALID_STATUSES)}",
        )

    item = (
        db.query(FactReviewQueueItem)
        .filter(FactReviewQueueItem.queue_item_id == item_id)
        .first()
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Queue item not found")

    item.status = body.status
    item.reviewer_notes = body.reviewer_notes
    item.resolved_at = datetime.now(timezone.utc).isoformat()
    item.resolved_by = current_user.email
    db.commit()
    db.refresh(item)

    company = None
    if item.company_id:
        company = (
            db.query(DimCompany)
            .filter(DimCompany.company_id == item.company_id)
            .first()
        )

    return _item_to_dict(
        item,
        company.company_name if company else None,
        company.primary_sector if company else None,
    )


# ===========================================================================
# Audit Trail router  —  GET /api/audit-trail
# ===========================================================================

audit_router = APIRouter(prefix="/api", tags=["audit-trail"])


@audit_router.get("/audit-trail")
def list_audit_events(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    action: Optional[str] = Query(default=None),
    entity_type: Optional[str] = Query(default=None),
    days: Optional[int] = Query(default=None),
    entity_id: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Paginated audit trail with optional filters.

    days: integer cutoff (e.g. 1=today, 7=last week, 30=last month, None=all)
    """
    q = db.query(FactAuditEvent)

    if action:
        q = q.filter(FactAuditEvent.action == action)
    if entity_type:
        q = q.filter(FactAuditEvent.entity_type == entity_type)
    if entity_id:
        q = q.filter(FactAuditEvent.entity_id.ilike(f"%{entity_id}%"))
    if days is not None:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        q = q.filter(FactAuditEvent.event_time >= cutoff)

    total = q.count()
    rows = (
        q.order_by(FactAuditEvent.event_time.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    items = [
        {
            "audit_event_id": r.audit_event_id,
            "run_id": r.run_id,
            "event_time": r.event_time,
            "actor_type": r.actor_type,
            "actor_id": r.actor_id,
            "action": r.action,
            "entity_type": r.entity_type,
            "entity_id": r.entity_id,
            "payload_json": r.payload_json,
        }
        for r in rows
    ]
    return {"items": items, "total": total}


# ===========================================================================
# Pipeline stats router  —  GET /api/pipeline/stats
# ===========================================================================

pipeline_router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


@pipeline_router.get("/stats")
def get_pipeline_stats(
    db: Session = Depends(get_db),
    _current_user: User = Depends(get_current_user),
) -> dict:
    """Data quality and coverage metrics for the Pipeline Monitor page."""
    total_companies = db.query(func.count(DimCompany.company_id)).scalar() or 0
    classified = (
        db.query(func.count(DimCompany.company_id))
        .filter(DimCompany.primary_sector.isnot(None))
        .scalar() or 0
    )

    total_holdings = (
        db.query(func.count(FactReportedHolding.reported_holding_id)).scalar() or 0
    )
    resolved_holdings = (
        db.query(func.count(FactReportedHolding.reported_holding_id))
        .filter(FactReportedHolding.company_id.isnot(None))
        .scalar() or 0
    )
    holdings_with_value = (
        db.query(func.count(FactReportedHolding.reported_holding_id))
        .filter(FactReportedHolding.reported_value_usd.isnot(None))
        .scalar() or 0
    )
    synthetic_holdings = (
        db.query(func.count(FactReportedHolding.reported_holding_id))
        .filter(FactReportedHolding.source == "synthetic")
        .scalar() or 0
    )
    bdc_holdings = total_holdings - synthetic_holdings

    recent_run_rows = (
        db.query(FactAuditEvent)
        .filter(FactAuditEvent.action == "pipeline_run_complete")
        .order_by(FactAuditEvent.event_time.desc())
        .limit(5)
        .all()
    )
    recent_runs = [
        {
            "audit_event_id": r.audit_event_id,
            "run_id": r.run_id,
            "event_time": r.event_time,
            "payload_json": r.payload_json,
        }
        for r in recent_run_rows
    ]

    def pct(num: int, denom: int) -> float:
        return round(num / denom * 100, 1) if denom else 0.0

    return {
        "total_companies": total_companies,
        "classified_companies": classified,
        "unclassified_companies": total_companies - classified,
        "classification_coverage": pct(classified, total_companies),
        "total_holdings": total_holdings,
        "entity_resolution_rate": pct(resolved_holdings, total_holdings),
        "holdings_with_value_pct": pct(holdings_with_value, total_holdings),
        "bdc_holdings": bdc_holdings,
        "synthetic_holdings": synthetic_holdings,
        "recent_runs": recent_runs,
    }
