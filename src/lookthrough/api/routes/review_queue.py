"""Review queue API endpoints for approving, rejecting, and dismissing flagged items."""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import case, func
from sqlalchemy.orm import Session

from src.lookthrough.auth.dependencies import get_current_user, get_db
from src.lookthrough.db.models import DimCompany, FactReviewQueueItem, User

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _item_to_dict(
    item: FactReviewQueueItem,
    company_name: Optional[str],
    primary_sector: Optional[str],
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

    items = [_item_to_dict(item, cname, csector) for item, cname, csector in rows]
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
