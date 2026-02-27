"""Audit Trail Generator for Institutional Look-Through Platform.

Generates an append-only audit trail that logs every significant system action.
This is critical for governance and explainability.

Supports both PostgreSQL and CSV modes:
- Default: Read/write from PostgreSQL database
- CSV mode: Set CSV_MODE=1 or use --csv flag for backward compatibility
"""
from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.lookthrough.db.repository import (
    _is_csv_mode,
    bulk_insert,
    dataframe_to_records,
    get_all,
)
from src.lookthrough.db.models import (
    EntityResolutionLog,
    FactAuditEvent,
    FactExposureClassification,
    FactInferredExposure,
    FactReviewQueueItem,
)
from src.lookthrough.schemas.gold_contracts import AuditEventRow, validate_dataframe


def _repo_root() -> Path:
    # src/lookthrough/governance/audit.py -> repo root is 4 parents up
    return Path(__file__).resolve().parents[3]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def generate_audit_trail(csv_mode: bool = False) -> pd.DataFrame:
    """
    Generate audit events from Gold table outputs.

    Sources:
    - fact_exposure_classification: AI classification events
    - fact_review_queue_item: Review queue insert events
    - entity_resolution_log: Entity resolution events
    - fact_inferred_exposure: Pipeline run completion event

    Args:
        csv_mode: If True, use CSV files instead of database

    Returns:
        DataFrame of audit events
    """
    root = _repo_root()
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    # Load source data from DB or CSV
    if csv_mode:
        classifications = _read_csv(gold / "fact_exposure_classification.csv")
        review_queue = _read_csv(gold / "fact_review_queue_item.csv")
        entity_log = _read_csv(gold / "entity_resolution_log.csv")
        exposures = _read_csv(gold / "fact_inferred_exposure.csv")
    else:
        classifications = get_all(FactExposureClassification)
        review_queue = get_all(FactReviewQueueItem)
        entity_log = get_all(EntityResolutionLog)
        exposures = get_all(FactInferredExposure)

    audit_events: list[dict] = []
    event_time = datetime.now(timezone.utc).isoformat()

    # Track row counts for pipeline_run_complete event
    row_counts = {
        "classifications": len(classifications),
        "review_queue_items": len(review_queue),
        "entity_resolutions": len(entity_log),
        "exposures": len(exposures),
    }

    # Event A: AI classifications
    if not classifications.empty:
        for _, row in classifications.iterrows():
            payload = {
                "taxonomy_type": str(row.get("taxonomy_type", "")),
                "node_name": str(row.get("taxonomy_node_id", "")),
                "confidence": float(row.get("confidence", 0.0)),
                "rationale": str(row.get("rationale", "")),
            }
            company_id = row.get("company_id")
            if pd.isna(company_id):
                company_id = row.get("classification_id", str(uuid.uuid4()))

            audit_events.append({
                "audit_event_id": str(uuid.uuid4()),
                "run_id": str(row.get("run_id", "")),
                "event_time": event_time,
                "actor_type": "system",
                "actor_id": str(row.get("model", "unknown_model")),
                "action": "ai_classification",
                "entity_type": "company",
                "entity_id": str(company_id),
                "payload_json": json.dumps(payload),
            })

    # Event B: Review queue items created
    if not review_queue.empty:
        for _, row in review_queue.iterrows():
            payload = {
                "reason": str(row.get("reason", "")),
                "priority": str(row.get("priority", "")),
            }
            audit_events.append({
                "audit_event_id": str(uuid.uuid4()),
                "run_id": str(row.get("run_id", "")),
                "event_time": event_time,
                "actor_type": "system",
                "actor_id": "review_queue_generator",
                "action": "review_queue_insert",
                "entity_type": "review_queue_item",
                "entity_id": str(row.get("queue_item_id", "")),
                "payload_json": json.dumps(payload),
            })

    # Event C: Entity resolutions
    if not entity_log.empty:
        # Entity resolution log doesn't have run_id, use from exposures if available
        default_run_id = ""
        if not exposures.empty and "run_id" in exposures.columns:
            default_run_id = str(exposures["run_id"].iloc[0])

        for _, row in entity_log.iterrows():
            matched_company_id = row.get("matched_company_id")
            payload = {
                "match_method": str(row.get("match_method", "")),
                "match_confidence": float(row.get("match_confidence", 0.0)),
                "matched_company_id": str(matched_company_id) if pd.notna(matched_company_id) else None,
            }
            audit_events.append({
                "audit_event_id": str(uuid.uuid4()),
                "run_id": default_run_id,
                "event_time": event_time,
                "actor_type": "system",
                "actor_id": "entity_resolver",
                "action": "entity_resolution",
                "entity_type": "holding",
                "entity_id": str(row.get("reported_holding_id", "")),
                "payload_json": json.dumps(payload),
            })

    # Event D: Pipeline run complete
    if not exposures.empty and "run_id" in exposures.columns:
        run_id = str(exposures["run_id"].iloc[0])
        payload = {
            "classification_count": row_counts["classifications"],
            "review_queue_count": row_counts["review_queue_items"],
            "entity_resolution_count": row_counts["entity_resolutions"],
            "exposure_count": row_counts["exposures"],
        }
        audit_events.append({
            "audit_event_id": str(uuid.uuid4()),
            "run_id": run_id,
            "event_time": event_time,
            "actor_type": "system",
            "actor_id": "run_pipeline",
            "action": "pipeline_run_complete",
            "entity_type": "pipeline",
            "entity_id": run_id,
            "payload_json": json.dumps(payload),
        })

    # Create DataFrame
    audit_df = pd.DataFrame(audit_events)

    if audit_df.empty:
        print("No audit events generated.")
        return audit_df

    # Validate against schema
    errors = validate_dataframe(audit_df, AuditEventRow)
    if errors:
        print("Validation errors:")
        for err in errors[:10]:  # Show first 10 errors
            print(f"  {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")

    # Write output (append-only, don't delete existing)
    if csv_mode:
        out_path = gold / "fact_audit_event.csv"
        audit_df.to_csv(out_path, index=False)
    else:
        # Append to existing audit events (don't delete)
        bulk_insert(FactAuditEvent, dataframe_to_records(audit_df))
        out_path = "PostgreSQL:fact_audit_event"

    # Print summary
    print("Audit Trail Summary")
    print("=" * 50)
    print(f"Total events: {len(audit_df)}")
    print()

    print("By action:")
    action_counts = audit_df["action"].value_counts()
    for action, count in action_counts.items():
        print(f"  {action}: {count}")
    print()

    print("By actor_id:")
    actor_counts = audit_df["actor_id"].value_counts()
    for actor, count in actor_counts.items():
        print(f"  {actor}: {count}")
    print()

    print(f"Wrote: {out_path}")

    return audit_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate audit trail")
    parser.add_argument("--csv", action="store_true", help="Use CSV mode instead of PostgreSQL")
    args = parser.parse_args()

    # Check CSV mode from args or environment
    csv_mode = args.csv or _is_csv_mode()
    print(f"Data mode: {'CSV' if csv_mode else 'PostgreSQL'}")

    generate_audit_trail(csv_mode=csv_mode)


if __name__ == "__main__":
    main()
