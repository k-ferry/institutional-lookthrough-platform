"""Review Queue Generator for Institutional Look-Through Platform.

Generates review queue items for cases that need human attention based on:
- Low confidence AI classifications
- Unclassifiable companies (null node_name)
- Unresolved entity matches
- Large unknown exposure buckets
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.lookthrough.schemas.gold_contracts import ReviewQueueItemRow, validate_dataframe


def _repo_root() -> Path:
    # src/lookthrough/governance/review_queue.py -> repo root is 4 parents up
    return Path(__file__).resolve().parents[3]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _determine_priority(confidence: float | None, match_method: str | None, reason: str) -> str:
    """Determine priority based on confidence and match method."""
    if match_method == "unresolved":
        return "high"
    if confidence is not None and confidence < 0.3:
        return "high"
    if confidence is not None and confidence < 0.7:
        return "medium"
    if reason == "large_unknown_exposure":
        return "medium"
    return "low"


def generate_review_queue() -> pd.DataFrame:
    """
    Generate review queue items from gold tables.

    Sources:
    - fact_exposure_classification.csv: low confidence or null classifications
    - entity_resolution_log.csv: unresolved entity matches
    - fact_inferred_exposure.csv: large unknown exposure buckets

    Returns:
        DataFrame of review queue items
    """
    root = _repo_root()
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    # Load source data
    classifications = _read_csv(gold / "fact_exposure_classification.csv")
    entity_log = _read_csv(gold / "entity_resolution_log.csv")
    exposures = _read_csv(gold / "fact_inferred_exposure.csv")

    queue_items: list[dict] = []
    created_at = datetime.now(timezone.utc).isoformat()

    # Condition A: AI classification confidence below 0.70
    if not classifications.empty and "confidence" in classifications.columns:
        low_confidence = classifications[classifications["confidence"] < 0.70]
        for _, row in low_confidence.iterrows():
            # Skip if also null classification (handled in condition B)
            taxonomy_node_id = row.get("taxonomy_node_id", "")
            if taxonomy_node_id == "00000000-0000-0000-0000-000000000000":
                continue

            confidence = float(row["confidence"])
            queue_items.append({
                "queue_item_id": str(uuid.uuid4()),
                "run_id": str(row.get("run_id", "")),
                "exposure_id": None,
                "reported_holding_id": None,
                "company_id": row.get("company_id") if pd.notna(row.get("company_id")) else None,
                "raw_company_name": str(row.get("raw_company_name", "")),
                "reason": "low_confidence_classification",
                "priority": _determine_priority(confidence, None, "low_confidence_classification"),
                "status": "pending",
                "created_at": created_at,
            })

    # Condition B: AI classification where node_name was null (unclassifiable)
    # Detected by taxonomy_node_id being the null UUID
    if not classifications.empty and "taxonomy_node_id" in classifications.columns:
        null_classifications = classifications[
            classifications["taxonomy_node_id"] == "00000000-0000-0000-0000-000000000000"
        ]
        for _, row in null_classifications.iterrows():
            confidence = float(row.get("confidence", 0.0))
            queue_items.append({
                "queue_item_id": str(uuid.uuid4()),
                "run_id": str(row.get("run_id", "")),
                "exposure_id": None,
                "reported_holding_id": None,
                "company_id": row.get("company_id") if pd.notna(row.get("company_id")) else None,
                "raw_company_name": str(row.get("raw_company_name", "")),
                "reason": "unclassifiable_company",
                "priority": _determine_priority(confidence, None, "unclassifiable_company"),
                "status": "pending",
                "created_at": created_at,
            })

    # Condition C: Entity resolution where match_method is unresolved
    if not entity_log.empty and "match_method" in entity_log.columns:
        unresolved = entity_log[entity_log["match_method"] == "unresolved"]
        for _, row in unresolved.iterrows():
            queue_items.append({
                "queue_item_id": str(uuid.uuid4()),
                "run_id": "",  # Entity resolution log doesn't have run_id
                "exposure_id": None,
                "reported_holding_id": str(row.get("reported_holding_id", "")),
                "company_id": row.get("matched_company_id") if pd.notna(row.get("matched_company_id")) else None,
                "raw_company_name": str(row.get("raw_company_name", "")),
                "reason": "unresolved_entity",
                "priority": "high",
                "status": "pending",
                "created_at": created_at,
            })

    # Condition D: Exposures where exposure_type is unknown and exposure_value_usd > 1000000
    if not exposures.empty:
        has_type = "exposure_type" in exposures.columns
        has_value = "exposure_value_usd" in exposures.columns
        if has_type and has_value:
            large_unknown = exposures[
                (exposures["exposure_type"].str.lower() == "unknown") &
                (exposures["exposure_value_usd"] > 1_000_000)
            ]
            for _, row in large_unknown.iterrows():
                queue_items.append({
                    "queue_item_id": str(uuid.uuid4()),
                    "run_id": str(row.get("run_id", "")),
                    "exposure_id": str(row.get("exposure_id", "")),
                    "reported_holding_id": None,
                    "company_id": row.get("company_id") if pd.notna(row.get("company_id")) else None,
                    "raw_company_name": str(row.get("raw_company_name", "")) if pd.notna(row.get("raw_company_name")) else None,
                    "reason": "large_unknown_exposure",
                    "priority": "medium",
                    "status": "pending",
                    "created_at": created_at,
                })

    # Create DataFrame
    queue_df = pd.DataFrame(queue_items)

    if queue_df.empty:
        print("No review queue items generated.")
        return queue_df

    # Validate against schema
    errors = validate_dataframe(queue_df, ReviewQueueItemRow)
    if errors:
        print("Validation errors:")
        for err in errors[:10]:  # Show first 10 errors
            print(f"  {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")

    # Write output
    out_path = gold / "fact_review_queue_item.csv"
    queue_df.to_csv(out_path, index=False)

    # Print summary
    print("Review Queue Summary")
    print("=" * 50)
    print(f"Total items: {len(queue_df)}")
    print()

    print("By reason:")
    reason_counts = queue_df["reason"].value_counts()
    for reason, count in reason_counts.items():
        print(f"  {reason}: {count}")
    print()

    print("By priority:")
    priority_counts = queue_df["priority"].value_counts()
    for priority in ["high", "medium", "low"]:
        count = priority_counts.get(priority, 0)
        print(f"  {priority}: {count}")
    print()

    print(f"Wrote: {out_path}")

    return queue_df


def main() -> None:
    generate_review_queue()


if __name__ == "__main__":
    main()
