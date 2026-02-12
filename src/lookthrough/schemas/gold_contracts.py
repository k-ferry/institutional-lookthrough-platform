"""Pydantic models defining schema contracts for all Gold table outputs.

These models validate dataframes before writing to CSV, ensuring data integrity
and consistency across the look-through pipeline.
"""
from __future__ import annotations

from typing import Literal, Optional

import pandas as pd
from pydantic import BaseModel, Field, ValidationError


class InferredExposureRow(BaseModel):
    """Schema for fact_inferred_exposure.csv rows."""

    exposure_id: str
    run_id: str
    portfolio_id: str
    fund_id: str
    company_id: Optional[str] = None
    raw_company_name: Optional[str] = None
    as_of_date: str
    exposure_value_usd: float
    exposure_weight: float
    exposure_type: str
    method: str


class ExposureClassificationRow(BaseModel):
    """Schema for fact_exposure_classification.csv rows."""

    classification_id: str
    run_id: str
    company_id: Optional[str] = None
    raw_company_name: str
    taxonomy_type: str
    taxonomy_node_id: str
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str
    assumptions_json: str
    model: str
    prompt_version: str


class AggregationSnapshotRow(BaseModel):
    """Schema for fact_aggregation_snapshot.csv rows."""

    run_id: str
    portfolio_id: str
    as_of_date: str
    taxonomy_type: str
    taxonomy_node_id: str
    total_exposure_value_usd: float
    total_exposure_p10: Optional[float] = None
    total_exposure_p90: Optional[float] = None
    coverage_pct: float
    confidence_weighted_exposure: float


class EntityResolutionLogRow(BaseModel):
    """Schema for entity_resolution_log.csv rows."""

    reported_holding_id: str
    raw_company_name: str
    matched_company_id: Optional[str] = None
    match_method: Literal["direct", "alias", "unresolved"]
    match_confidence: float


class ReviewQueueItemRow(BaseModel):
    """Schema for fact_review_queue_item.csv rows."""

    queue_item_id: str
    run_id: str
    exposure_id: Optional[str] = None
    reported_holding_id: Optional[str] = None
    company_id: Optional[str] = None
    raw_company_name: Optional[str] = None
    reason: str
    priority: Literal["high", "medium", "low"]
    status: str = "pending"
    created_at: str


class AuditEventRow(BaseModel):
    """Schema for fact_audit_event.csv rows."""

    audit_event_id: str
    run_id: str
    event_time: str
    actor_type: Literal["system", "agent", "human"]
    actor_id: str
    action: str
    entity_type: str
    entity_id: str
    payload_json: str


def validate_dataframe(df: pd.DataFrame, model: type[BaseModel]) -> list[str]:
    """
    Validate each row of a DataFrame against a Pydantic model.

    Args:
        df: DataFrame to validate
        model: Pydantic model class to validate against

    Returns:
        List of error messages (empty if all rows are valid)
    """
    errors: list[str] = []

    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        # Convert NaN values to None for Pydantic
        for key, value in row_dict.items():
            if pd.isna(value):
                row_dict[key] = None

        try:
            model.model_validate(row_dict)
        except ValidationError as e:
            errors.append(f"Row {idx}: {e}")

    print(f"Validated {len(df)} rows against {model.__name__}: {len(errors)} errors")
    return errors
