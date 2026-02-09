"""
AI-assisted company classification (V1).

This module provides a governed wrapper around LLM classification.
It does NOT modify canonical entities and does NOT fabricate holdings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass(frozen=True)
class ClassificationResult:
    taxonomy_type: str
    taxonomy_node_name: Optional[str]
    confidence: float
    rationale: str
    assumptions: list[str]


def classify_company_v1(
    company_name: str,
    taxonomy_type: str,
    allowed_nodes: list[str],
) -> ClassificationResult:
    """
    Placeholder implementation.

    V1 behavior:
    - No LLM call yet
    - Always returns null classification with zero confidence
    """
    return ClassificationResult(
        taxonomy_type=taxonomy_type,
        taxonomy_node_name=None,
        confidence=0.0,
        rationale="LLM classification not yet enabled",
        assumptions=[],
    )
