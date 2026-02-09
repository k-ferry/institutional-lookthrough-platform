from __future__ import annotations

import argparse
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

from anthropic import Anthropic, transform_schema


# ----------------------------
# Schema for structured output
# ----------------------------
class CompanyClassificationOut(BaseModel):
    taxonomy_type: str = Field(..., description="One of: sector, industry, geography")
    node_name: Optional[str] = Field(
        None, description="Must be one of allowed_nodes; null if cannot classify"
    )
    confidence: float = Field(..., description="Float in [0,1]")
    rationale: str = Field(..., description="1-3 sentences")
    assumptions: list[str] = Field(default_factory=list)


@dataclass(frozen=True)
class ClassifierConfig:
    model: str = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5-20250929")
    max_tokens: int = 512
    prompt_version: str = "v1"
    confidence_threshold: float = 0.70  # below this should go to review queue later


def _repo_root() -> Path:
    # src/lookthrough/ai/classify_companies.py -> repo root is 4 parents up
    return Path(__file__).resolve().parents[3]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def classify_one(
    client: Anthropic,
    cfg: ClassifierConfig,
    prompt_text: str,
    taxonomy_type: str,
    allowed_nodes: list[str],
    company_name: str,
    company_country: Optional[str],
    company_description: Optional[str],
) -> CompanyClassificationOut:
    user_payload = {
        "company_name": company_name,
        "company_country": company_country,
        "company_description": company_description,
        "allowed_nodes": allowed_nodes,
    }

    # Structured outputs: force JSON that matches our schema. :contentReference[oaicite:2]{index=2}
    response = client.messages.create(
        model=cfg.model,
        max_tokens=cfg.max_tokens,
        messages=[
            {
                "role": "user",
                "content": prompt_text
                + "\n\n"
                + "taxonomy_type: "
                + taxonomy_type
                + "\n"
                + "Return JSON matching the required schema.\n\n"
                + json.dumps(user_payload, ensure_ascii=False),
            }
        ],
        output_config={
            "format": {
                "type": "json_schema",
                "schema": transform_schema(CompanyClassificationOut),
            }
        },
    )

    raw = response.content[0].text
    try:
        parsed = CompanyClassificationOut.model_validate_json(raw)
    except ValidationError as e:
        # If schema validation fails (should be rare with structured outputs), fallback safely
        return CompanyClassificationOut(
            taxonomy_type=taxonomy_type,
            node_name=None,
            confidence=0.0,
            rationale=f"ValidationError: {str(e)[:200]}",
            assumptions=[],
        )

    # Post-validate
    parsed.confidence = _clamp01(parsed.confidence)
    if parsed.node_name is not None and parsed.node_name not in allowed_nodes:
        # Disallow hallucinated nodes
        parsed.node_name = None
        parsed.confidence = 0.0
        parsed.rationale = "Returned node_name was not in allowed_nodes; set to null."
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20, help="Max companies to classify (V1 safety).")
    parser.add_argument(
        "--taxonomy-type",
        type=str,
        default="industry",
        choices=["sector", "industry", "geography"],
        help="Taxonomy type to classify.",
    )
    args = parser.parse_args()

    cfg = ClassifierConfig()
    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    prompt_path = root / "src" / "lookthrough" / "ai" / "prompts" / "company_classification.md"
    prompt_text = _read_text(prompt_path)

    # Inputs
    companies = _read_csv(silver / "dim_company.csv")
    taxonomy = _read_csv(silver / "dim_taxonomy_node.csv")

    # Build allowed node list
    tax = taxonomy[taxonomy["taxonomy_type"] == args.taxonomy_type].copy()
    allowed_nodes = sorted(tax["node_name"].astype(str).unique().tolist())

    # Pick target companies to classify
    # V1: classify first N companies (later: only Unknowns / only those appearing in exposures)
    companies = companies.head(args.limit).copy()

    # Optional fields
    country_col = "country" if "country" in companies.columns else None
    desc_col = "company_description" if "company_description" in companies.columns else None

    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))  # official SDK usage :contentReference[oaicite:3]{index=3}

    run_id = str(uuid.uuid4())
    out_rows = []

    for _, c in companies.iterrows():
        company_id = str(c["company_id"]) if "company_id" in companies.columns else None
        company_name = str(c["company_name"]) if "company_name" in companies.columns else str(c.get("raw_company_name", ""))

        company_country = str(c[country_col]) if country_col and pd.notna(c[country_col]) else None
        company_desc = str(c[desc_col]) if desc_col and pd.notna(c[desc_col]) else None

        result = classify_one(
            client=client,
            cfg=cfg,
            prompt_text=prompt_text,
            taxonomy_type=args.taxonomy_type,
            allowed_nodes=allowed_nodes,
            company_name=company_name,
            company_country=company_country,
            company_description=company_desc,
        )

        # Map node_name -> taxonomy_node_id
        if result.node_name is None:
            taxonomy_node_id = "00000000-0000-0000-0000-000000000000"
        else:
            match = tax[tax["node_name"].astype(str) == result.node_name]
            taxonomy_node_id = str(match.iloc[0]["taxonomy_node_id"]) if len(match) else "00000000-0000-0000-0000-000000000000"

        out_rows.append(
            {
                "classification_id": str(uuid.uuid4()),
                "run_id": run_id,
                "company_id": company_id,
                "raw_company_name": company_name,
                "taxonomy_type": result.taxonomy_type,
                "taxonomy_node_id": taxonomy_node_id,
                "confidence": result.confidence,
                "rationale": result.rationale,
                "assumptions_json": json.dumps(result.assumptions, ensure_ascii=False),
                "model": cfg.model,
                "prompt_version": cfg.prompt_version,
            }
        )

    out_df = pd.DataFrame(out_rows)
    out_path = gold / "fact_exposure_classification.csv"
    out_df.to_csv(out_path, index=False)

    print("Wrote:", out_path)
    print("Rows:", len(out_df))
    print("run_id:", run_id)
    print("model:", cfg.model)


if __name__ == "__main__":
    main()
