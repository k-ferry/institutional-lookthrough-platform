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

from src.lookthrough.db.repository import (
    _is_csv_mode,
    get_all,
    upsert_rows,
)
from src.lookthrough.db.models import (
    DimCompany,
    DimTaxonomyNode,
    FactExposureClassification,
)


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

    # Structured outputs: force JSON that matches our schema.
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
    parser.add_argument("--csv", action="store_true", help="Use CSV mode instead of PostgreSQL")
    args = parser.parse_args()

    csv_mode = args.csv or _is_csv_mode()

    cfg = ClassifierConfig()
    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    prompt_path = root / "src" / "lookthrough" / "ai" / "prompts" / "company_classification.md"
    prompt_text = _read_text(prompt_path)

    # -----------------------------------------------------------------------
    # Load inputs
    # -----------------------------------------------------------------------
    if csv_mode:
        companies = _read_csv(silver / "dim_company.csv")
        taxonomy = _read_csv(silver / "dim_taxonomy_node.csv")
    else:
        companies = get_all(DimCompany)
        taxonomy = get_all(DimTaxonomyNode)

        # In PostgreSQL mode, only classify companies that are missing the
        # relevant taxonomy node ID — avoids re-classifying already-known companies.
        if args.taxonomy_type == "industry":
            unclassified_mask = (
                companies["industry_taxonomy_node_id"].isna()
                | (companies["industry_taxonomy_node_id"].astype(str) == "")
            )
        elif args.taxonomy_type == "geography":
            unclassified_mask = (
                companies["country_taxonomy_node_id"].isna()
                | (companies["country_taxonomy_node_id"].astype(str) == "")
            )
        else:  # sector
            unclassified_mask = (
                companies["primary_sector"].isna()
                | (companies["primary_sector"].astype(str).str.strip() == "")
            )

        companies = companies[unclassified_mask].copy()
        print(f"Found {len(companies)} unclassified companies (taxonomy_type={args.taxonomy_type})")

    # Build allowed node list for this taxonomy type
    tax = taxonomy[taxonomy["taxonomy_type"] == args.taxonomy_type].copy()
    allowed_nodes = sorted(tax["node_name"].astype(str).unique().tolist())

    # Cap at limit
    companies = companies.head(args.limit).copy()

    # -----------------------------------------------------------------------
    # Load existing classifications to skip already-classified companies
    # -----------------------------------------------------------------------
    already_classified: set[tuple[str, str]] = set()

    if csv_mode:
        out_path = gold / "fact_exposure_classification.csv"
        existing_df: Optional[pd.DataFrame] = None
        if out_path.exists():
            existing_df = pd.read_csv(out_path)
            for _, row in existing_df.iterrows():
                already_classified.add((str(row["company_id"]), str(row["taxonomy_type"])))
            print(f"Loaded {len(existing_df)} existing classifications from {out_path}")
    else:
        existing_classifications = get_all(FactExposureClassification)
        if not existing_classifications.empty:
            for _, row in existing_classifications.iterrows():
                already_classified.add((str(row["company_id"]), str(row["taxonomy_type"])))
            print(f"Loaded {len(existing_classifications)} existing classifications from PostgreSQL")
        existing_df = None  # not used in DB mode

    # Optional fields present in some tables
    country_col = "primary_country" if "primary_country" in companies.columns else None
    desc_col = "company_description" if "company_description" in companies.columns else None

    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    run_id = str(uuid.uuid4())
    out_rows = []
    skipped = 0

    # -----------------------------------------------------------------------
    # Classification loop
    # -----------------------------------------------------------------------
    for _, c in companies.iterrows():
        company_id = str(c["company_id"]) if "company_id" in companies.columns else None
        company_name = (
            str(c["company_name"]) if "company_name" in companies.columns
            else str(c.get("raw_company_name", ""))
        )

        # Skip if already classified for this taxonomy_type
        if (company_id, args.taxonomy_type) in already_classified:
            skipped += 1
            continue

        company_country = (
            str(c[country_col]) if country_col and pd.notna(c[country_col]) else None
        )
        company_desc = (
            str(c[desc_col]) if desc_col and pd.notna(c[desc_col]) else None
        )

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
            taxonomy_node_id = (
                str(match.iloc[0]["taxonomy_node_id"])
                if len(match)
                else "00000000-0000-0000-0000-000000000000"
            )

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

    new_df = pd.DataFrame(out_rows)

    # -----------------------------------------------------------------------
    # Write output
    # -----------------------------------------------------------------------
    if csv_mode:
        # Combine with existing CSV data if present
        if existing_df is not None:
            out_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            out_df = new_df
        out_df.to_csv(out_path, index=False)
        print(f"Wrote: {out_path}")
    else:
        # Write classification results to PostgreSQL
        if out_rows:
            upsert_rows(FactExposureClassification, out_rows, ["classification_id"])
            print(f"Wrote {len(out_rows)} classifications to PostgreSQL:fact_exposure_classification")
        else:
            print("No new classifications to write.")

        # Update dim_company with sector/industry/node_id from classification results.
        # Only applies to industry-type classifications (those are the ones that fill
        # industry_taxonomy_node_id + primary_sector + primary_industry on the company row).
        if out_rows and args.taxonomy_type == "industry":
            _update_dim_company_from_classifications(
                out_rows=out_rows,
                tax=tax,
                taxonomy=taxonomy,
            )

    print(f"Skipped (already classified): {skipped}")
    print(f"New rows: {len(new_df)}")
    print(f"run_id: {run_id}")
    print(f"model: {cfg.model}")


def _update_dim_company_from_classifications(
    out_rows: list[dict],
    tax: pd.DataFrame,
    taxonomy: pd.DataFrame,
) -> None:
    """Update dim_company.primary_sector, primary_industry, and industry_taxonomy_node_id
    for every company that received an industry classification this run.

    Loads the full current dim_company records so that all other columns are preserved
    through the upsert (upsert_rows updates every non-key column).
    """
    # Build lookup: company_id -> taxonomy_node_id for this run's results
    classification_map: dict[str, str] = {}
    for row in out_rows:
        node_id = row["taxonomy_node_id"]
        if node_id == "00000000-0000-0000-0000-000000000000":
            continue  # unclassifiable — don't overwrite anything
        classification_map[str(row["company_id"])] = node_id

    if not classification_map:
        print("No classifiable results to write back to dim_company.")
        return

    # Load full company records so we can do a complete upsert without losing other fields
    all_companies = get_all(DimCompany)
    if all_companies.empty:
        return

    # Build taxonomy lookup: node_id -> node row
    tax_by_id = {str(row["taxonomy_node_id"]): row for _, row in taxonomy.iterrows()}

    company_update_records = []

    for _, company_row in all_companies.iterrows():
        cid = str(company_row["company_id"])
        if cid not in classification_map:
            continue

        node_id = classification_map[cid]
        node = tax_by_id.get(node_id)
        if node is None:
            continue

        # Resolve the parent sector node (level 1 parent of the industry node)
        parent_id = node.get("parent_node_id")
        sector_name: Optional[str] = None
        if parent_id and not pd.isna(parent_id):
            parent_node = tax_by_id.get(str(parent_id))
            if parent_node is not None:
                sector_name = str(parent_node["node_name"])

        # Build a full record — start from the existing company row and patch the three fields
        record = company_row.to_dict()
        record["primary_sector"] = sector_name
        record["primary_industry"] = str(node["node_name"])
        record["industry_taxonomy_node_id"] = node_id
        company_update_records.append(record)

    if company_update_records:
        upsert_rows(DimCompany, company_update_records, ["company_id"])
        print(
            f"Updated {len(company_update_records)} dim_company rows "
            f"(primary_sector, primary_industry, industry_taxonomy_node_id)"
        )
    else:
        print("No dim_company rows needed updating.")


if __name__ == "__main__":
    main()
