"""
Map BDC reported_sector descriptions to GICS (Global Industry Classification Standard).

This module uses Claude to intelligently map free-text sector/industry descriptions
from BDC filings to standardized GICS sub-industry codes.

Usage:
    python -m src.lookthrough.ai.map_to_gics
    python -m src.lookthrough.ai.map_to_gics --limit 50
    python -m src.lookthrough.ai.map_to_gics --batch-size 10
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

from anthropic import Anthropic, transform_schema

from src.lookthrough.taxonomy.gics import get_sub_industry_lookup, get_gics_taxonomy


# ----------------------------
# Schema for structured output
# ----------------------------
class GICSMappingItem(BaseModel):
    """Single mapping from reported_sector to GICS sub_industry."""

    reported_sector: str = Field(..., description="The original reported_sector description")
    gics_sub_industry_code: str = Field(
        ...,
        description="8-digit GICS sub-industry code (e.g., '45102010' for Application Software)",
    )
    confidence: float = Field(
        ...,
        description="Confidence score from 0.0 to 1.0",
    )
    rationale: str = Field(
        ...,
        description="Brief explanation of why this GICS mapping was chosen (1-2 sentences)",
    )


class GICSMappingBatch(BaseModel):
    """Batch response containing multiple GICS mappings."""

    mappings: list[GICSMappingItem] = Field(
        ...,
        description="List of mappings from reported_sector to GICS sub_industry",
    )


@dataclass(frozen=True)
class MapperConfig:
    model: str = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5-20250929")
    max_tokens: int = 4096
    batch_size: int = 20  # Number of descriptions to map per API call


def _repo_root() -> Path:
    # src/lookthrough/ai/map_to_gics.py -> repo root is 4 parents up
    return Path(__file__).resolve().parents[3]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _build_gics_reference() -> str:
    """Build a compact reference of all GICS sub-industries for the prompt."""
    taxonomy = get_gics_taxonomy()
    lines = []

    # Group by sector for readability
    sectors = [n for n in taxonomy if n["level"] == "sector"]
    for sector in sectors:
        lines.append(f"\n## {sector['code']} - {sector['name']}")

        # Get sub-industries under this sector
        sub_industries = [
            n for n in taxonomy
            if n["level"] == "sub_industry" and n["code"].startswith(sector["code"])
        ]
        for si in sub_industries:
            lines.append(f"  {si['code']}: {si['name']}")

    return "\n".join(lines)


def _build_prompt(descriptions: list[str], gics_reference: str) -> str:
    """Build the prompt for mapping descriptions to GICS."""
    descriptions_json = json.dumps(descriptions, ensure_ascii=False, indent=2)

    return f"""You are an expert financial analyst specializing in industry classification.

Your task is to map company/investment descriptions to the GICS (Global Industry Classification Standard) taxonomy.

## GICS Sub-Industry Reference
{gics_reference}

## Instructions

For each description below, determine the most appropriate GICS sub-industry code (8-digit code).

Consider:
1. The primary business activity described
2. The end market or customer segment
3. The products or services mentioned
4. Industry-specific terminology

If a description is ambiguous or could fit multiple categories:
- Choose the most specific applicable sub-industry
- If it mentions software, prefer IT sub-industries (45xxxxxx) unless clearly healthcare/financial software
- If it mentions services, consider whether it's B2B professional services vs consumer services
- For holding companies or diversified businesses, use Industrial Conglomerates (20105010) or Multi-Sector Holdings (40201020)

If a description is invalid (e.g., just numbers, interest rates, or gibberish), map to the most generic applicable category with low confidence.

## Descriptions to Map
{descriptions_json}

Return a JSON object with a "mappings" array containing one entry per description, in the same order as the input.
Each mapping must include:
- reported_sector: The original description (exact match)
- gics_sub_industry_code: The 8-digit GICS code
- confidence: Float 0.0-1.0 (use lower values for ambiguous mappings)
- rationale: Brief explanation"""


def map_batch(
    client: Anthropic,
    cfg: MapperConfig,
    descriptions: list[str],
    gics_reference: str,
    valid_codes: set[str],
) -> list[dict]:
    """Map a batch of descriptions to GICS using Claude."""
    prompt = _build_prompt(descriptions, gics_reference)

    response = client.messages.create(
        model=cfg.model,
        max_tokens=cfg.max_tokens,
        messages=[{"role": "user", "content": prompt}],
        output_config={
            "format": {
                "type": "json_schema",
                "schema": transform_schema(GICSMappingBatch),
            }
        },
    )

    raw = response.content[0].text
    try:
        parsed = GICSMappingBatch.model_validate_json(raw)
    except ValidationError as e:
        # Return error mappings for all descriptions
        return [
            {
                "reported_sector": desc,
                "gics_sub_industry_code": "",
                "confidence": 0.0,
                "rationale": f"ValidationError: {str(e)[:200]}",
            }
            for desc in descriptions
        ]

    # Post-validate and enrich with full hierarchy
    sub_industry_lookup = get_sub_industry_lookup()
    results = []

    for mapping in parsed.mappings:
        code = mapping.gics_sub_industry_code
        confidence = _clamp01(mapping.confidence)

        # Validate code exists
        if code not in valid_codes:
            # Try to find closest match or mark as invalid
            results.append({
                "reported_sector": mapping.reported_sector,
                "gics_sector_code": "",
                "gics_sector_name": "",
                "gics_industry_group_code": "",
                "gics_industry_group_name": "",
                "gics_industry_code": "",
                "gics_industry_name": "",
                "gics_sub_industry_code": code,
                "gics_sub_industry_name": "",
                "confidence": 0.0,
                "rationale": f"Invalid GICS code returned: {code}",
            })
            continue

        # Enrich with full hierarchy
        hierarchy = sub_industry_lookup[code]
        results.append({
            "reported_sector": mapping.reported_sector,
            **hierarchy,
            "confidence": confidence,
            "rationale": mapping.rationale,
        })

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Map BDC reported_sector descriptions to GICS codes using Claude."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of unique descriptions to map (default: all)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Number of descriptions per API call (default: 20)",
    )
    args = parser.parse_args()

    cfg = MapperConfig(batch_size=args.batch_size)
    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    # Load holdings to get unique reported_sector values
    holdings_path = silver / "fact_reported_holding.csv"
    holdings = _read_csv(holdings_path)

    # Get unique non-null descriptions
    unique_sectors = (
        holdings["reported_sector"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    # Filter out empty strings
    unique_sectors = [s for s in unique_sectors if s and s.lower() != "nan"]
    print(f"Found {len(unique_sectors)} unique reported_sector values")

    # Load existing mappings to skip already-mapped descriptions
    out_path = gold / "gics_mapping.csv"
    already_mapped: set[str] = set()
    existing_df = None

    if out_path.exists():
        existing_df = pd.read_csv(out_path)
        already_mapped = set(existing_df["reported_sector"].astype(str).tolist())
        print(f"Loaded {len(existing_df)} existing mappings from {out_path}")

    # Filter to only unmapped descriptions
    to_map = [s for s in unique_sectors if s not in already_mapped]
    print(f"Descriptions to map: {len(to_map)}")

    if args.limit:
        to_map = to_map[: args.limit]
        print(f"Limited to: {len(to_map)}")

    if not to_map:
        print("No new descriptions to map.")
        return

    # Build GICS reference and valid codes set
    gics_reference = _build_gics_reference()
    taxonomy = get_gics_taxonomy()
    valid_codes = {n["code"] for n in taxonomy if n["level"] == "sub_industry"}

    client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    all_results = []
    total_batches = (len(to_map) + cfg.batch_size - 1) // cfg.batch_size

    for i in range(0, len(to_map), cfg.batch_size):
        batch = to_map[i : i + cfg.batch_size]
        batch_num = i // cfg.batch_size + 1
        print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} descriptions)...")

        results = map_batch(client, cfg, batch, gics_reference, valid_codes)
        all_results.extend(results)

    # Create output DataFrame
    new_df = pd.DataFrame(all_results)

    # Combine with existing data if present
    if existing_df is not None:
        # Ensure column order matches
        cols = [
            "reported_sector",
            "gics_sector_code",
            "gics_sector_name",
            "gics_industry_group_code",
            "gics_industry_group_name",
            "gics_industry_code",
            "gics_industry_name",
            "gics_sub_industry_code",
            "gics_sub_industry_name",
            "confidence",
            "rationale",
        ]
        for col in cols:
            if col not in existing_df.columns:
                existing_df[col] = ""
        existing_df = existing_df[cols]
        new_df = new_df[cols]
        out_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        out_df = new_df

    out_df.to_csv(out_path, index=False)

    # Print summary
    print(f"\nWrote: {out_path}")
    print(f"New mappings: {len(new_df)}")
    print(f"Total mappings: {len(out_df)}")
    print(f"Model: {cfg.model}")

    # Show sample of new mappings
    if len(new_df) > 0:
        print("\nSample mappings:")
        sample = new_df.head(5)
        for _, row in sample.iterrows():
            print(f"  '{row['reported_sector'][:50]}...' -> {row['gics_sub_industry_code']} ({row['gics_sub_industry_name']}) [{row['confidence']:.2f}]")


if __name__ == "__main__":
    main()
