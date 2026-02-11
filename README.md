# Institutional Look-Through Exposure Platform

An AI-native exposure intelligence layer that helps institutional investors reason about what they own under incomplete, delayed, and inconsistent data.

## What This Is

This platform performs look-through exposure analysis across multi-asset portfolios containing public and private funds. It classifies underlying holdings by sector, industry, and geography — explicitly surfacing coverage gaps, confidence levels, and items requiring human review.

## Course Coverage

This monorepo serves as the deliverable for two graduate courses:

| Course | Focus | Key Modules |
|--------|-------|-------------|
| Capstone Project | Exposure inference, AI classification, aggregation, dashboard | inference/, ai/, aggregation Gold tables |
| Practical Data Science | Data quality, review workflows, audit, governance | governance/, review queue, audit events |

## Core Design Principles

- Explicit uncertainty — every exposure has a confidence score; unknowns are surfaced, not hidden
- Human-in-the-loop — ambiguous classifications route to a review queue
- Auditability — every AI decision has a rationale and is logged
- AI as differentiator — used for classification, confidence scoring, and natural language insights; never used to hallucinate data

## Pipeline Overview

Synthetic Generator -> Silver Tables -> Exposure Inference -> AI Classification -> Aggregation -> Gold Tables -> Review Queue + Audit Events

## How to Run (V1)

1. Install dependencies: pip install -r requirements.txt
2. Generate synthetic data: python -m src.lookthrough.synthetic.generate
3. Run exposure inference: python -m src.lookthrough.inference.exposure
4. Run AI classification (requires ANTHROPIC_API_KEY): python -m src.lookthrough.ai.classify_companies --limit 20
5. Run aggregation: python -m src.lookthrough.inference.aggregate

Outputs are written to data/silver/ (generated inputs) and data/gold/ (inference outputs).

## Tech Stack

- Python — core language
- Pandas / NumPy — data processing
- Anthropic Claude API — AI classification with structured outputs
- Pydantic — schema validation
- Prefect — orchestration (planned)
- Microsoft Fabric — data home (planned)
- Power BI / Streamlit — dashboards (planned)

## Repo Structure

- src/lookthrough/synthetic/ — Synthetic data generator (Silver layer)
- src/lookthrough/inference/ — Exposure inference and aggregation (Gold layer)
- src/lookthrough/ai/ — AI classification with Claude
- src/lookthrough/ai/prompts/ — Versioned prompt templates
- src/lookthrough/governance/ — Review queue and audit events (planned)
- data-engineering/ — Schema docs, ERD, data dictionary
- docs/ — V1 plan, AI contract and governance rules

## Data Model

Follows a Bronze / Silver / Gold pattern. See data-engineering/schema.md for full table definitions.

## License

Private — academic and research use.
