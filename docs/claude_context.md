Don't do anything yet, I'll give you a task next, this is for context: You are working inside my repo: institutional-lookthrough-platform.

First, read these files:
- docs/v1_plan.md
- data-engineering/schema.md
- data-engineering/data_dictionary.md

Goal: implement the V1 synthetic data generator (10 sectors → ~30 industries, 8 funds, 500 companies, 8 quarters) that outputs CSVs to data/silver/.

Constraints:
- Do not change folder structure unless necessary.
- Do not assume file contents beyond what you read.
- Propose a small, safe set of changes, then implement.
- Keep changes “one commit” sized at a time.

## Current State (Updated)

- Synthetic data generator complete (Silver layer)
- Deterministic exposure inference implemented
- Explicit unknown exposure bucket implemented
- Gold aggregation snapshots available
- AI classification scaffold in progress (Claude as first backend)
