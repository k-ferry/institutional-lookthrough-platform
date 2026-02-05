# Synthetic Data Generator (V1)

This module generates public + synthetic institutional portfolio data to power the look-through exposure platform.

Outputs (CSV) are written to:
- `data/silver/`

V1 characteristics:
- 8 funds, 500 companies, 8 quarters
- Realistic imperfections: alias names, missing fields, partial coverage, conflicting labels
- Designed to support downstream inference, confidence scoring, and review queue routing
