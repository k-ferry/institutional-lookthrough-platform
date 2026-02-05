# V1 Plan — Institutional Look-Through Platform

## Goal (V1)
Deliver a working vertical slice that:
- Generates realistic **synthetic** institutional portfolio + private fund holdings data
- Produces **look-through exposure rollups** by sector and geography
- Reports **coverage %** and **confidence** (no false precision)
- Creates a **review queue** for low-confidence/ambiguous items (Practical DS)
- Is reproducible end-to-end from the repo

## Non-goals (explicitly out of scope for V1)
- Parsing real LP statement PDFs (schema supports it, but no ingestion)
- Full OCR / document layout extraction
- Full production UI (one minimal UI is enough for V1)

---

# Synthetic Data Generator — V1 Spec (Draft)

## Entities to Generate
1. Portfolio
2. Funds (public funds + private funds)
3. Companies (canonical universe)
4. Fund reports (quarterly)
5. Reported holdings (company lists per fund report)

## Required Outputs (files/tables)
- `dim_portfolio`
- `dim_fund`
- `dim_company`
- `meta_taxonomy_version`, `dim_taxonomy_node` (simple taxonomy)
- `fact_fund_report`
- `fact_reported_holding`

## Realism knobs (we will intentionally simulate these)
- Inconsistent company naming (aliases)
- Missing fields (sector/country/value)
- Partial coverage (e.g., only top holdings reported)
- Conflicting classifications (to trigger queue)
- Lag/quarterly reporting cadence

---

# Milestones (checklist)

## M1 — Data generator produces Bronze/Silver inputs
- [ ] Create taxonomy (sector + geography)
- [ ] Generate canonical companies (N=?)
- [ ] Generate funds (private/public mix)
- [ ] Generate quarterly fund reports
- [ ] Generate holdings + imperfections (missingness, aliases, partial coverage)
- [ ] Save outputs to `data/` (CSV or Parquet)

## M2 — Basic pipeline produces Gold exposure rollups
- [ ] Entity resolution (aliases → canonical)
- [ ] Classification mapping to taxonomy (rule-based baseline)
- [ ] Coverage and confidence metrics
- [ ] Produce `fact_aggregation_snapshot`

## M3 — Practical DS workflow artifacts
- [ ] Create review queue items for low-confidence cases
- [ ] Write audit events for system decisions + human overrides (placeholder)

## M4 — Minimal UI (stretch for V1)
- [ ] Streamlit page: Portfolio exposure by sector + geo
- [ ] Streamlit page: Review queue

---

# Decisions / Open Questions
- [ ] How many companies/funds/quarters in V1?
- [ ] What is the minimum taxonomy depth?
- [ ] What are the confidence propagation rules in V1?
