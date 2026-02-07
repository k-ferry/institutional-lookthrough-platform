# AI Contract + Governance (V1)

This document defines the governed use of LLMs in the look-through platform.
LLMs may assist with classification and explanation, but must not fabricate holdings.

---

## Scope of AI in V1

### Allowed (V1)
1. **Company classification** into taxonomy nodes (sector/industry, geography if needed)
2. **Rationales** explaining why a classification was chosen
3. **Insight narration** based only on computed aggregates (no new facts)

### Not allowed (V1)
- Inventing holdings, values, or weights
- Modifying canonical IDs (portfolio_id, fund_id, company_id)
- Writing directly to Gold outputs without a deterministic wrapper
- Taking actions without emitting an audit event

---

## Inputs to LLM

For each company to classify:
- company_name (canonical)
- optional: short description (synthetic or public)
- optional: website domain
- taxonomy version metadata (list of valid sector/industry nodes)

---

## Outputs from LLM (required)

The LLM must return a strict JSON object:

- taxonomy_type: "sector" | "industry" | "geography"
- node_name: string (must match a node in dim_taxonomy_node for the active taxonomy_version)
- confidence: float in [0,1]
- rationale: short text (1–3 sentences)
- assumptions: list of strings

If node_name is not found, return:
- node_name: null
- confidence: 0
- rationale explaining why
- assumptions list

---

## Decision Wrapping Rules

LLM output is **advisory**.
A deterministic wrapper must:
- validate node_name exists in dim_taxonomy_node for the active taxonomy_version
- clamp confidence to [0,1]
- write results to fact_exposure_classification
- create a review queue item when confidence < threshold or node_name is null

---

## Review Hooks (V1)

Create a `fact_review_queue_item` when:
- confidence < 0.70
- conflicting classifications exist for same company across sources
- missing critical attributes (e.g., no country and no sector)

---

## Audit Logging (V1)

Every LLM call must append an audit event capturing:
- model identifier
- prompt version
- inputs (hashed or summarized)
- raw output JSON
- validated output
- confidence
- who/what triggered it (system vs human)

---

## Exposure Inference Toggles (Playbook Config)

These are inference settings that affect how exposures are computed and normalized.

### 1) Scale to NAV
**toggle:** scale_exposure_to_nav: true/false

Purpose:
- If a fund is net long/short or uses leverage, raw gross exposure may not tie to NAV.
- This toggle scales exposures to ensure totals reconcile to NAV (or portfolio value) for comparability.

V1 behavior:
- When true, normalize fund-level exposures so sum(weights) == 1.0 (or sum(value) == NAV proxy)
- When false, allow gross exposure sums to differ from 1.0

### 2) Include/Exclude non-investment balance sheet items
**toggle:** include_non_investment_bs_items: true/false

Purpose:
- Some fund statements include operational assets/liabilities that may distort "investment exposure".

V1 behavior:
- Default false.
- When true, allow additional synthetic line items and include them in reconciliation/coverage metrics,
  but keep them separately labeled as non-investment.


### 3) Substitute public market proxy (option to lever)
**toggle:** use_public_market_proxy: true/false

Purpose:
- When data is unreliable or unavailable substitute with public market proxy.

V1 behavior:
- Default false.
- When true, allow an ETF selection and include them in reconciliation/coverage metrics,
  but keep them separately labeled as proxy exposures.

---



## Prompt Versioning

Prompts are versioned as text files in:
- src/lookthrough/ai/prompts/

Any change to a prompt increments prompt version and must be recorded in audit events.
