# LookThrough Development Log

## How to Use This File

This file tracks every development session, the decisions made, what was built, and current status.
**Update this file at the end of every session** — record what was done, any decisions made, and what comes next.
This is the single source of truth for development context when resuming work.

---

## Architecture Decisions

Key decisions already made and locked in. Do not change these without recording the reason.

| Decision | What Was Chosen | Why |
|---|---|---|
| **Auth token storage** | JWT in httpOnly cookie (not localStorage) | Security — httpOnly prevents XSS token theft |
| **Password hashing** | `bcrypt` directly (not `passlib`) | Compatibility — `passlib` broke with `bcrypt` 4.x |
| **Data mode** | PostgreSQL default, CSV fallback via `--csv` / `CSV_MODE=1` | All pipeline modules support both; CSV enables offline dev |
| **Dev proxy** | Vite proxies `/api` and `/auth` to `localhost:8000` | Avoids CORS headers in development |
| **Frontend data fetching** | React Query (`@tanstack/react-query`) | Cache keyed by filter params; stale-while-revalidate; no manual state for async data |
| **Styling** | Tailwind CSS + shadcn-style component primitives | Institutional navy/slate color scheme (`primary-*` = navy, `secondary-*` = slate) |
| **DB access pattern** | Repository layer (`db/repository.py`) wraps all SQLAlchemy | Pipeline modules never touch SQLAlchemy directly; swap DB without changing pipeline |
| **AI structured output** | `anthropic.transform_schema()` + Pydantic models | Guarantees JSON schema compliance; prevents hallucinated node names |
| **GICS taxonomy** | Hardcoded in `taxonomy/gics.py` (2023 revision) | No external dependency; deterministic UUIDs from GICS codes |
| **Audit trail** | Append-only (`bulk_insert`, never `delete_all`) | Governance requirement — audit events must not be overwritten |

---

## Completed Work

### Phase 0 — Data Pipeline (pre-frontend)

- [x] Synthetic data generator (`synthetic/generate.py`) driven by `config.yaml`
- [x] BDC filing parser (`ingestion/parse_bdc_filing.py`) — parses ARCC, MAIN, OBDC 10-K HTML files
- [x] Unified data source loader (`ingestion/load_sources.py`) — merges synthetic + BDC into Silver tables
- [x] Entity resolution (`inference/entity_resolution.py`) — 5 strategies with confidence scoring
- [x] Company consolidation — `--consolidate` flag deduplicates companies across sources
- [x] Exposure inference (`inference/exposure.py`) — V1 deterministic equal-weight allocation
- [x] Aggregation (`inference/aggregate.py`) — sector/industry/geography rollups with coverage_pct
- [x] AI company classification (`ai/classify_companies.py`) — Claude + structured JSON + anti-hallucination
- [x] GICS sector mapping (`ai/map_to_gics.py`) — maps reported_sector free text to 8-digit GICS codes
- [x] Review queue generator (`governance/review_queue.py`) — 4 trigger conditions, 3 priority levels
- [x] Audit trail generator (`governance/audit.py`) — 4 event types, append-only
- [x] Full PostgreSQL migration — all modules support `--csv` fallback via `CSV_MODE=1`
- [x] Repository layer (`db/repository.py`) — `get_all`, `upsert_rows`, `bulk_insert`, `delete_all`, `execute_query`
- [x] Pipeline runner (`run_pipeline.py`) — 9 steps, `--classify` flag, `--csv` flag
- [x] GICS taxonomy module (`taxonomy/gics.py`) — full 2023 hierarchy, deterministic UUIDs

### Phase 1 — Authentication

- [x] JWT access token generation and validation
- [x] `/auth/login` — validates credentials, sets httpOnly cookie (24h)
- [x] `/auth/logout` — clears cookie
- [x] `/auth/me` — returns current user from cookie
- [x] `/auth/register` — creates new user
- [x] `bcrypt` password hashing (direct, not passlib)
- [x] `get_current_user` FastAPI dependency for all protected routes
- [x] Admin seed script (`auth/seed.py`) — creates `admin@lookthrough.com / admin123`

### Phase 2 — Frontend Scaffold

- [x] React 18 + Vite project setup
- [x] Tailwind CSS with custom institutional color palette
- [x] React Router v6 with protected routes and redirect-to-login
- [x] React Query provider with 5-minute stale time and 1 retry
- [x] Axios client (`api/client.js`) with `withCredentials: true`
- [x] `AuthContext` — wraps app, checks `/auth/me` on load, exposes `isAuthenticated`
- [x] `AppLayout` — sidebar navigation (Dashboard, Holdings, Funds, AI Agent, Settings)
- [x] `LoginPage` — form, error handling, redirect on success
- [x] Vite proxy config for `/api` and `/auth` → `http://localhost:8000`

### Phase 3 — Dashboard

- [x] `GET /api/dashboard/stats` — total holdings, companies, funds, AUM, data sources
- [x] `GET /api/dashboard/sector-breakdown` — top 10 sectors by holding count
- [x] `GET /api/dashboard/fund-breakdown` — per-fund holding count, AUM, % of portfolio
- [x] `GET /api/dashboard/geography-breakdown` — per-country breakdown (endpoint ready, UI not wired)
- [x] `DashboardPage` — 4 stat cards with skeleton loading states and error banners
- [x] Sector allocation horizontal bar chart (Recharts `BarChart`) with tooltip showing value + %
- [x] Fund breakdown table with AUM, holding count, portfolio % badge

### Phase 4 — AI Agent Chat

- [x] `POST /api/agent/chat` — routes to `agent/chat.py`, returns response + tools_used
- [x] Agent tool registry (`agent/tools.py`) — portfolio summary, sector/fund/geography, top holdings, review queue, company details, GICS data, confidence distributions
- [x] Multi-provider support: Claude (`claude-sonnet-4-20250514`), OpenAI (GPT-4o), Ollama (llama3.1)
- [x] `AgentPage` — full-screen chat UI with starter questions, message bubbles, typing indicator
- [x] Tool names surfaced as chip badges on AI responses ("sources")
- [x] Conversation history sent with each request for context continuity
- [x] `FormattedText` component handles `**bold**` and newlines in agent responses

### Phase 5 — Holdings Explorer

- [x] `GET /api/holdings` — paginated list with search, fund_id, sector, has_value filters
- [x] `GET /api/holdings/filters` — available funds, sectors, has_value_count for dropdowns
- [x] `HoldingsPage` — paginated table (50/page, up to 200) with full pagination controls
- [x] Debounced search (300ms) on company name
- [x] Fund dropdown, sector dropdown, "With Value Only" checkbox
- [x] Color-coded sector pills (11 GICS sector color mappings)
- [x] Columns: Company, Fund, Sector, Country, Reported Value, Date, Source
- [x] Skeleton loading states, empty state, error banner
- [x] React Query cache keyed by all filter params

---

## In Progress

### Phase 6 — Foundation Fixes

Connecting the AI classification and GICS mapping side pipelines into the main PostgreSQL workflow.

- [ ] Connect `ai/classify_companies.py` to read/write PostgreSQL (currently CSV only)
- [ ] Write GICS mappings from `gics_mapping` table back to `dim_company.primary_industry` and `industry_taxonomy_node_id`
- [ ] Add approve/reject/dismiss workflow to `fact_review_queue_item` (status updates beyond "pending")
- [ ] Add API endpoints for review queue CRUD

---

## Roadmap

### Phase 6 — Foundation Fixes *(current)*

- Connect `ai/classify_companies.py` to PostgreSQL (read dim_company, write fact_exposure_classification to DB)
- Write GICS mapping results back to `dim_company.primary_industry` and `industry_taxonomy_node_id`
- Add review queue approve / reject / snooze workflow (PATCH endpoint + status transitions)
- Add review queue API endpoints: `GET /api/review-queue`, `PATCH /api/review-queue/:id`

### Phase 7 — Ops Interface *(Data Science course deliverable)*

- Review Queue UI — table with priority badges, reason tags, approve/reject/snooze actions
- Audit Trail UI — filterable event log with actor, action, entity, timestamp, payload
- Entity Resolution Viewer — show match method, confidence, canonical vs raw name for every holding
- Pipeline Run Monitor — run history, step durations, pass/fail status, coverage stats per run
- Classification Manager — view AI classifications, confidence scores, override incorrect labels

### Phase 8 — Front Office Polish *(Capstone deliverable)*

- Geography exposure page — world map or table view, country-level rollup (requires fixing country data gap first)
- Fund detail page — click-through from dashboard fund table → holdings list, sector breakdown, time series
- Company detail page — click-through from holdings → all positions across funds, classification, resolution history
- GICS drill-down — click a sector → industries → sub-industries → individual holdings
- Export — CSV / Excel download for holdings and exposure breakdowns

### Phase 9 — Production Readiness

- Document upload UI — drag-and-drop 10-K HTML filing → triggers parse + pipeline run
- User management — admin panel to create/deactivate users, assign roles
- Environment config — `.env` file for DB URL, API keys, JWT secret
- Performance — add PostgreSQL indexes on frequently queried columns (fund_id, company_id, as_of_date)
- Caching — cache aggregation snapshots, add ETags to dashboard endpoints

---

## Known Issues / Data Gaps

| Issue | Impact | Fix In |
|---|---|---|
| `classify_companies.py` reads/writes CSV only | AI classifications don't persist to PostgreSQL; not visible in dashboards without extra steps | Phase 6 |
| `map_to_gics.py` writes to `gics_mapping.csv` only | GICS sub-industry data is not written back to `dim_company`; industry drill-down shows blanks | Phase 6 |
| `fact_review_queue_item.status` is always `"pending"` | No approve/reject/dismiss workflow exists; queue grows unbounded with no resolution | Phase 6 |
| `dim_company.primary_country` is NULL for all BDC companies | Geography breakdown shows "Unknown" for ~3,589 real holdings | Phase 7/8 |
| `dim_company.primary_industry` is NULL for BDC companies | Industry-level classification unavailable until AI classification is connected to DB | Phase 6 |
| `/funds` route renders `DashboardPage` | Clicking "Funds" in the nav shows the dashboard — stub, not implemented | Phase 8 |
| `/settings` route renders `DashboardPage` | Clicking "Settings" in the nav shows the dashboard — stub, not implemented | Phase 9 |
| AI classification pipeline disconnected from live DB | Running `python run_pipeline.py --classify` updates CSVs, not PostgreSQL; must re-run pipeline to pick up | Phase 6 |
| `total_exposure_p10` / `p90` are always null | Confidence intervals not computed in V1 aggregation | Future |
| `fact_aggregation_snapshot` not used by any API | Computed every run but no endpoint queries it; dashboards use raw holding queries instead | Phase 8 |

---

## Session Log

### 2026-02-27

**What was built:**
- Phase 1 (Auth): JWT login/logout/me, httpOnly cookie, bcrypt, admin seed script
- Phase 2 (Frontend scaffold): React 18 + Vite, Tailwind, React Router, React Query, Axios, AuthContext, AppLayout, LoginPage
- Phase 3 (Dashboard): 3 dashboard API endpoints, DashboardPage with stat cards, sector bar chart, fund table — all wired to live PostgreSQL
- Phase 4 (AI Agent): POST /api/agent/chat, AgentPage with full chat UI, starter questions, tool chip badges
- Phase 5 (Holdings Explorer): paginated holdings API with filters, HoldingsPage with debounced search, dropdowns, color-coded sector pills

**Key decisions made:** See Architecture Decisions table above.

**Inventory audit completed:** Full repository audit documented — all modules, tables, columns, endpoints, and data gaps catalogued.

### 2026-02-27 — Session 2

**What was built:**
- Phase 6 Fix 1: Connected AI classification and GICS mapping to PostgreSQL — 857 GICS mappings written, 1,089 dim_company rows updated with real sector/industry labels
- Phase 6 Fix 2: Review queue workflow — approve/reject/dismiss endpoints, bulk actions, reviewer notes, resolved_at/resolved_by tracking
- Phase 7: Full Ops Interface — Review Queue UI (73 items, inline and bulk actions), Audit Trail (7,749 events, expandable JSON payloads), Pipeline Monitor (coverage metrics, data quality progress bars, pipeline run history)
- Sidebar redesigned with FRONT OFFICE / OPS sections

**Key numbers after this session:**
- 82.8% classification coverage (will improve after overnight full classification run)
- 99.9% entity resolution rate
- 73 review queue items (16 high priority)
- 7,749 audit events across 2 pipeline runs

**Running overnight:**
- `python run_pipeline.py --classify --limit 9999` — will classify remaining ~1,294 companies

**Next session:** Phase 8 — Front Office polish (geography exposure, fund detail page, company detail page)