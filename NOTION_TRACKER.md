# LookThrough — Project Tracker

---

## 🏢 Product Overview

**LookThrough** is a full-stack institutional portfolio transparency platform that performs "look-through" analysis on fund-of-fund holdings — decomposing opaque fund positions into their underlying company-level exposures, sector classifications, and geographic breakdowns. It ingests real SEC 10-K filings from Business Development Companies (BDCs) alongside synthetic institutional portfolio data, runs an AI-powered classification pipeline, and surfaces the results through two purpose-built interfaces.

**Who it's for:**
- **Front Office analysts** — need clean, searchable exposure data, sector breakdowns, and an AI agent that can answer portfolio questions in natural language
- **Ops / data teams** — need to review AI classification decisions, manage a priority-ranked review queue, trace every data transformation through an audit log, and monitor pipeline health

**Key differentiator:** The platform ingests real, publicly filed SEC 10-K documents from ARCC, MAIN, and OBDC (three major BDCs representing ~$35B+ in AUM). This is not toy data — the holdings table contains 3,589 real positions parsed from actual regulatory filings. This proves the full ingestion-to-insight pipeline works on production-grade documents.

**Two-platform structure:**
| Interface | Users | Key Pages |
|---|---|---|
| **Front Office** | Analysts, PMs | Dashboard, Holdings Explorer, AI Agent Chat |
| **Ops** | Data analysts, ops team | Review Queue, Audit Trail, Pipeline Monitor |

**Academic context:** Built across two graduate courses simultaneously — the Ops interface (Phase 7) serves a Data Science course deliverable, and the Front Office polish (Phase 8) serves a Capstone deliverable. The architecture and scope are intentionally startup-grade: multi-provider AI, real regulatory data, medallion data architecture, JWT auth, and a full REST API.

---

## 🏗️ Architecture at a Glance

| Layer | Technology | Purpose |
|---|---|---|
| **Backend** | Python 3.12 + FastAPI | REST API, data pipeline runner (9 steps), AI agent orchestration |
| **Frontend** | React 18 + Vite + Tailwind CSS | Dashboard, Holdings, Ops interface, AI chat — all JWT-authenticated |
| **Database** | PostgreSQL 16 (Docker) | Primary datastore; all Silver/Gold tables; CSV fallback via `CSV_MODE=1` |
| **AI / LLM** | Claude (Anthropic), GPT-4o (OpenAI), Llama3.1 (Ollama/local) | Company classification, GICS mapping, agent chat, ops research assistant |
| **Document Ingestion** | Custom HTML parser (`parse_bdc_filing.py`) | Parses SEC 10-K HTML filings into structured holdings data |
| **Auth** | JWT + httpOnly cookie + bcrypt | Stateless auth; httpOnly prevents XSS token theft; 24h expiry |

**Data architecture — Medallion pattern:**
- **Bronze** — raw HTML filing text, unchanged
- **Silver** — canonical dimension tables (`dim_company`, `dim_fund`, `dim_taxonomy_node`) and fact tables (`fact_reported_holding`, `fact_fund_report`)
- **Gold** — computed exposures, aggregations, classifications (`fact_exposure_classification`, `fact_aggregation_snapshot`, `fact_review_queue_item`, `fact_audit_event`)

---

## 📊 Current Data

| Metric | Value |
|---|---|
| **Total holdings** | 6,054 (2,465 synthetic + 3,589 real BDC from SEC filings) |
| **Companies** | 1,804 unique entities after consolidation |
| **Funds** | 11 (8 synthetic + 3 real: ARCC, MAIN, OBDC) |
| **GICS mappings written to DB** | 857 |
| **Entity resolution rate** | 99.9% |
| **Classification coverage** | 82.8% — improving after overnight full classification run (~1,294 companies queued) |
| **Review queue items** | 73 (16 high priority) |
| **Audit events** | 7,749 across 2 pipeline runs |

---

## ✅ Completed Features

### Phase 0 — Data Pipeline

- [x] Synthetic data generator (`synthetic/generate.py`) driven by `config.yaml`
- [x] BDC filing parser (`ingestion/parse_bdc_filing.py`) — parses ARCC, MAIN, OBDC 10-K HTML files
- [x] Unified data loader (`ingestion/load_sources.py`) — merges synthetic + BDC into Silver tables
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
- [x] `/auth/login` — validates credentials, sets httpOnly cookie (24h expiry)
- [x] `/auth/logout` — clears cookie
- [x] `/auth/me` — returns current user from cookie
- [x] `/auth/register` — creates new user
- [x] bcrypt password hashing (direct, not passlib — compatibility fix)
- [x] `get_current_user` FastAPI dependency injected on all protected routes
- [x] Admin seed script (`auth/seed.py`) — creates `admin@lookthrough.com / <see DEVLOG>`

### Phase 2 — Frontend Scaffold

- [x] React 18 + Vite project setup with hot reload
- [x] Tailwind CSS with custom institutional color palette (`primary-*` = navy, `secondary-*` = slate)
- [x] React Router v6 with protected routes and redirect-to-login
- [x] React Query provider with 5-minute stale time and 1 retry
- [x] Axios client (`api/client.js`) with `withCredentials: true`
- [x] `AuthContext` — wraps app, checks `/auth/me` on load, exposes `isAuthenticated`
- [x] `AppLayout` — sidebar navigation split into FRONT OFFICE / OPS sections
- [x] `LoginPage` — form, error handling, redirect on success
- [x] Vite proxy config for `/api` and `/auth` → `http://localhost:8000`

### Phase 3 — Dashboard

- [x] `GET /api/dashboard/stats` — total holdings, companies, funds, AUM, data sources
- [x] `GET /api/dashboard/sector-breakdown` — top 10 sectors by holding count
- [x] `GET /api/dashboard/fund-breakdown` — per-fund holding count, AUM, % of portfolio
- [x] `GET /api/dashboard/geography-breakdown` — per-country breakdown (endpoint ready)
- [x] `DashboardPage` — 4 stat cards with skeleton loading states and error banners
- [x] Sector allocation horizontal bar chart (Recharts) with tooltip showing value + %
- [x] Fund breakdown table with AUM, holding count, portfolio % badge

### Phase 4 — AI Agent Chat

- [x] `POST /api/agent/chat` — routes to `agent/chat.py`, returns response + tools_used list
- [x] Agent tool registry — portfolio summary, sector/fund/geography breakdowns, top holdings, review queue items, company details, GICS data, confidence distributions
- [x] Multi-provider: Claude (`claude-sonnet-4-20250514`), OpenAI (GPT-4o), Ollama (llama3.1)
- [x] `AgentPage` — full-screen chat UI with starter questions, message bubbles, typing indicator
- [x] Tool names surfaced as chip badges on AI responses ("used these data sources")
- [x] Conversation history sent with each request for context continuity
- [x] `FormattedText` component handles `**bold**` and newlines in agent responses

### Phase 5 — Holdings Explorer

- [x] `GET /api/holdings` — paginated list with search, fund_id, sector, has_value filters
- [x] `GET /api/holdings/filters` — available funds, sectors, has_value_count for dropdowns
- [x] `HoldingsPage` — paginated table (50/page, up to 200) with full pagination controls
- [x] Debounced search (300ms) on company name
- [x] Fund dropdown, sector dropdown, "With Value Only" checkbox
- [x] Color-coded sector pills (11 GICS sector color mappings)
- [x] Columns: Company, Fund, Sector, Country, Reported Value, Date, Source badge
- [x] Skeleton loading states, empty state, error banner

### Phase 6 — Foundation Fixes *(largely complete)*

- [x] Connected AI classification pipeline to PostgreSQL — 857 GICS mappings written to DB
- [x] 1,089 `dim_company` rows updated with real sector/industry labels from AI
- [x] Review queue approve/reject/dismiss PATCH endpoint
- [x] Bulk action endpoint (`PATCH /api/review-queue/bulk`)
- [x] Reviewer notes, `resolved_at`, and `resolved_by` tracking on queue items

### Phase 7 — Ops Interface *(Data Science course deliverable)*

- [x] **Review Queue page** — paginated table, priority/status/reason filters, inline approve/reject/dismiss, bulk select + bulk action
- [x] **Review Queue detail panel** — expandable per-row panel with company context, fund badges, holdings value, entity resolution method + confidence, AI classification with rationale
- [x] Zero UUID display fix — shows "Could not classify" in muted red instead of raw `00000000-...` UUID
- [x] **AI Research Assistant** — "Research this Company" panel in the detail panel:
  - LLM selector: Claude (amber), GPT-4o (green), Llama3.1/Local (purple, free badge)
  - `POST /api/review-queue/research` — routes to selected provider, returns response + duration_ms
  - Response panel: provider badge, response time, formatted text, "Try different LLM" link
  - Shorter prompt for Ollama (local model speed optimization); 300s timeout
- [x] **Audit Trail page** — paginated event log, filters by action/entity type/days/entity ID, expandable JSON payload viewer
- [x] **Pipeline Monitor page** — classification coverage, entity resolution rate, holdings with value %, BDC vs synthetic split, pipeline run history table

---

## 🔄 In Progress

### Phase 6 — Remaining Items

| Item | Status |
|---|---|
| Overnight full classification run (`--classify --limit 9999`) | Running — will push coverage from 82.8% toward ~95%+ |
| `dim_company.primary_country` for BDC companies | Still NULL for all 3,589 real holdings — geography breakdown shows "Unknown" |
| `dim_company.primary_industry` fully populated for BDC | Partial — AI classification writing to DB but pipeline hasn't completed for all companies yet |

---

## 🗺️ Roadmap

### Phase 8 — Front Office Polish *(Capstone deliverable — NEXT)*

- Geography exposure page — world map or breakdown table by country (requires fixing `primary_country` NULL gap first)
- Fund detail page — click-through from dashboard fund table → holdings list, sector breakdown, time series
- Company detail page — click-through from holdings → all positions across funds, classification history, entity resolution log
- GICS drill-down — click a sector → industries → sub-industries → individual holdings
- Export — CSV / Excel download for holdings and exposure breakdowns

### Phase 9 — Production Readiness

- Document upload UI — drag-and-drop 10-K HTML filing → triggers parse + full pipeline run
- User management — admin panel to create/deactivate users, assign roles (analyst vs ops)
- Environment config — `.env` file for DB URL, API keys, JWT secret (currently hardcoded for dev)
- Performance — PostgreSQL indexes on `fund_id`, `company_id`, `as_of_date`, `status`
- Caching — cache aggregation snapshots, add ETags to dashboard endpoints

---

## 🐛 Known Issues / Data Gaps

| Issue | Impact | Fix In |
|---|---|---|
| `dim_company.primary_country` is NULL for all BDC companies | Geography breakdown shows "Unknown" for ~3,589 real holdings | Phase 7/8 |
| `dim_company.primary_industry` NULL for BDC companies | Industry-level drill-down unavailable until AI classification run completes | Phase 6 (in progress) |
| `/funds` route renders DashboardPage | Clicking "Funds" in nav shows the dashboard — stub not implemented | Phase 8 |
| `/settings` route renders DashboardPage | Clicking "Settings" in nav shows the dashboard — stub not implemented | Phase 9 |
| `total_exposure_p10` / `p90` always null | Confidence intervals not computed in V1 aggregation | Future |
| `fact_aggregation_snapshot` not used by any API | Computed every pipeline run but no endpoint queries it; dashboards use raw holding queries | Phase 8 |
| Review queue grows unbounded until classified | Items stay "pending" until a human reviewer acts; no auto-dismiss on re-classification | Phase 8 |

---

## 🔑 Key Technical Decisions

| Decision | Choice | Reason |
|---|---|---|
| **Auth token storage** | JWT in httpOnly cookie (not localStorage) | Security — httpOnly prevents XSS token theft |
| **Password hashing** | `bcrypt` directly (not `passlib`) | Compatibility — `passlib` broke with `bcrypt` 4.x |
| **Data mode** | PostgreSQL default, CSV fallback via `--csv` / `CSV_MODE=1` | All pipeline modules support both; CSV enables offline/test dev |
| **Dev proxy** | Vite proxies `/api` and `/auth` to `localhost:8000` | Avoids CORS headers in development; zero config change for prod |
| **Frontend data fetching** | React Query (`@tanstack/react-query`) | Cache keyed by filter params; stale-while-revalidate; no manual async state |
| **Styling** | Tailwind CSS + shadcn-style component primitives | Institutional navy/slate palette; `primary-*` = navy, `secondary-*` = slate |
| **DB access pattern** | Repository layer (`db/repository.py`) wraps all SQLAlchemy | Pipeline modules never touch SQLAlchemy directly; swap DB without touching pipeline |
| **AI structured output** | `anthropic.transform_schema()` + Pydantic models | Guarantees JSON schema compliance; prevents hallucinated taxonomy node names |
| **GICS taxonomy** | Hardcoded in `taxonomy/gics.py` (2023 revision) | No external dependency; deterministic UUIDs from GICS codes; fully reproducible |
| **Audit trail** | Append-only (`bulk_insert`, never `delete_all`) | Governance requirement — audit events must not be overwritten or deleted |
| **LLM research UX** | User selects provider per call (not all three simultaneously) | More efficient, user controls cost/speed tradeoff; Ollama = free + private |
| **Ollama integration** | Local Llama3.1 (4.9GB), shorter prompt, 300s timeout | Free inference; data never leaves machine; prompt shortened for local model speed |

---

## 🚀 How to Run the App

Run these commands in order. Backend and frontend must both be running simultaneously.

```bash
# 1. Start PostgreSQL (Docker container must be running)
docker start lookthrough-db

# 2. Start backend API (Terminal 1 — keep running)
uvicorn src.lookthrough.api.main:app --reload

# 3. Start frontend dev server (Terminal 2 — keep running)
cd frontend && npm run dev

# 4. Open in browser
http://localhost:3000

# 5. Login
# Email:    admin@lookthrough.com
# Password: <see DEVLOG>
```

**To run the full pipeline (re-ingest + re-classify):**
```bash
# Standard run (no AI classification)
python run_pipeline.py

# Full run with AI classification (slow — calls Claude API)
python run_pipeline.py --classify

# Full run with limit (for testing)
python run_pipeline.py --classify --limit 50

# CSV mode (no database required)
python run_pipeline.py --csv
```

---

## 🔐 Credentials & Keys *(Private — do not share)*

| Secret | Value |
|---|---|
| **Database URL** | `postgresql://lookthrough:lookthrough@localhost:5432/lookthrough` |
| **Admin login** | `admin@lookthrough.com` / `<see DEVLOG>` |
| **JWT secret** | Set via `JWT_SECRET_KEY` environment variable |
| **Anthropic API key** | Set via `ANTHROPIC_API_KEY` environment variable |
| **OpenAI API key** | Set via `OPENAI_API_KEY` environment variable |
| **Ollama** | Running locally at `http://localhost:11434` — no API key required |

---

## 📅 Session Log

| Date | What Was Built | Key Decisions | Next Steps |
|---|---|---|---|
| **2026-02-27** | Phase 1 (JWT auth, httpOnly cookie, bcrypt, seed script) · Phase 2 (React 18 + Vite + Tailwind, React Router, React Query, AuthContext, AppLayout) · Phase 3 (Dashboard — 3 API endpoints, stat cards, sector chart, fund table) · Phase 4 (AI Agent — chat endpoint, tool registry, AgentPage with starter questions) · Phase 5 (Holdings Explorer — paginated API, search, filters, sector pills) | JWT in httpOnly cookie; bcrypt direct (passlib broken); Vite proxy avoids CORS; React Query for cache; institutional navy/slate palette | Phase 6 foundation fixes — connect AI to PG, review queue workflow |
| **2026-02-27 Session 2** | Phase 6 Fix 1 (AI classification → PostgreSQL, 857 GICS mappings, 1,089 dim_company rows updated) · Phase 6 Fix 2 (approve/reject/dismiss endpoints, bulk actions, reviewer notes) · Phase 7 (Review Queue UI, Audit Trail UI with expandable JSON, Pipeline Monitor with coverage metrics) · Sidebar redesigned with FRONT OFFICE / OPS sections | Audit trail append-only (governance); bulk actions via separate endpoint before `/{id}` route | Overnight classification run; Phase 8 Front Office polish |
| **2026-03-01 Session 3** | Review Queue detail panel (company context, AI classification, entity resolution) · Zero UUID display fix ("Could not classify" in red) · AI Research Assistant — LLM selector (Claude/GPT-4o/Ollama), `POST /api/review-queue/research`, response panel with provider badge + timing · Ollama/Llama3.1 installed locally (4.9GB) · Ollama timeout 300s, shorter prompt, UX loading message | LLM per-call (user chooses, not all three); Ollama local = free + data stays on machine; shorter prompt for local model speed | Phase 8 — Front Office polish (geography, fund detail, company detail, GICS drill-down) |

---

## 🎯 Presentation Talking Points

### For the Professor / Academic Presentation

- The platform demonstrates the **full data engineering stack**: raw document ingestion → entity resolution → AI classification → relational storage → REST API → interactive UI — built from scratch, no no-code tools
- Real regulatory data from **SEC EDGAR filings** (ARCC, MAIN, OBDC 10-Ks) proves the pipeline handles production-grade messy data, not just clean synthetic inputs
- **Medallion architecture** (Bronze → Silver → Gold) is the industry-standard pattern used at Bloomberg, BlackRock, and major data platforms — intentionally chosen to demonstrate awareness of real-world data engineering conventions
- The **Ops interface** (Review Queue, Audit Trail, Pipeline Monitor) demonstrates understanding of data governance requirements that are often overlooked in academic projects: every classification decision is traceable, every queue item is auditable, coverage metrics are surfaced in real time
- **Multi-provider AI** (Claude, GPT-4o, Ollama) shows understanding of the LLM ecosystem and cost/privacy tradeoffs — Ollama runs locally with no API cost and data never leaves the machine

### For an Investor Pitch

- **Market context:** Institutional fund-of-fund transparency is a multi-billion dollar problem. Allocators to BDCs, hedge funds, and PE funds often have zero look-through into underlying positions — regulators (SEC, FSOC) are increasingly requiring it
- **Real data proof:** Ingesting actual 10-K filings from production BDCs (not synthetic data) proves the parsing and classification pipeline works on the real documents funds actually file — this is the hardest part of the problem
- **AI-native from day one:** Classification, GICS mapping, entity resolution, and the analyst research assistant are all AI-powered. The system gets more accurate as models improve, with no code changes required
- **Two-sided platform:** Front Office serves analysts who need insights; Ops serves the data team that maintains quality. This is the right architecture for a SaaS product — different user personas, different surfaces, shared data layer
- **Moat:** The hardest parts — filing parser, entity resolution across data sources, GICS taxonomy mapping, review queue governance — are all built. The dataset grows every quarter as BDCs file new 10-Ks

### The BDC Data Framing (Proof of Concept, Not Production)

- ARCC (Ares Capital), MAIN (Main Street Capital), and OBDC (Blue Owl Capital) are three of the largest BDCs in the US with combined AUM exceeding $35B
- Their 10-K filings are **public documents** on SEC EDGAR — this is not proprietary data; it is the same data any institutional allocator can download today
- The platform is built to **ingest any BDC 10-K filing** — adding a new fund is a matter of downloading the HTML file; the parser handles the rest
- "Proof of concept" means: the infrastructure works on real data; production would add more funds, real-time filing monitoring via EDGAR RSS, and formal data licensing agreements

### What Makes This Startup-Grade vs. Student Project

| Student Project | This Platform |
|---|---|
| SQLite or CSV files | PostgreSQL with medallion architecture |
| Hardcoded data | Real SEC filings + synthetic data generation |
| Single LLM call | Multi-provider AI with structured output, anti-hallucination, tool-calling agent |
| No auth | JWT + httpOnly cookies + bcrypt + protected routes |
| No data governance | Review queue, audit trail, coverage metrics, append-only audit log |
| Frontend only | Full REST API that could be consumed by any client |
| One user type | Two distinct user personas (Front Office vs Ops) with purpose-built interfaces |
| No error handling | Graceful degradation, skeleton loaders, error banners throughout |
