# LookThrough — Institutional Portfolio Transparency Platform

> Graduate capstone · M.S. Data Science, Pace University 2026

[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-18-61DAFB?logo=react&logoColor=black)](https://react.dev)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Claude](https://img.shields.io/badge/Claude-Haiku%20%2B%20Sonnet-D97706)](https://anthropic.com)

---

## Overview

LookThrough is a full-stack institutional portfolio transparency platform built for the **Northbridge Endowment Fund** — a $713M fund-of-funds portfolio spanning 12 funds and 5,826 underlying holdings. It ingests multi-source fund documents (SEC filings, 13F XML, private fund PDFs), resolves company identities across sources using AI-powered entity resolution, classifies every holding into the GICS taxonomy, and surfaces look-through exposure analytics through a production-grade front-office dashboard and a natural-language AI agent.

The platform solves a real problem: institutional allocators invested in private fund wrappers — BDCs, private credit funds, PE funds — cannot see their true underlying company-level exposure. LookThrough strips away the fund wrapper and delivers consolidated, position-level transparency.

---

## The Problem

When an endowment allocates $50M to a Business Development Company, it owns a proportional share of that BDC's 200+ underlying loans and equity positions — but its portfolio management system shows a single line item: the BDC fund itself. Multiply this across 12 funds and a CIO has no reliable answer to basic questions:

- *What is our actual exposure to the healthcare sector across all funds?*
- *Are we double-counting any portfolio companies across multiple BDC investments?*
- *What percentage of our endowment is in unrated private credit?*

Traditional solutions require manual spreadsheet aggregation — error-prone, stale by the time it's assembled, and impossible to scale.

---

## Solution

LookThrough automates the full look-through workflow:

1. **Multi-source ingestion** — parses SEC BDC 10-K HTML filings (Schedules of Investments), SEC 13F XML, and private fund PDF documents to extract structured holding-level data
2. **Entity resolution** — a five-strategy fuzzy matching pipeline (exact → alias → normalized → token overlap → first-entity) resolves company name variants across sources into a single canonical company record
3. **AI GICS classification** — a two-step Claude pipeline classifies each company into one of 163 GICS sub-industries at ~$1–2 per 1,300 companies (97% token reduction vs. naive single-prompt approach)
4. **LP ownership scaling** — each fund's NAV-based LP ownership percentage is applied to scale gross holdings to the endowment's true economic exposure
5. **Look-through analytics** — consolidated sector, industry, geography, and fund-level exposure dashboards with 8-quarter time-series history

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Total holdings | 5,826 across 12 funds |
| Portfolio AUM | $713M Northbridge Endowment |
| GICS classification coverage | 83.5% |
| Entity resolution rate | 99.9% |
| Historical exposure data | 8 quarters |
| Data sources | SEC BDC 10-K · SEC 13F · PDF fund documents · Synthetic |
| AI classification cost | ~$1–2 per 1,300 companies |

---

## Architecture

```
User
 │
 ▼
React Frontend (Vite + Tailwind)
 │  Front Office: Dashboard · Holdings · GICS Explorer · AI Agent
 │  Operations:  Ingestion · Review Queue · Audit Trail · Pipeline Monitor
 │
 ▼
FastAPI Backend (Python 3.12)
 │  JWT auth (httpOnly cookie) · SQLAlchemy ORM · 15 REST endpoints
 │
 ├──────────────────────┬─────────────────────────┐
 ▼                      ▼                         ▼
PostgreSQL         Claude AI                 SEC EDGAR API
(Gold/Silver/      Haiku: GICS               + pdfplumber
 Bronze tables)    classification            PDF extraction
                   Sonnet: NL agent
                   (tool-use, not RAG)
```

**Medallion data architecture:**

```
Bronze  →  Silver  →  Gold
Raw         Canonical   Exposures
filings     dims/facts  Aggregations
                        Governance
```

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| **Backend** | Python 3.12, FastAPI, SQLAlchemy 2.0, Pydantic v2, pandas |
| **Frontend** | React 18, Vite, Recharts, Tailwind CSS, React Query, React Router v6 |
| **Database** | PostgreSQL 16, SQLAlchemy ORM, native upsert (ON CONFLICT DO UPDATE) |
| **AI / LLM** | Anthropic Claude Haiku (classification), Claude Sonnet (agent), structured JSON output |
| **Document Parsing** | pdfplumber (PDF), BeautifulSoup4 (HTML), lxml (SEC 13F XML) |
| **Auth** | JWT (httpOnly cookie), BCrypt, FastAPI dependency injection |
| **Deployment** | Railway (backend + PostgreSQL), Vercel (frontend) |

---

## Features

### Front Office

- **Exposure Dashboard** — sector, industry, and geography breakdown charts with fund-level drill-down; live stat cards (AUM, holdings count, classification coverage)
- **8-Quarter Trend** — time-series exposure chart showing how sector allocations have evolved across reporting periods
- **Holdings Explorer** — paginated, searchable table across all 5,826 holdings with fund, sector, and classification filters
- **GICS Sector Explorer** — interactive drill-down from 11 GICS sectors → 25 industry groups → 74 industries → 163 sub-industries, with scaled NAV exposure at each level
- **Fund Detail Pages** — LP-ownership-scaled holdings for each underlying fund with classification confidence scores
- **AI Portfolio Agent** — Claude Sonnet answers natural-language questions ("What's our healthcare concentration?") by calling live data tools, not retrieval — answers are always current

### Operations

- **Multi-source Ingestion Pipeline** — 9-step pipeline runner with per-step status, error visibility, and live trigger from the UI
- **Pipeline Monitor** — real-time document ingestion status across all 4 data sources
- **Review Queue** — AI classifications below 70% confidence, unresolvable entities, and large unknown exposures (>$1M) flagged for human review
- **Human-in-the-Loop Classification** — analysts can accept, override, or escalate any AI classification with a full dropdown of valid GICS nodes
- **Audit Trail** — append-only log of every system and human action across all pipeline runs

---

## AI Capabilities

### GICS Classification Pipeline
Two-step approach to minimize token cost while maintaining accuracy:
1. **Step 1 (Haiku)** — given company name + description, predict the GICS sector and industry group (coarse)
2. **Step 2 (Haiku)** — given sector/industry group context, select the specific sub-industry from the valid node list

Result: 97% token reduction vs. passing all 163 sub-industry descriptions in a single prompt. Cost: ~$1–2 per full portfolio run of 1,300 companies. All outputs validated against the canonical GICS node list before write — no hallucinated taxonomy nodes reach the database.

### Natural Language Agent
Claude Sonnet with a registered tool suite — not RAG. The agent calls live database tools (portfolio summary, sector breakdown, fund exposure, top holdings, review queue, GICS mapping, confidence distribution) and constructs answers from structured query results. Supports OpenAI (GPT-4o) and Ollama (local LLMs) as drop-in provider swaps.

### Entity Resolution
Five-strategy cascade with confidence scoring at each level:
1. Exact name match
2. Known alias lookup
3. Normalized name match (punctuation, case, legal suffixes stripped)
4. Token overlap (Jaccard similarity)
5. First-entity fallback with low confidence flag

**99.9% resolution rate** across 5,826 holdings from 4 heterogeneous sources.

---

## Live Demo

[Railway deployment link — add after deploy]

**Demo credentials:** `admin@lookthrough.com` / `admin123`

---

## Local Development

**Prerequisites:** Docker Desktop, Python 3.12, Node.js 18+

```bash
# 1. Start PostgreSQL
docker start lookthrough-db

# First time only:
docker run --name lookthrough-db \
  -e POSTGRES_USER=lookthrough \
  -e POSTGRES_PASSWORD=lookthrough \
  -e POSTGRES_DB=lookthrough \
  -p 5432:5432 -d postgres:16

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Run the full data pipeline
python run_pipeline.py

# With AI classification (requires ANTHROPIC_API_KEY):
python run_pipeline.py --classify --limit 50

# 4. Seed the admin user
python -m src.lookthrough.auth.seed

# 5. Start the backend (Terminal 1)
uvicorn src.lookthrough.api.main:app --reload

# 6. Start the frontend (Terminal 2)
cd frontend && npm install && npm run dev
```

Open **http://localhost:3000** and log in with `admin@lookthrough.com` / `admin123`.

**Required environment variables:**

```bash
DATABASE_URL=postgresql://lookthrough:lookthrough@localhost:5432/lookthrough
ANTHROPIC_API_KEY=your-key-here
SECRET_KEY=your-secret-key-here
```

---

## Project Structure

```
institutional-lookthrough-platform/
├── src/lookthrough/
│   ├── ingestion/          # BDC 10-K parser, 13F XML, PDF, source loader
│   ├── inference/          # Entity resolution, exposure scaling, aggregation
│   ├── ai/                 # GICS classification, GICS mapping (Claude)
│   ├── governance/         # Review queue, audit trail
│   ├── synthetic/          # Synthetic data generator (config-driven)
│   ├── agent/              # NL agent: tool registry, chat, provider routing
│   ├── api/                # FastAPI app, routes, auth
│   └── db/                 # SQLAlchemy models, repository layer, engine
├── frontend/
│   └── src/
│       ├── pages/          # Dashboard, Holdings, GICS, Agent, Ops pages
│       ├── api/            # Axios client, typed request functions
│       └── contexts/       # Auth context, React Query setup
├── data/
│   └── bronze/filings/     # Real SEC 10-K HTML filings (ARCC, MAIN, OBDC)
├── run_pipeline.py         # 9-step pipeline orchestrator
├── railway.json            # Railway deployment config
└── requirements.txt
```

---

## Data Sources

| Source | Type | Holdings | Notes |
|--------|------|----------|-------|
| Ares Capital (ARCC) | SEC 10-K HTML | ~2,100 | Schedule of Investments |
| Main Street Capital (MAIN) | SEC 10-K HTML | ~450 | Schedule of Investments |
| Blue Owl Capital (OBDC) | SEC 10-K HTML | ~1,040 | Schedule of Investments |
| Synthetic funds | Config-driven generator | ~2,200 | 9 funds, configurable |

All BDC filings fetched from SEC EDGAR. HTML Schedule of Investments tables parsed with BeautifulSoup4 with holding-level extraction confidence scoring.

---

## Author

**Kyle Ferry, CFA, CPA**
M.S. Data Science, Pace University — expected 2026

[LinkedIn](https://linkedin.com/in/your-profile) · [Portfolio](https://your-site.com)

---

## License

Private — academic and research use only.
