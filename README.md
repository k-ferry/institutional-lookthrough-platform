# LookThrough — Institutional Portfolio Transparency Platform

## What Is This?

LookThrough is an institutional portfolio transparency platform built for fund-of-funds managers, family offices, and institutional allocators who need look-through visibility into their private market holdings. It ingests fund-level data — including real filings from Business Development Companies (BDCs) filed with the SEC — parses portfolio holdings, resolves company identities across sources, classifies every holding into a standardized GICS taxonomy using AI, and surfaces the results through a front-office analytics dashboard and a natural-language AI agent. The goal is to give a CIO or portfolio analyst the same quality of exposure analysis on a private credit or private equity portfolio that they already have on their public equities book.

---

## Key Differentiator: Real Private Company Data from SEC EDGAR

Most portfolio transparency tools work with synthetic or demo data. LookThrough ingests actual SEC 10-K filings from BDC managers and parses their Schedules of Investments directly:

| Fund | Filing | Holdings |
|---|---|---|
| Ares Capital Corporation (ARCC) | 10-K 2026 | ~2,100 positions |
| Main Street Capital (MAIN) | 10-K 2025 | ~450 positions |
| Blue Owl Capital (OBDC) | 10-K 2025 | ~1,040 positions |

**Proven pipeline results: 3,589 real holdings across 1,307 unique private companies** extracted directly from SEC filings via HTML table parsing, with extraction confidence scored at the holding level.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE (Python)                       │
│                                                                     │
│  SEC Filings (HTML)  +  Synthetic Generator                         │
│              ↓                                                      │
│  Bronze Layer  →  Silver Layer  →  Gold Layer                       │
│  (raw filings)    (canonical dims    (exposures,                    │
│                    & facts)           aggregations,                 │
│                                       governance)                   │
└─────────────────────────────────────────────────────────────────────┘
                          ↓
         ┌────────────────────────┐
         │  PostgreSQL (Docker)   │
         │  SQLAlchemy ORM        │
         └────────────────────────┘
                 ↓              ↓
┌─────────────────────┐   ┌────────────────────────────────────────┐
│  FastAPI Backend     │   │  React Frontend (Vite + Tailwind)      │
│  JWT Auth (httpOnly) │◄──│  Front Office:  CIO / Analyst          │
│  10 REST endpoints   │   │  Ops:           Data Management        │
└─────────────────────┘   └────────────────────────────────────────┘
                          ↓
              ┌───────────────────────┐
              │  AI Layer             │
              │  Claude / OpenAI /    │
              │  Ollama               │
              │  GICS Classification  │
              │  NL Agent (tools)     │
              └───────────────────────┘
```

### Two-Platform Structure

**Front Office** — for the CIO and investment analyst
- Portfolio overview dashboard: total AUM, holdings count, sector allocation chart, fund breakdown table
- Holdings Explorer: search, filter, and paginate across all 6,054 holdings with fund and sector filters
- AI Portfolio Assistant: ask natural-language questions about exposures, concentrations, and risk; the agent queries live data before answering and cites the tools it used

**Ops** — for the data management and operations team
- Review Queue: AI classifications below the confidence threshold, unresolved entity matches, and large unknown exposure buckets flagged for human review and approval
- Audit Trail: append-only log of every system and human action across all pipeline runs
- Entity Resolution Viewer: inspect how raw company names were matched to canonical company IDs
- Pipeline Run Monitor: step-by-step run history, coverage statistics, and error visibility
- Classification Manager: review, correct, and approve AI-generated GICS industry classifications

---

## Getting Started

### Prerequisites

- Python 3.12
- Node.js 18+
- Docker Desktop

### 1. Start the database

```bash
docker start lookthrough-db
```

> First time only — create the container:
> ```bash
> docker run --name lookthrough-db \
>   -e POSTGRES_PASSWORD=postgres \
>   -e POSTGRES_DB=lookthrough \
>   -p 5432:5432 -d postgres:16
> ```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the full data pipeline

```bash
python run_pipeline.py
```

Populates all Silver and Gold tables from synthetic data and BDC filings. Add `--classify` to run AI classification (requires `ANTHROPIC_API_KEY` environment variable):

```bash
python run_pipeline.py --classify --limit 50
```

### 4. Seed the admin user

```bash
python -m src.lookthrough.auth.seed
```

### 5. Start the API server *(Terminal 1)*

```bash
uvicorn src.lookthrough.api.main:app --reload
```

### 6. Start the frontend *(Terminal 2)*

```bash
cd frontend && npm install && npm run dev
```

### 7. Open the app

Navigate to **http://localhost:3000** and log in with:

```
Email:    admin@lookthrough.com
Password: admin123
```

---

## Data Overview

| Source | Holdings | Companies | Funds |
|---|---|---|---|
| Synthetic (generated) | 2,465 | ~500 | ~8 |
| BDC Filings (real SEC data) | 3,589 | 1,307 | 3 |
| **Total** | **6,054** | **1,804** | **11** |

---

## Pipeline Stages

```
Stage 1   Synthetic Generation     src/lookthrough/synthetic/generate.py
          ↓
Stage 2   BDC Ingestion            src/lookthrough/ingestion/parse_bdc_filing.py
          ↓                        src/lookthrough/ingestion/load_sources.py
Stage 3   Entity Resolution        src/lookthrough/inference/entity_resolution.py
          ↓                        5 strategies: exact → alias → normalized →
          ↓                                       token overlap → first-entity
Stage 4   Exposure Inference       src/lookthrough/inference/exposure.py
          ↓                        equal-weight fund allocation (V1 deterministic)
Stage 5   AI Classification        src/lookthrough/ai/classify_companies.py
          ↓                        Claude → GICS sector/industry taxonomy nodes
Stage 6   Aggregation              src/lookthrough/inference/aggregate.py
          ↓                        sector / industry / geography rollups with
          ↓                        confidence-weighted exposure metrics
Stage 7   Review Queue             src/lookthrough/governance/review_queue.py
          ↓                        flags: low-confidence, unresolved, large-unknown
Stage 8   Audit Trail              src/lookthrough/governance/audit.py
                                   append-only log of all system actions
```

Every stage supports PostgreSQL (default) and CSV fallback via `--csv` flag or `CSV_MODE=1`.

---

## AI Features

| Feature | Module | Description |
|---|---|---|
| **GICS Classification** | `ai/classify_companies.py` | Classifies each company into sector / industry / geography taxonomy nodes via Claude using structured JSON output and anti-hallucination guards (node names validated against allowed list) |
| **GICS Sector Mapping** | `ai/map_to_gics.py` | Maps free-text `reported_sector` descriptions from BDC filings to 8-digit GICS sub-industry codes in batches of 20 |
| **Natural Language Agent** | `agent/chat.py` | Tool-calling agent that queries live portfolio data before answering; supports Claude, OpenAI, and Ollama (switchable) |

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Backend** | Python 3.12, FastAPI, SQLAlchemy 2.0, Pydantic v2, pandas, BeautifulSoup4 |
| **Frontend** | React 18, Vite, Tailwind CSS, React Query (@tanstack/react-query), React Router v6, Recharts, Axios |
| **Database** | PostgreSQL 16 via Docker, SQLAlchemy ORM, PostgreSQL native upsert (ON CONFLICT DO UPDATE) |
| **AI / ML** | Anthropic Claude API, OpenAI API (GPT-4o), Ollama (local LLMs), structured JSON output |
| **Infrastructure** | Docker (database), uvicorn ASGI server, Vite dev proxy for /api and /auth |

---

## Project Status

**Active development — capstone / startup project.**

The full data pipeline, JWT authentication, and core front-office UI (dashboard, holdings explorer, AI agent chat) are complete and wired to live PostgreSQL data. The Ops interface (review queue management, audit trail viewer, pipeline run monitoring) is the current development focus.

See [`DEVLOG.md`](./DEVLOG.md) for detailed session history, architectural decisions, known issues, and the full development roadmap.

---

## License

Private — academic and research use.
