# LookThrough Technical Reference

*Prepared for presentation — April 29, 2026*

---

## 1. How the AI Agent Chatbot Works

The chatbot on the Agent page lets you ask plain-English questions about the portfolio and get real answers backed by live data.

### What happens when you type a question

1. **You type a question** — for example: *"What is our technology exposure?"*

2. **The question is sent to Claude** (Anthropic's AI model) along with a system prompt that says, in plain terms:
   - You are a portfolio analyst assistant for Northbridge Endowment Fund.
   - Never make up numbers. Always call a tool to get real data first.
   - Use per-fund latest dates so you don't double-count.
   - Report your confidence level in every answer.

3. **Claude decides which tool to call.** It picks from a list of tools like:
   - `get_sector_breakdown` — total exposure by GICS sector
   - `get_top_holdings` — largest individual positions
   - `get_fund_breakdown` — exposure by fund
   - `get_review_queue_items` — flagged items needing human review
   - `get_company_details` — facts about a specific company
   - `get_confidence_distribution` — how confident the AI was when classifying holdings

4. **Python runs the tool.** The tool is just a Python function that queries the PostgreSQL database using SQLAlchemy. The LLM never touches the database directly — it only calls the tool by name and receives a Python dictionary back.

5. **Claude reads the data and writes an answer.** It formats the numbers, adds context, and returns a plain-English response to the UI.

6. **The loop can repeat.** If Claude needs more data to answer fully, it calls another tool before responding.

### The key rule that prevents hallucination

Every query that touches dollar amounts uses what the team calls the *per-fund latest date* pattern. Because different funds report their holdings at different quarterly dates, a naive query would mix stale and current data or double-count. The system always finds the most recent report date *per fund* before summing, ensuring totals are accurate.

---

## 2. Pipeline Step by Step

The pipeline is a sequence of 14 steps that runs whenever new fund data arrives. Each step reads from files or the database and writes cleaned, enriched results back. Think of it as an assembly line where raw filings become actionable intelligence.

### The three layers (Medallion Architecture)

| Layer | Name | What it contains |
|-------|------|-----------------|
| Bronze | Raw | Exactly what came in from the source — HTML filings, CSV exports |
| Silver | Canonical | Cleaned, deduplicated, standardized facts and dimensions |
| Gold | Aggregated | Exposure totals, classifications, governance flags |

### The 14 steps

**Step 1 — Load Sources**
Reads three real-world BDC (Business Development Company) 10-K HTML filings (ARCC, MAIN, OBDC) plus synthetic fund data generated for testing. Normalizes everything into a single holdings table.

**Step 2 — Deduplicate**
Removes duplicate rows that appear in multiple filings. Keeps the most recent version of each holding.

**Step 3 — Resolve Entities**
Matches company names across funds. If "Microsoft Corp" and "Microsoft Corporation" appear in two funds, they get linked to the same canonical company record.

**Step 4 — Map to GICS**
Calls Claude to map company descriptions to GICS (Global Industry Classification Standard) sectors and industries. Processes in batches of 20 to reduce API costs.

**Step 5 — Classify Companies** *(AI step, optional)*
Calls Claude to classify each company into a sector and industry. Uses a two-step approach: first classify the sector, then narrow down the industry (reduces API calls by ~50%). Any classification with confidence below 70% is sent to the review queue.

**Step 6 — Apply Instrument Rules**
Before calling the AI, applies rule-based overrides for known instrument types:
- ETFs → Financials / Exchange-Traded Funds
- Cryptocurrencies → Financials / Digital Assets
- SPACs and warrants → Financials / Capital Markets
This avoids wasting API calls on instruments with obvious classifications.

**Step 7 — Enrich Geography**
Looks up the primary country for each company. Currently uses name-matching heuristics; AI-based geography classification is planned but not yet built.

**Step 8 — Calculate Scaled Exposure**
This is the most important financial calculation in the system. It answers: *"What is Northbridge's actual dollar exposure to each holding?"*

The formula:
```
scaled_value = (holding_value / total_fund_value) × northbridge_nav_in_fund
```

Because Northbridge owns a *slice* of each fund (not the whole fund), you can't just use the raw holding value. You have to scale it down to Northbridge's proportional share.

**Step 9 — Aggregate**
Rolls up scaled exposure by sector, industry, country, and fund type for the dashboard charts.

**Step 10 — Flag Review Queue Items**
Scans for anything that needs human attention:
- AI confidence below 70%
- Companies that couldn't be classified at all
- Entity names that couldn't be resolved
- Individual positions larger than $1 million with unknown classification

**Step 11 — Build Audit Trail**
Records every change made to a company's classification — who changed it, when, what it was before, what it is now. Supports regulatory compliance.

**Step 12 — Snapshot**
Takes a point-in-time snapshot of aggregated exposures. Used for trend charts showing how the portfolio has changed over time.

**Step 13 — Validate**
Runs data quality checks. Flags if totals don't reconcile, if required fields are missing, or if percentages don't add up to 100%.

**Step 14 — Report**
Writes a summary report of the pipeline run — how many records were processed, how many were flagged, how many classifications succeeded or failed.

---

## 3. Technology Stack

| Category | Technology | What it does |
|----------|-----------|-------------|
| **Database** | PostgreSQL | Primary data store for all holdings, companies, funds, and aggregations |
| **Backend language** | Python 3.11 | Pipeline processing, API server, AI integration |
| **API framework** | FastAPI | REST API that the frontend calls for all data |
| **Database ORM** | SQLAlchemy | Python library that translates Python code into SQL queries |
| **Authentication** | JWT (JSON Web Tokens) + BCrypt | Secure login; passwords hashed before storage; session token stored as httponly cookie |
| **AI model** | Claude (Anthropic) | Company classification, GICS mapping, agent chatbot |
| **AI model (alt)** | GPT-4o (OpenAI) | Alternative chatbot model, switchable |
| **AI model (alt)** | Llama 3.1 via Ollama | Local/offline alternative chatbot model |
| **Frontend framework** | React 18 | All UI components and pages |
| **Frontend build tool** | Vite | Fast development server and production bundler |
| **CSS framework** | Tailwind CSS | Utility-first styling |
| **Charts** | Recharts | All charts (bar, pie/donut, line/area) |
| **HTTP client** | React Query (TanStack Query) | Data fetching, caching, and loading states in the frontend |
| **Icons** | Lucide React | Icon library used throughout the UI |
| **Dev environment** | Windows 11 + WSL optional | Local development |
| **Data fallback** | CSV mode (`CSV_MODE=1`) | Every pipeline step can run without a database using CSV files |

---

## 4. File Structure

```
institutional-lookthrough-platform/
│
├── run_pipeline.py                  Main pipeline runner — executes all 14 steps
├── TECHNICAL_REFERENCE.md           This document
│
├── data/
│   ├── bronze/filings/              Raw HTML 10-K filings (ARCC, MAIN, OBDC)
│   ├── silver/                      Cleaned CSV outputs (used in CSV mode)
│   └── gold/                        Aggregated CSV outputs (used in CSV mode)
│
├── src/lookthrough/
│   │
│   ├── api/
│   │   ├── main.py                  FastAPI app — registers all routes, CORS, auth middleware
│   │   └── routes/
│   │       ├── auth.py              Login, register, logout, /me
│   │       ├── dashboard.py         Stats, sector/fund/geography/industry/country breakdowns, trend
│   │       ├── holdings.py          Paginated holdings table with search and filters
│   │       ├── gics.py              GICS Sector Explorer — drill into a sector's holdings
│   │       └── agent.py             AI chatbot endpoint — runs the tool-calling loop
│   │
│   ├── db/
│   │   ├── models.py                SQLAlchemy table definitions (all Silver and Gold tables)
│   │   └── repository.py           Database query functions used by the API
│   │
│   ├── auth/                        JWT creation, validation, password hashing
│   │
│   ├── ingestion/
│   │   ├── load_sources.py          Step 1 — ingest BDC filings + synthetic data
│   │   ├── deduplicate.py           Step 2 — remove duplicate holdings
│   │   └── resolve_entities.py     Step 3 — match company names across funds
│   │
│   ├── inference/
│   │   ├── map_to_gics.py           Step 4 — GICS sector/industry mapping via Claude
│   │   ├── classify_companies.py   Step 5 — AI classification with confidence scoring
│   │   └── instrument_rules.py     Step 6 — rule-based overrides for ETFs, crypto, SPACs
│   │
│   ├── governance/
│   │   ├── review_queue.py          Step 10 — flag items for human review
│   │   └── audit_trail.py          Step 11 — record classification change history
│   │
│   ├── ai/
│   │   └── agent/
│   │       ├── agent.py             Chatbot loop — sends messages to Claude, handles tool calls
│   │       └── tools.py             TOOLS_REGISTRY — all 8 tool definitions + query functions
│   │
│   └── synthetic/
│       ├── generate.py              Generates realistic fake fund data for testing
│       └── config.yaml              Configuration for synthetic data generation
│
└── frontend/
    ├── src/
    │   ├── App.jsx                  React Router routes (/dashboard, /holdings, /agent, etc.)
    │   ├── main.jsx                 React app entry point
    │   │
    │   ├── pages/
    │   │   ├── DashboardPage.jsx    Overview: stat cards, sector chart, industry/country charts, fund lineup
    │   │   ├── HoldingsPage.jsx     Paginated holdings table with search and filters
    │   │   ├── FundsPage.jsx        Fund allocation donut chart + fund cards
    │   │   ├── AgentPage.jsx        AI chatbot interface
    │   │   └── ops/
    │   │       └── ReviewQueuePage.jsx   Human review queue with search and sortable columns
    │   │
    │   ├── components/
    │   │   ├── layout/              Sidebar, TopBar, Layout wrapper
    │   │   └── ui/                  Reusable UI components (Card, Badge, Button, etc.)
    │   │
    │   └── lib/
    │       └── api.js               Axios instance — all API calls go through here
    │
    ├── index.html
    ├── vite.config.js
    └── tailwind.config.js
```

---

## 5. Key Numbers for Presentation

| Metric | Value | Notes |
|--------|-------|-------|
| Real fund filings ingested | 3 | ARCC, MAIN, OBDC — public BDC 10-K filings |
| AI confidence threshold | 70% | Below this → sent to review queue for human review |
| Classification batch size | 20 | Companies sent to Claude per API call for GICS mapping |
| API cost reduction | ~50% | From two-step classification (sector first, then industry) |
| Pipeline steps | 14 | From raw HTML filing to dashboard-ready aggregations |
| Data layers | 3 | Bronze (raw) → Silver (clean) → Gold (aggregated) |
| Review queue items (current) | ~247 | Items pending human classification review |
| Country classification coverage | ~81.3% | Percentage of holdings with a known primary country |
| Chatbot tools available | 8 | Tools the AI agent can call to answer questions |
| Supported LLM backends | 3 | Claude (default), GPT-4o, Llama 3.1 (local) |
| Frontend pages | 6 | Dashboard, Holdings, Funds, Agent, Review Queue, Settings |
| Authentication | JWT + BCrypt | Industry-standard secure login |

---

*Last updated: April 26, 2026*
