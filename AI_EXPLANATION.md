# AI Components — Plain English Explanation
*Northbridge Endowment Fund Look-Through Platform*

---

## Table of Contents

1. [The Chatbot (chat.py + tools.py)](#1-the-chatbot)
2. [Company Classification (classify_companies.py)](#2-company-classification)
3. [PDF Extraction (ingest_pdf_documents.py)](#3-pdf-extraction)

---

## 1. The Chatbot

### What These Files Do

**chat.py** — One sentence: Receives a user's question, passes it to an AI model along with a set of database-query tools, and returns a professional written answer.

**tools.py** — One sentence: Contains the nine database query functions (tools) that the AI is allowed to call when it needs data to answer a question.

---

### Walk-Through: "What is our Technology exposure?"

Here is exactly what happens, step by step, when a user types that question:

**Step 1 — The message arrives**

The frontend sends the text `"What is our Technology exposure?"` to the API endpoint. Python calls `chat()`, which routes to `chat_with_claude()` (the default provider).

**Step 2 — Build the conversation package**

Python assembles:
- The **system prompt** (full text below)
- Any prior conversation turns (chat history from this session)
- The new user message

**Step 3 — First call to Claude Sonnet**

Python calls the Anthropic API with:
- Model: `claude-sonnet-4-20250514`
- Max tokens: 4,096
- The system prompt, conversation history, and user message
- A list of 9 available tools (database functions Claude may call)

Claude reads the question and the tool descriptions. It decides: *"I need to call `get_sector_exposure` to answer this."*

Claude does **not** query the database directly. It returns a structured instruction saying *"please run this tool with these arguments."*

**Step 4 — Python runs the tool**

Python receives Claude's instruction and calls `get_sector_exposure()` in tools.py. That function:
1. Queries `fact_lp_scaled_exposure` in PostgreSQL
2. Uses the per-fund latest date pattern (each fund's most recent quarter)
3. Joins to `dim_company` to get `primary_sector` names
4. Normalizes sector names (e.g. "Technology" → "Information Technology")
5. Groups by sector and sums `scaled_value_usd`
6. Calculates `pct_of_total` = sector value ÷ $713,300,000 × 100
7. Returns the top 10 sectors plus an "Other" bucket

**Step 5 — Tool result example**

The raw dictionary returned by `get_sector_exposure()` looks like this:

```json
{
  "as_of_date": "2024-12-31 to 2025-12-31",
  "total_portfolio_value_usd": 713300000.0,
  "classified_exposure_usd": 590000000.0,
  "unclassified_exposure_usd": 123300000.0,
  "unclassified_pct": 17.3,
  "coverage_pct": 82.7,
  "fund_name_filter": null,
  "sector_count": 10,
  "sectors": [
    {
      "sector_name": "Information Technology",
      "total_exposure_value_usd": 227400000.0,
      "pct_of_total": 31.9
    },
    {
      "sector_name": "Health Care",
      "total_exposure_value_usd": 85200000.0,
      "pct_of_total": 11.9
    },
    ...
  ]
}
```

**Step 6 — Second call to Claude Sonnet**

Python sends the tool result back to Claude as a "tool result" message. Claude now has:
- The original question
- The raw data from the database

Claude writes a professional answer following the rules in the system prompt — using $ millions, % of total portfolio, no lengthy disclaimers.

**Step 7 — Answer returned to the user**

Claude's response text is returned to the frontend and displayed in the chat window. A typical answer:

> *"Northbridge's Information Technology exposure is $227.4M, representing 31.9% of the total portfolio. This is the largest sector allocation, followed by Health Care at $85.2M (11.9%) and Financials at $71.1M (10.0%)."*

---

### The Full System Prompt Sent to Claude

This exact text is sent at the start of every conversation (with today's date and the data date range filled in dynamically):

```
You are the AI assistant for Northbridge Endowment Fund. The portfolio
represents Northbridge's LP positions scaled to their ownership stake in each fund.
Total current portfolio exposure is approximately $713M across 12 funds. All exposure
figures shown are Northbridge's proportional exposure, not the full fund values.

Today's date is 2026-04-28. Portfolio data spans multiple reporting dates (latest per fund,
ranging from 2024-12-31 to 2025-12-31). When reporting total exposure use all funds at their
latest available date = $713M total. Never reference dates beyond 2025-12-31.
When asked about current exposure, use the latest available data.

Your role is to help users understand their portfolio's look-through exposures,
including sector, industry, geography, fund-level, and company-level breakdowns.

Portfolio data quality context:
The portfolio has 83% classification coverage. The unclassified 17% are primarily BDC
borrowers with opaque holding company names — this is normal for private credit portfolios.
Do not repeatedly warn about data quality in every response. Mention coverage once briefly
if asked, or if coverage is below 85%, then move on.

Key principles:
1. Always use the provided tools to query portfolio data BEFORE answering questions.
   Do not guess or fabricate holdings, exposure values, or percentages.

2. Never show duplicate sector entries. Aggregate all positions by sector before reporting.

3. Always report exposure in millions (e.g. $713.3M not $713,300,000).

4. When asked about sector exposure, show top 5 sectors with $ amount and % of total
   portfolio. Do not list individual holdings unless specifically asked.

5. When reporting sector exposure percentages, always use % of total portfolio ($713M)
   as the denominator, not % of classified portfolio. Example: 'Technology exposure is
   $227M, representing 31.9% of the total portfolio.' Never say '32.6% of classified
   portfolio' — this is confusing to investors.

6. Keep responses concise and professional — 3-5 sentences for simple queries, a structured
   list for complex ones. No lengthy disclaimers.

7. For analytical questions about risks, market context, or investment implications,
   clearly label your analysis as AI-generated insight separate from portfolio facts.
   Use phrases like "Based on the data, my analysis suggests..." or "From an analytical
   perspective..."

8. If a question cannot be answered with the available tools or data, say so clearly
   rather than speculating.

Available capabilities:
- Portfolio summary and high-level metrics
- Sector, industry, and geography exposure breakdowns
- Fund-level and company-level exposure details
- Portfolio health and data quality summary (use get_portfolio_health for data quality questions)
- Review queue items requiring attention
- Confidence distribution analysis
```

---

### How Claude Knows Which Tool to Call

Claude is not given the database schema. Instead, each tool function in tools.py has a docstring (a description written in plain English), and Python automatically converts these into a tool specification that Claude receives. For example, `get_sector_exposure` has this description:

> *"Get portfolio exposure breakdown by sector. Returns sector-level exposure aggregated from scaled LP exposure. Each sector appears exactly once (deduplicated). Returns top 10 sectors plus an 'Other' bucket. Use this to understand the high-level sector allocation of the portfolio."*

Claude reads all 9 tool descriptions and picks whichever one matches the user's question. It reasons in plain English: "The user asked about sector exposure → I should call `get_sector_exposure`."

---

### The Nine Available Tools

| Tool | What It Returns |
|------|-----------------|
| `get_sector_exposure` | Top 10 sectors by $ value and % of total portfolio |
| `get_industry_exposure` | Sub-sector industry breakdown |
| `get_geography_exposure` | Country-level exposure breakdown |
| `get_fund_exposure` | Breakdown by fund (Vertex, ARCC, etc.) |
| `get_company_exposure` | Top companies by exposure value |
| `get_review_queue` | Items flagged for human review |
| `get_portfolio_summary` | High-level totals, coverage, source mix |
| `get_confidence_distribution` | AI classification confidence statistics |
| `get_portfolio_health` | Data quality overview |

---

### Does Claude Query the Database Directly?

**No.** Claude never touches the database. The flow is always:

```
User → Python (chat.py) → Claude (decides which tool)
                        ← Claude (tool call instruction)
     → Python (tools.py runs the SQL query)
                        → Claude (receives the data)
                        ← Claude (writes the answer)
     → User
```

Claude only sees the tool results — clean JSON dictionaries. Python does all the SQL.

---

### How Conversation History Works

Each message turn is stored in a list:
```python
conversation_history = [
    {"role": "user",      "content": "What is our Technology exposure?"},
    {"role": "assistant", "content": "Information Technology is $227M (31.9%)..."},
    {"role": "user",      "content": "How does that compare to last quarter?"},
]
```

The entire history is sent with every new message so Claude has context. The frontend is responsible for maintaining and passing this list. There is no server-side session storage — the history lives in the browser.

---

### GPT-4o and Ollama Alternatives

The same logic runs with two other AI providers, selectable by setting `provider`:

**OpenAI / GPT-4o** — Uses the OpenAI SDK instead of Anthropic. The tool format differs slightly (called "functions" rather than "tools") but the logic is identical. Same system prompt, same tools, same results.

**Ollama (local, e.g. llama3.1)** — Uses a locally running open-source model via HTTP. No data leaves the building. Same tool-calling loop, but Ollama's reliability with tool use is lower than Claude or GPT-4o, so responses may be less consistent.

The choice of provider does not change the database queries — only which AI writes the final answer.

---

## 2. Company Classification

### What This File Does

**classify_companies.py** — One sentence: Takes a list of company names with descriptions and asks Claude Haiku to assign each one a GICS sector, GICS industry, and country, then writes the results back to the database.

---

### Step-by-Step: How a Company Gets Classified

**Step 0 — Instrument rule check (no AI needed)**

Before calling the AI at all, Python checks if the company name matches a hardcoded pattern:

| Pattern | Auto-assigned Sector | Auto-assigned Industry |
|---------|---------------------|----------------------|
| Contains "Bitcoin", "Crypto", "BTC" | Financials | Digital Assets |
| Contains "ETF", "Index Fund", "SPAC" | Financials | Investment Vehicles |
| Contains "Warrant", "Option" | Financials | Derivatives |
| Contains "Treasury", "T-Bill" | Financials | Government Securities |

If a match is found, classification is complete with 100% confidence. No API call is made.

**Step 1 — Sector Classification (first AI call)**

For each company that didn't match an instrument rule, Python sends this prompt to Claude Haiku:

```
taxonomy_type: sector
Return JSON matching the required schema.

{
  "company_name": "Pioneer Group",
  "company_country": "JP",
  "company_description": "A diversified industrial conglomerate...",
  "allowed_nodes": [
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities"
  ]
}
```

Claude Haiku returns JSON matching this structure:

```json
{
  "taxonomy_type": "sector",
  "node_name": "Industrials",
  "confidence": 0.87,
  "rationale": "Pioneer Group operates diversified industrial manufacturing and distribution businesses across Japan.",
  "assumptions": ["Based on company name and country context"]
}
```

**Step 2 — Industry Classification (second AI call)**

Using the sector result from Step 1, Python filters the allowed industry list to only the industries within that sector. For "Industrials" the allowed nodes would be: Capital Goods, Commercial & Professional Services, Transportation, etc.

The same prompt structure is sent again, but now:
- `taxonomy_type: industry`
- `allowed_nodes` contains only the sub-industries of "Industrials"

Claude Haiku returns:

```json
{
  "taxonomy_type": "industry",
  "node_name": "Capital Goods",
  "confidence": 0.82,
  "rationale": "The company manufactures industrial equipment.",
  "assumptions": []
}
```

**Step 3 — Country Classification (third AI call, if needed)**

If `primary_country` is NULL in the database, Python sends a simpler prompt:

```
What country is 'Pioneer Group' headquartered or domiciled in?
Return ONLY the ISO 2-letter country code (e.g. US, GB, DE, JP, CN).
If this is an opaque holding company, SPAC, or you cannot determine
the country with confidence, return NULL.
Do not explain. Return only the code or NULL.
```

Claude Haiku returns: `JP`

Special rule: Companies sourced from 13F regulatory filings are automatically assigned `US` with no API call (13F filers are by definition US public companies).

**Step 4 — Results written to database**

After classification, Python writes:
- `dim_company.primary_sector` = "Industrials"
- `dim_company.primary_industry` = "Capital Goods"
- `dim_company.primary_country` = "JP"
- `fact_exposure_classification` table gets a row with the confidence score and rationale

---

### What Happens When Haiku Can't Classify

If the confidence score is below **0.70** (the threshold), the company is flagged and added to the review queue. A human reviewer sees it in the Review Queue page and can manually assign the correct sector and industry.

If the JSON response from Haiku is malformed or invalid, Python catches the error and assigns a zero-confidence classification, which also lands in the review queue.

---

### The AI Model Used

All classification uses **Claude Haiku** (`claude-haiku-4-5-20251001`), not Claude Sonnet. Haiku is faster and cheaper for batch processing thousands of companies. Max 512 tokens per response. Batches of 20 companies are processed at a time.

Rate limiting is handled automatically: if the API returns a rate-limit error, Python waits 60 seconds and retries.

---

## 3. PDF Extraction

### What This File Does

**ingest_pdf_documents.py** — One sentence: Opens PDF fund documents from a OneDrive folder, extracts the text, sends it to Claude Haiku with a structured prompt, and saves the resulting holdings data to the database.

---

### Step-by-Step: How a PDF Gets Ingested

**Step 1 — File discovery and deduplication**

Python scans the OneDrive folder: `C:\Users\kylej\OneDrive\LookThrough Fund Documents`

Each PDF file is hashed with MD5. If the hash is already in the manifest file (a local JSON log), the file is skipped. This prevents re-processing documents that haven't changed.

**Step 2 — Fund matching**

Each subfolder (e.g. "Vertex Growth Fund III") is fuzzy-matched to a fund in the database using a similarity score. The threshold is 60% match. If no fund matches, a new fund record is created.

**Step 3 — Document type detection**

Python reads the filename and first page of the PDF to determine which of three document types it is:

| Type | What It Contains | Keywords Detected |
|------|-----------------|-------------------|
| `financial_statements` | Schedule of Investments — all holdings with values | "schedule of investments", "portfolio of investments" |
| `lp_statement` | Northbridge's own capital account | "capital account", "LP statement", "partner capital" |
| `transparency_report` | Hedge fund position-level report | "transparency", "position report", "holdings report" |

**Step 4 — Text extraction**

Python uses `pdfplumber` to extract the raw text from all pages. The text is truncated to 40,000 characters to stay within Claude's context window.

**Step 5 — AI extraction via Claude Haiku**

Python sends the extracted text to Claude Haiku with one of three prompts depending on document type:

---

**Prompt for Financial Statements (Schedule of Investments):**

```
Extract the Schedule of Investments from this fund document.
Return ONLY valid JSON:
{
  "fund_name": "...",
  "reporting_date": "YYYY-MM-DD",
  "total_net_assets_usd": 604000000,
  "holdings": [
    {
      "company_name": "...",
      "sector": "...",
      "cost_basis_usd": 0.0,
      "fair_value_usd": 0.0,
      "ownership_pct": 0.0
    }
  ]
}

Extract total_net_assets_usd from the balance sheet
(labeled as Net Assets, Total Net Assets, or Partners Capital).
Return as full integer in USD.

IMPORTANT: All monetary values must be full integers in USD, not abbreviated
(e.g. 1500000 not 1.5M, 604000000 not $604MM).

Document text:
[... up to 40,000 characters of PDF text ...]
```

---

**Prompt for LP Capital Account Statements:**

```
Extract the LP capital account data from this document.
Return ONLY valid JSON:
{
  "fund_name": "...",
  "lp_name": "...",
  "reporting_date": "YYYY-MM-DD",
  "nav_usd": 0.0,
  "contributions_usd": 0.0,
  "distributions_usd": 0.0,
  "irr_pct": 0.0,
  "moic": 0.0,
  "unfunded_commitment_usd": 0.0
}

IMPORTANT: All monetary values must be full integers in USD, not abbreviated.

Document text:
[... PDF text ...]
```

---

**Prompt for Transparency Reports (Hedge Fund Holdings):**

```
Extract all holdings from this hedge fund transparency report.
Return ONLY valid JSON:
{
  "fund_name": "...",
  "reporting_date": "YYYY-MM-DD",
  "holdings": [
    {
      "company_name": "...",
      "is_public": true,
      "fair_value_usd": 0.0,
      "pct_nav": 0.0,
      "sector": "..."
    }
  ]
}

IMPORTANT: All monetary values must be full integers in USD, not abbreviated.

Document text:
[... PDF text ...]
```

---

**Step 6 — Response parsing**

Claude Haiku returns raw JSON text. Python:
1. Strips any markdown formatting (` ```json ``` ` fences) that Haiku sometimes adds
2. Parses the JSON with `json.loads()`
3. If parsing fails, logs the error and skips the file

**Step 7 — Writing to database**

The extracted data is upserted (insert or update) into four tables:

| Table | What Gets Written |
|-------|------------------|
| `dim_fund` | Fund name, type, source |
| `fact_fund_report` | Reporting date, total net assets |
| `dim_company` | Company name (new companies created here) |
| `fact_reported_holding` | Each holding: company, fair value, ownership % |

The model used is: **Claude Haiku** (`claude-haiku-4-5-20251001`), max 4,096 tokens.

---

### Example: What the Extracted JSON Looks Like

For a financial statement PDF from Vertex Growth Fund with 3 holdings:

```json
{
  "fund_name": "Vertex Growth Fund III",
  "reporting_date": "2025-12-31",
  "total_net_assets_usd": 142000000,
  "holdings": [
    {
      "company_name": "Acme Robotics Inc",
      "sector": "Information Technology",
      "cost_basis_usd": 8500000,
      "fair_value_usd": 12300000,
      "ownership_pct": 3.2
    },
    {
      "company_name": "BioSynth Therapeutics",
      "sector": "Health Care",
      "cost_basis_usd": 5000000,
      "fair_value_usd": 7800000,
      "ownership_pct": 1.8
    }
  ]
}
```

This JSON is then split: `dim_company` gets "Acme Robotics Inc" and "BioSynth Therapeutics" as new rows, and `fact_reported_holding` gets two rows with the dollar values.

---

## Summary: Which AI Model Does What

| Task | Model | When It Runs |
|------|-------|-------------|
| Answer investor questions | Claude Sonnet (`claude-sonnet-4-20250514`) | Every chat message |
| Classify company sectors/industries | Claude Haiku (`claude-haiku-4-5-20251001`) | Pipeline Step 7 (batch) |
| Classify company countries | Claude Haiku (`claude-haiku-4-5-20251001`) | Pipeline Step 7 (batch) |
| Extract holdings from PDFs | Claude Haiku (`claude-haiku-4-5-20251001`) | PDF ingestion pipeline |

Haiku is used for all batch processing (fast, cheap, handles structured JSON well).
Sonnet is used for the chatbot (more capable reasoning and writing quality).
