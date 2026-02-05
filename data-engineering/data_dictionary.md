# Data Dictionary

This dictionary describes each table’s purpose, key columns, and how it is produced and consumed.

---

## Bronze Layer (Raw Inputs)

### bronze_document
**Purpose:** Track ingested documents (synthetic now; real PDFs later).  
**Key columns:**
- `document_uri`: where the source document lives
- `checksum_sha256`: immutability and duplicate detection
- `period_end`, `received_date`: reporting context  
**Produced by:** ingestion step  
**Consumed by:** report tracking, audit, future PDF ingestion

### bronze_document_section
**Purpose:** Store detected document sections (e.g., schedule of investments).  
**Key columns:**
- `section_type`, `page_start`, `page_end`
- `detection_confidence`  
**Produced by:** (future) section detection module  
**Consumed by:** (future) holdings parser

### fact_document_parse_event
**Purpose:** Observability for parser warnings/errors (append-only).  
**Key columns:**
- `severity`, `event_type`, `payload_json`  
**Produced by:** (future) parser  
**Consumed by:** debugging, operational monitoring

---

## Silver Layer (Conformed Entities + Metadata)

### dim_portfolio
**Purpose:** Canonical portfolio entity.  
**Key columns:**
- `portfolio_name`, `base_currency`, `owner_type`  
**Produced by:** configuration/bootstrap  
**Consumed by:** exposure outputs, dashboards

### dim_fund
**Purpose:** Canonical fund entity.  
**Key columns:**
- `fund_name`, `manager_name`, `strategy`, `vintage_year`  
**Produced by:** configuration/bootstrap  
**Consumed by:** reports, exposures, dashboards

### dim_company
**Purpose:** Canonical company entity used across public and private holdings.  
**Key columns:**
- `company_name`, `primary_country`, `website`  
**Produced by:** public datasets + entity resolution  
**Consumed by:** holdings normalization, exposure inference

### dim_entity_alias
**Purpose:** Map name variants to canonical entities (supports entity resolution).  
**Key columns:**
- `entity_type`, `entity_id`, `alias_text`, `confidence`  
**Produced by:** entity resolution pipeline  
**Consumed by:** matching raw names to canonical entities

### meta_taxonomy_version
**Purpose:** Version control for classification hierarchies (sector/geography).  
**Key columns:**
- `version_name`, `source_uri`  
**Produced by:** configuration  
**Consumed by:** classification and reproducibility

### dim_taxonomy_node
**Purpose:** Hierarchical taxonomy nodes (sector, industry, geography).  
**Key columns:**
- `taxonomy_type`, `node_name`, `parent_node_id`, `path`, `level`  
**Produced by:** taxonomy import  
**Consumed by:** rollups and dashboards

### meta_playbook_version
**Purpose:** Version control for rules, thresholds, and workflow logic.  
**Key columns:**
- `version_name`, `config_uri`, `notes`  
**Produced by:** configuration  
**Consumed by:** routing logic, auditability

### meta_run
**Purpose:** Reproducible run metadata tying outputs to code + configs.  
**Key columns:**
- `code_version_git_sha`, `parameters_json`
- `taxonomy_version_id`, `playbook_version_id`  
**Produced by:** orchestration runtime  
**Consumed by:** audit, comparing runs

---

## Facts (Reports + Holdings)

### fact_fund_report
**Purpose:** Fund reporting snapshot tied to a reporting period and document.  
**Key columns:**
- `fund_id`, `report_period_end`, `document_id`
- `coverage_estimate`  
**Produced by:** ingestion pipeline  
**Consumed by:** holdings, DQ scoring, downstream inference

### fact_reported_holding
**Purpose:** Holdings as reported (pre-inference), including extraction lineage.  
**Key columns:**
- `raw_company_name`
- `company_id` (nullable)
- `reported_value_usd`, `reported_pct_nav` (nullable)
- `extraction_method`, `extraction_confidence`
- `document_id`, `page_number`, `row_number`  
**Produced by:** synthetic inputs now; (future) PDF ingestion  
**Consumed by:** inference engine, review queue

---

## Gold Layer (Capstone Outputs)

### fact_inferred_exposure
**Purpose:** Canonical exposure outputs per run.  
**Key columns:**
- `exposure_value_usd`, `exposure_weight`
- `exposure_type`, `method`
- `as_of_date`, `run_id`  
**Produced by:** exposure inference pipeline  
**Consumed by:** dashboards, agent tools

### fact_exposure_classification
**Purpose:** Exposure classification (sector/geography) with confidence.  
**Key columns:**
- `taxonomy_type`, `taxonomy_node_id`
- `classification_confidence`, `classification_method`  
**Produced by:** classification pipeline  
**Consumed by:** aggregation and reporting

### fact_exposure_uncertainty
**Purpose:** Uncertainty bands per exposure.  
**Key columns:**
- `p10_value_usd`, `p50_value_usd`, `p90_value_usd`, `std_dev`  
**Produced by:** uncertainty module  
**Consumed by:** risk/concentration analysis

### fact_aggregation_snapshot
**Purpose:** BI-optimized rollups for dashboards and fast queries.  
**Key columns:**
- `portfolio_id`, `as_of_date`, `taxonomy_node_id`
- `total_exposure_value_usd`, `coverage_pct`
- optional uncertainty rollups (`p10`, `p90`)  
**Produced by:** aggregation step  
**Consumed by:** Power BI visuals, UI summaries

---

## Gold Layer (Practical Data Science Outputs)

### fact_data_quality_assessment
**Purpose:** Data quality scores by dimension and report.  
**Key columns:**
- `fund_report_id`, `dimension`, `score`, `details_json`  
**Produced by:** data quality scoring pipeline  
**Consumed by:** ops KPIs, review routing

### fact_conflict_case
**Purpose:** Track detected conflicts requiring attention.  
**Key columns:**
- `conflict_type`, `severity`, `status`, `evidence_json`  
**Produced by:** validation/conflict detection  
**Consumed by:** review workflows

### fact_review_queue_item
**Purpose:** Review queue items linking to exposures or reported holdings.  
**Key columns:**
- `reason`, `priority`, `status`
- `exposure_id` and/or `reported_holding_id`  
**Produced by:** routing logic (rules and/or agent)  
**Consumed by:** UI workflow, human decisions

### fact_human_decision
**Purpose:** Human decisions for queue items, including overrides.  
**Key columns:**
- `decision`, `override_payload`, `analyst`, `comment`  
**Produced by:** UI actions  
**Consumed by:** audit, downstream recompute

### fact_audit_event
**Purpose:** Append-only audit trail for system and human actions.  
**Key columns:**
- `actor_type`, `actor_id`
- `action`, `entity_type`, `entity_id`
- `payload_json`, chain hashes (`hash_prev`, `hash_curr`)  
**Produced by:** system + agent + UI  
**Consumed by:** auditability, debugging, governance reporting
