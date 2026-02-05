# Data Schema Design (Bronze / Silver / Gold)

This schema supports two aligned but distinct project tracks:

- **Capstone**: look-through exposure inference, uncertainty, and aggregation
- **Practical Data Science**: data quality, workflow, review queues, and auditability

The data model follows a **Bronze / Silver / Gold** pattern:
- **Bronze**: immutable raw inputs and document metadata
- **Silver**: conformed entities and normalized relationships
- **Gold**: analytics-ready outputs optimized for dashboards and applications

---

## Table Inventory

### Bronze (raw inputs)
- bronze_document
- bronze_document_section
- fact_document_parse_event

### Silver (dimensions + metadata)
- dim_portfolio
- dim_fund
- dim_company
- dim_entity_alias
- meta_taxonomy_version
- dim_taxonomy_node
- meta_playbook_version
- meta_run

### Facts (reports + holdings)
- fact_fund_report
- fact_reported_holding

### Gold (capstone outputs)
- fact_inferred_exposure
- fact_exposure_classification
- fact_exposure_uncertainty
- fact_aggregation_snapshot

### Gold (practical outputs)
- fact_data_quality_assessment
- fact_conflict_case
- fact_review_queue_item
- fact_human_decision
- fact_audit_event

---

## Keys, Constraints, and Indexes

### bronze_document
**Primary key:** document_id  
**Foreign keys:**  
- fund_id → dim_fund  
- portfolio_id → dim_portfolio  

**Indexes:**
- (fund_id, period_end)
- (checksum_sha256) UNIQUE

Purpose: store document metadata for current synthetic inputs and future PDF ingestion.

---

### bronze_document_section
**Primary key:** section_id  
**Foreign keys:** document_id → bronze_document  

**Indexes:**
- (document_id, page_start)

Purpose: identify logical sections within documents (e.g., schedule of investments).

---

### fact_document_parse_event
**Primary key:** parse_event_id  
**Foreign keys:** document_id → bronze_document  

**Indexes:**
- (document_id, event_time)
- (severity)

Purpose: append-only log of parsing warnings, errors, and events.

---

### dim_portfolio
**Primary key:** portfolio_id  

**Indexes:**
- (portfolio_name)

Purpose: canonical portfolio entity.

---

### dim_fund
**Primary key:** fund_id  

**Indexes:**
- (manager_name)
- (strategy)

Purpose: canonical fund entity.

---

### dim_company
**Primary key:** company_id  

**Indexes:**
- (company_name)

Purpose: canonical company entity used across public and private holdings.

---

### dim_entity_alias
**Primary key:** alias_id  

**Indexes:**
- (alias_text)
- (entity_type, entity_id)

Purpose: support entity resolution from raw names to canonical entities.

---

### meta_taxonomy_version
**Primary key:** taxonomy_version_id  

**Indexes:**
- (version_name) UNIQUE

Purpose: version control for classification hierarchies.

---

### dim_taxonomy_node
**Primary key:** taxonomy_node_id  
**Foreign keys:**
- taxonomy_version_id → meta_taxonomy_version  
- parent_node_id → dim_taxonomy_node (self-reference)

**Indexes:**
- (taxonomy_version_id, taxonomy_type)
- (parent_node_id)
- (path)

Purpose: hierarchical taxonomy nodes (sector, industry, geography).

---

### meta_playbook_version
**Primary key:** playbook_version_id  

**Indexes:**
- (version_name) UNIQUE

Purpose: version control for rules, thresholds, and workflow logic.

---

### meta_run
**Primary key:** run_id  
**Foreign keys:**
- playbook_version_id → meta_playbook_version  
- taxonomy_version_id → meta_taxonomy_version  

**Indexes:**
- (run_type, started_at)

Purpose: tie outputs to code version, configuration, and assumptions.

---

### fact_fund_report
**Primary key:** fund_report_id  
**Foreign keys:**
- fund_id → dim_fund  
- document_id → bronze_document  

**Constraints:**
- UNIQUE (fund_id, report_period_end)

**Indexes:**
- (fund_id, report_period_end)

Purpose: represent fund-level reporting snapshots.

---

### fact_reported_holding
**Primary key:** reported_holding_id  
**Foreign keys:**
- fund_report_id → fact_fund_report  
- document_id → bronze_document  
- section_id → bronze_document_section (nullable)  
- company_id → dim_company (nullable)

**Indexes:**
- (fund_report_id)
- (company_id)
- (document_id, page_number)

Purpose: holdings as reported, including extraction lineage and confidence.

---

## Capstone Output Tables (Gold)

### fact_inferred_exposure
**Primary key:** exposure_id  
**Foreign keys:**
- run_id → meta_run  
- portfolio_id → dim_portfolio  
- fund_id → dim_fund (nullable for direct holdings)  
- company_id → dim_company  
- taxonomy_version_id → meta_taxonomy_version  

**Constraints:**
- UNIQUE (run_id, portfolio_id, fund_id, company_id, as_of_date, exposure_type)

**Indexes:**
- (portfolio_id, as_of_date)
- (fund_id, as_of_date)
- (company_id)

Purpose: canonical exposure outputs produced by the inference engine.

---

### fact_exposure_classification
**Primary key:** (exposure_id, taxonomy_type)  
**Foreign keys:**
- exposure_id → fact_inferred_exposure  
- taxonomy_node_id → dim_taxonomy_node  

**Indexes:**
- (taxonomy_node_id)

Purpose: classification of exposures by sector/geography with confidence.

---

### fact_exposure_uncertainty
**Primary key:** (exposure_id, uncertainty_method)  
**Foreign keys:**
- exposure_id → fact_inferred_exposure  

Purpose: uncertainty bands and sensitivity metrics.

---

### fact_aggregation_snapshot
**Foreign keys:**
- run_id → meta_run  
- portfolio_id → dim_portfolio  
- taxonomy_node_id → dim_taxonomy_node  

**Indexes:**
- (portfolio_id, as_of_date, taxonomy_type)

Purpose: dashboard-optimized rollups for fast querying.

---

## Practical Data Science Tables (Gold)

### fact_data_quality_assessment
**Primary key:** dq_id  
**Foreign keys:**
- run_id → meta_run  
- fund_report_id → fact_fund_report  

**Indexes:**
- (fund_report_id)
- (dimension)

Purpose: data quality scoring by dimension.

---

### fact_conflict_case
**Primary key:** conflict_id  
**Foreign keys:**
- run_id → meta_run  

**Indexes:**
- (status)
- (severity)

Purpose: track detected conflicts requiring review.

---

### fact_review_queue_item
**Primary key:** queue_item_id  
**Foreign keys:**
- run_id → meta_run  
- exposure_id → fact_inferred_exposure (nullable)  
- reported_holding_id → fact_reported_holding (nullable)

**Indexes:**
- (status, priority)

Purpose: human review queue for low-confidence or conflicting items.

---

### fact_human_decision
**Primary key:** decision_id  
**Foreign keys:**
- queue_item_id → fact_review_queue_item  

**Indexes:**
- (queue_item_id)

Purpose: record human approvals and overrides.

---

### fact_audit_event
**Primary key:** audit_event_id  
**Foreign keys:**
- run_id → meta_run  

**Indexes:**
- (run_id, event_time)
- (entity_type, entity_id)

Purpose: append-only audit trail for system and human actions.

---

## Justified Denormalization (Gold Layer)

Gold tables such as `fact_aggregation_snapshot` intentionally store pre-aggregated results to optimize BI and application performance.

Rather than requiring repeated joins across large fact and dimension tables at query time, rollups are computed once during the pipeline and served directly to dashboards and agent tools.

This design:
- reduces query latency
- simplifies semantic modeling
- preserves reproducibility via `run_id`

---

## Data Growth and Optimization

Expected growth drivers:
- exposures scale with portfolios × funds × companies × time
- audit events scale with system activity (append-only)

Optimization strategies:
- partition large fact tables by `as_of_date`
- index frequently filtered columns
- rebuild Gold snapshots deterministically from upstream data

---

## Backup and Recovery

- Bronze inputs are immutable and retained with checksums
- Silver and Gold tables can be rebuilt from Bronze plus `meta_run` metadata
- Table versioning or time travel is used where supported
- Periodic exports of Gold snapshots provide additional recovery assurance

