"""SQLAlchemy ORM models for Silver and Gold tables.

Uses SQLAlchemy 2.0 style with DeclarativeBase.
All models match the existing CSV schema exactly.
"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


# =============================================================================
# Authentication Models
# =============================================================================


class User(Base):
    """User account for authentication."""

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )


# =============================================================================
# Silver Layer - Dimension Tables
# =============================================================================


class DimPortfolio(Base):
    """Canonical portfolio entity."""

    __tablename__ = "dim_portfolio"

    portfolio_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    portfolio_name: Mapped[str] = mapped_column(String(500))
    base_currency: Mapped[str] = mapped_column(String(10))
    owner_type: Mapped[str] = mapped_column(String(100), nullable=True)
    created_at: Mapped[str] = mapped_column(String(50), nullable=True)
    source: Mapped[str] = mapped_column(String(50), nullable=True)


class DimFund(Base):
    """Canonical fund entity."""

    __tablename__ = "dim_fund"

    fund_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    fund_name: Mapped[str] = mapped_column(String(500))
    manager_name: Mapped[str] = mapped_column(String(500), nullable=True)
    fund_type: Mapped[str] = mapped_column(String(100), nullable=True)
    strategy: Mapped[str] = mapped_column(String(100), nullable=True)
    vintage_year: Mapped[float] = mapped_column(Float, nullable=True)
    base_currency: Mapped[str] = mapped_column(String(10), nullable=True)
    source: Mapped[str] = mapped_column(String(50), nullable=True)


class DimCompany(Base):
    """Canonical company entity used across public and private holdings."""

    __tablename__ = "dim_company"

    company_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    company_name: Mapped[str] = mapped_column(String(500))
    primary_sector: Mapped[str] = mapped_column(Text, nullable=True)
    primary_industry: Mapped[str] = mapped_column(Text, nullable=True)
    primary_country: Mapped[str] = mapped_column(String(100), nullable=True)
    industry_taxonomy_node_id: Mapped[str] = mapped_column(String(36), nullable=True)
    country_taxonomy_node_id: Mapped[str] = mapped_column(String(36), nullable=True)
    website: Mapped[str] = mapped_column(String(500), nullable=True)
    created_at: Mapped[str] = mapped_column(String(50), nullable=True)
    source: Mapped[str] = mapped_column(String(50), nullable=True)


class DimEntityAlias(Base):
    """Map name variants to canonical entities (supports entity resolution)."""

    __tablename__ = "dim_entity_alias"

    alias_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(50))
    entity_id: Mapped[str] = mapped_column(String(100))  # consolidation IDs can be 51+ chars
    alias_text: Mapped[str] = mapped_column(String(500))
    confidence: Mapped[float] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(50), nullable=True)


class DimTaxonomyNode(Base):
    """Hierarchical taxonomy nodes (sector, industry, geography)."""

    __tablename__ = "dim_taxonomy_node"

    taxonomy_node_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    taxonomy_version_id: Mapped[str] = mapped_column(String(36))
    taxonomy_type: Mapped[str] = mapped_column(String(50))
    node_name: Mapped[str] = mapped_column(String(500))
    parent_node_id: Mapped[str] = mapped_column(String(36), nullable=True)
    path: Mapped[str] = mapped_column(String(1000), nullable=True)
    level: Mapped[float] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(50), nullable=True)


class MetaTaxonomyVersion(Base):
    """Version control for classification hierarchies (sector/geography)."""

    __tablename__ = "meta_taxonomy_version"

    taxonomy_version_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    version_name: Mapped[str] = mapped_column(String(500))
    source_uri: Mapped[str] = mapped_column(String(500), nullable=True)
    created_at: Mapped[str] = mapped_column(String(50), nullable=True)
    source: Mapped[str] = mapped_column(String(50), nullable=True)


# =============================================================================
# Silver Layer - Fact Tables
# =============================================================================


class FactFundReport(Base):
    """Fund reporting snapshot tied to a reporting period and document."""

    __tablename__ = "fact_fund_report"

    fund_report_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    fund_id: Mapped[str] = mapped_column(String(36))
    report_period_end: Mapped[str] = mapped_column(String(20))
    received_date: Mapped[str] = mapped_column(String(20), nullable=True)
    document_id: Mapped[str] = mapped_column(String(36), nullable=True)
    coverage_estimate: Mapped[float] = mapped_column(Float, nullable=True)
    nav_usd: Mapped[float] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(50), nullable=True)


class FactReportedHolding(Base):
    """Holdings as reported (pre-inference), including extraction lineage."""

    __tablename__ = "fact_reported_holding"

    reported_holding_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    fund_report_id: Mapped[str] = mapped_column(String(36))
    company_id: Mapped[str] = mapped_column(String(36), nullable=True)
    raw_company_name: Mapped[str] = mapped_column(String(500), nullable=True)
    reported_sector: Mapped[str] = mapped_column(Text, nullable=True)
    reported_country: Mapped[str] = mapped_column(String(100), nullable=True)
    reported_value_usd: Mapped[float] = mapped_column(Float, nullable=True)
    reported_pct_nav: Mapped[float] = mapped_column(Float, nullable=True)
    extraction_method: Mapped[str] = mapped_column(String(100), nullable=True)
    extraction_confidence: Mapped[float] = mapped_column(Float, nullable=True)
    document_id: Mapped[str] = mapped_column(String(36), nullable=True)
    page_number: Mapped[float] = mapped_column(Float, nullable=True)
    row_number: Mapped[float] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(50), nullable=True)
    as_of_date: Mapped[str] = mapped_column(String(20), nullable=True)


# =============================================================================
# Gold Layer - Capstone Output Tables
# =============================================================================


class FactInferredExposure(Base):
    """Canonical exposure outputs per run."""

    __tablename__ = "fact_inferred_exposure"

    exposure_id: Mapped[str] = mapped_column(String(100), primary_key=True)  # consolidation IDs can be 51+ chars
    run_id: Mapped[str] = mapped_column(String(36))
    portfolio_id: Mapped[str] = mapped_column(String(36))
    fund_id: Mapped[str] = mapped_column(String(36))
    company_id: Mapped[str] = mapped_column(String(36), nullable=True)
    raw_company_name: Mapped[str] = mapped_column(String(500), nullable=True)
    as_of_date: Mapped[str] = mapped_column(String(20))
    exposure_value_usd: Mapped[float] = mapped_column(Float)
    exposure_weight: Mapped[float] = mapped_column(Float)
    exposure_type: Mapped[str] = mapped_column(String(100))
    method: Mapped[str] = mapped_column(String(100))


class FactExposureClassification(Base):
    """Exposure classification (sector/geography) with confidence."""

    __tablename__ = "fact_exposure_classification"

    classification_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36))
    company_id: Mapped[str] = mapped_column(String(36), nullable=True)
    raw_company_name: Mapped[str] = mapped_column(String(500), nullable=True)
    taxonomy_type: Mapped[str] = mapped_column(String(50))
    taxonomy_node_id: Mapped[str] = mapped_column(String(36))
    confidence: Mapped[float] = mapped_column(Float)
    rationale: Mapped[str] = mapped_column(Text, nullable=True)
    assumptions_json: Mapped[str] = mapped_column(Text, nullable=True)
    model: Mapped[str] = mapped_column(String(100), nullable=True)
    prompt_version: Mapped[str] = mapped_column(String(50), nullable=True)


class FactAggregationSnapshot(Base):
    """BI-optimized rollups for dashboards and fast queries."""

    __tablename__ = "fact_aggregation_snapshot"

    # Composite primary key
    run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    as_of_date: Mapped[str] = mapped_column(String(20), primary_key=True)
    taxonomy_type: Mapped[str] = mapped_column(String(50), primary_key=True)
    taxonomy_node_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    total_exposure_value_usd: Mapped[float] = mapped_column(Float)
    total_exposure_p10: Mapped[float] = mapped_column(Float, nullable=True)
    total_exposure_p90: Mapped[float] = mapped_column(Float, nullable=True)
    coverage_pct: Mapped[float] = mapped_column(Float, nullable=True)
    confidence_weighted_exposure: Mapped[float] = mapped_column(Float, nullable=True)


# =============================================================================
# Gold Layer - Practical Data Science Output Tables
# =============================================================================


class FactReviewQueueItem(Base):
    """Review queue items linking to exposures or reported holdings."""

    __tablename__ = "fact_review_queue_item"

    queue_item_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36))
    exposure_id: Mapped[str] = mapped_column(String(100), nullable=True)  # consolidation IDs can be 51+ chars
    reported_holding_id: Mapped[str] = mapped_column(String(100), nullable=True)  # consolidation IDs can be 51+ chars
    company_id: Mapped[str] = mapped_column(String(36), nullable=True)
    raw_company_name: Mapped[str] = mapped_column(String(500), nullable=True)
    reason: Mapped[str] = mapped_column(Text)
    priority: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(50))
    created_at: Mapped[str] = mapped_column(String(50))


class FactAuditEvent(Base):
    """Append-only audit trail for system and human actions."""

    __tablename__ = "fact_audit_event"

    audit_event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36))
    event_time: Mapped[str] = mapped_column(String(50))
    actor_type: Mapped[str] = mapped_column(String(20))
    actor_id: Mapped[str] = mapped_column(String(100))
    action: Mapped[str] = mapped_column(String(100))
    entity_type: Mapped[str] = mapped_column(String(50))
    entity_id: Mapped[str] = mapped_column(String(100))  # consolidation IDs can be 51+ chars
    payload_json: Mapped[str] = mapped_column(Text, nullable=True)


class EntityResolutionLog(Base):
    """Log of entity resolution decisions."""

    __tablename__ = "entity_resolution_log"

    # Use reported_holding_id as primary key since each holding gets one resolution entry
    # Note: consolidation records use IDs like "consolidation_xxx" which can be 51+ chars
    reported_holding_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    raw_company_name: Mapped[str] = mapped_column(String(500), nullable=True)
    matched_company_id: Mapped[str] = mapped_column(String(36), nullable=True)
    match_method: Mapped[str] = mapped_column(String(50), nullable=True)
    match_confidence: Mapped[float] = mapped_column(Float, nullable=True)
    timestamp: Mapped[str] = mapped_column(String(50), nullable=True)
    action: Mapped[str] = mapped_column(String(50), nullable=True)
    canonical_company_id: Mapped[str] = mapped_column(String(36), nullable=True)
    canonical_company_name: Mapped[str] = mapped_column(String(500), nullable=True)
    duplicate_company_id: Mapped[str] = mapped_column(String(36), nullable=True)
    duplicate_company_name: Mapped[str] = mapped_column(String(500), nullable=True)
    method: Mapped[str] = mapped_column(String(50), nullable=True)
    reason: Mapped[str] = mapped_column(Text, nullable=True)


class GICSMapping(Base):
    """GICS taxonomy mapping for reported sectors."""

    __tablename__ = "gics_mapping"

    # Use reported_sector as primary key
    reported_sector: Mapped[str] = mapped_column(Text, primary_key=True)
    gics_sector_code: Mapped[float] = mapped_column(Float, nullable=True)
    gics_sector_name: Mapped[str] = mapped_column(String(500), nullable=True)
    gics_industry_group_code: Mapped[float] = mapped_column(Float, nullable=True)
    gics_industry_group_name: Mapped[str] = mapped_column(String(500), nullable=True)
    gics_industry_code: Mapped[float] = mapped_column(Float, nullable=True)
    gics_industry_name: Mapped[str] = mapped_column(String(500), nullable=True)
    gics_sub_industry_code: Mapped[float] = mapped_column(Float, nullable=True)
    gics_sub_industry_name: Mapped[str] = mapped_column(String(500), nullable=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=True)
    rationale: Mapped[str] = mapped_column(Text, nullable=True)
