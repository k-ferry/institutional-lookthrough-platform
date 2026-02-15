"""Unified data source loader for the Institutional Look-Through Platform.

This module merges holdings from all available sources (synthetic, BDC filings, etc.)
into the canonical Silver tables the pipeline expects.

Usage:
    python -m src.lookthrough.ingestion.load_sources
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SILVER_DIR = Path("data/silver")

# Synthetic Silver table filenames
SYNTHETIC_TABLES = [
    "dim_company.csv",
    "dim_fund.csv",
    "fact_fund_report.csv",
    "fact_reported_holding.csv",
    "dim_taxonomy_node.csv",
    "dim_entity_alias.csv",
    "dim_portfolio.csv",
    "meta_taxonomy_version.csv",
]

# BDC parsed output filenames
BDC_TABLES = {
    "holdings": "bdc_fact_reported_holding.csv",
    "funds": "bdc_dim_fund.csv",
    "reports": "bdc_fact_fund_report.csv",
}

# Source column values
SOURCE_SYNTHETIC = "synthetic"
SOURCE_BDC = "bdc_filing"


# ---------------------------------------------------------------------------
# Deterministic UUID Generation
# ---------------------------------------------------------------------------

def make_uuid(seed: str) -> str:
    """Generate a deterministic UUID from a seed string."""
    hash_bytes = hashlib.md5(seed.encode()).digest()
    return str(uuid.UUID(bytes=hash_bytes))


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_csv_if_exists(path: Path) -> Optional[pd.DataFrame]:
    """Load a CSV file if it exists, otherwise return None."""
    if path.exists():
        return pd.read_csv(path)
    return None


def load_synthetic_tables() -> dict[str, Optional[pd.DataFrame]]:
    """Load all synthetic Silver tables."""
    tables = {}
    for filename in SYNTHETIC_TABLES:
        name = filename.replace(".csv", "")
        tables[name] = load_csv_if_exists(SILVER_DIR / filename)
    return tables


def load_bdc_tables() -> dict[str, Optional[pd.DataFrame]]:
    """Load all BDC parsed output tables."""
    return {
        "holdings": load_csv_if_exists(SILVER_DIR / BDC_TABLES["holdings"]),
        "funds": load_csv_if_exists(SILVER_DIR / BDC_TABLES["funds"]),
        "reports": load_csv_if_exists(SILVER_DIR / BDC_TABLES["reports"]),
    }


# ---------------------------------------------------------------------------
# Merging Logic
# ---------------------------------------------------------------------------

def add_source_column(df: Optional[pd.DataFrame], source: str) -> Optional[pd.DataFrame]:
    """Add source column to dataframe if not already present."""
    if df is None:
        return None
    df = df.copy()
    if "source" not in df.columns:
        df["source"] = source
    return df


def merge_funds(
    synthetic_funds: Optional[pd.DataFrame],
    bdc_funds: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Merge synthetic and BDC funds, avoiding duplicates by fund_id."""
    synthetic_funds = add_source_column(synthetic_funds, SOURCE_SYNTHETIC)
    bdc_funds = add_source_column(bdc_funds, SOURCE_BDC)

    dfs = [df for df in [synthetic_funds, bdc_funds] if df is not None]
    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    # Deduplicate by fund_id, keeping first occurrence
    merged = merged.drop_duplicates(subset=["fund_id"], keep="first")
    return merged


def merge_fund_reports(
    synthetic_reports: Optional[pd.DataFrame],
    bdc_reports: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Merge synthetic and BDC fund reports, avoiding duplicates by fund_report_id."""
    synthetic_reports = add_source_column(synthetic_reports, SOURCE_SYNTHETIC)
    bdc_reports = add_source_column(bdc_reports, SOURCE_BDC)

    dfs = [df for df in [synthetic_reports, bdc_reports] if df is not None]
    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    # Deduplicate by fund_report_id
    merged = merged.drop_duplicates(subset=["fund_report_id"], keep="first")
    return merged


def create_company_entries_for_bdc(
    bdc_holdings: pd.DataFrame,
    existing_companies: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Create new dim_company entries for BDC holdings not already in dim_company."""
    if bdc_holdings.empty:
        return pd.DataFrame()

    # Get unique raw_company_names from BDC holdings
    bdc_company_names = bdc_holdings["raw_company_name"].dropna().unique()

    # Get existing company names (case-insensitive matching)
    existing_names = set()
    if existing_companies is not None and not existing_companies.empty:
        existing_names = set(
            existing_companies["company_name"].str.lower().dropna().tolist()
        )

    # Create entries for new companies
    new_companies = []
    for raw_name in bdc_company_names:
        if raw_name.lower() not in existing_names:
            company_id = make_uuid(f"bdc_company_{raw_name}")
            new_companies.append({
                "company_id": company_id,
                "company_name": raw_name,
                "primary_sector": None,  # Will be inferred from reported_sector if available
                "primary_industry": None,
                "primary_country": None,
                "industry_taxonomy_node_id": None,
                "country_taxonomy_node_id": None,
                "website": None,
                "created_at": pd.Timestamp.now().date().isoformat(),
                "source": SOURCE_BDC,
            })
            existing_names.add(raw_name.lower())

    return pd.DataFrame(new_companies)


def merge_companies(
    synthetic_companies: Optional[pd.DataFrame],
    bdc_holdings: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Merge synthetic companies with new BDC company entries."""
    synthetic_companies = add_source_column(synthetic_companies, SOURCE_SYNTHETIC)

    new_bdc_companies = pd.DataFrame()
    if bdc_holdings is not None and not bdc_holdings.empty:
        new_bdc_companies = create_company_entries_for_bdc(bdc_holdings, synthetic_companies)

    dfs = [df for df in [synthetic_companies, new_bdc_companies] if df is not None and not df.empty]
    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    # Deduplicate by company_id
    merged = merged.drop_duplicates(subset=["company_id"], keep="first")
    return merged


def merge_holdings(
    synthetic_holdings: Optional[pd.DataFrame],
    bdc_holdings: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Merge synthetic and BDC holdings, avoiding duplicates by reported_holding_id."""
    synthetic_holdings = add_source_column(synthetic_holdings, SOURCE_SYNTHETIC)
    bdc_holdings = add_source_column(bdc_holdings, SOURCE_BDC)

    dfs = [df for df in [synthetic_holdings, bdc_holdings] if df is not None]
    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    # Deduplicate by reported_holding_id
    merged = merged.drop_duplicates(subset=["reported_holding_id"], keep="first")
    return merged


def extract_bdc_sectors(bdc_holdings: Optional[pd.DataFrame]) -> list[str]:
    """Extract unique sector/industry names from BDC holdings reported_sector field."""
    if bdc_holdings is None or bdc_holdings.empty:
        return []

    sectors = bdc_holdings["reported_sector"].dropna().unique().tolist()
    return [s for s in sectors if s and isinstance(s, str) and len(s.strip()) > 0]


def add_taxonomy_nodes_for_bdc_sectors(
    existing_nodes: Optional[pd.DataFrame],
    bdc_sectors: list[str],
    taxonomy_version: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Add new taxonomy nodes for BDC sectors not already in dim_taxonomy_node."""
    if not bdc_sectors:
        return existing_nodes if existing_nodes is not None else pd.DataFrame()

    # Get taxonomy version ID
    version_id = None
    if taxonomy_version is not None and not taxonomy_version.empty:
        version_id = taxonomy_version.iloc[0]["taxonomy_version_id"]
    else:
        version_id = make_uuid("taxonomy_v1")

    # Get existing node names
    existing_names = set()
    if existing_nodes is not None and not existing_nodes.empty:
        existing_names = set(existing_nodes["node_name"].str.lower().dropna().tolist())

    # Create new nodes for BDC sectors/industries
    new_nodes = []
    for sector_name in bdc_sectors:
        if sector_name.lower() not in existing_names:
            node_id = make_uuid(f"bdc_sector_{sector_name}")
            new_nodes.append({
                "taxonomy_node_id": node_id,
                "taxonomy_version_id": version_id,
                "taxonomy_type": "industry",  # BDC reported_sector is typically industry-level
                "node_name": sector_name,
                "parent_node_id": None,  # Unknown parent sector
                "path": f"/BDC/{sector_name}",
                "level": 2,
                "source": SOURCE_BDC,
            })
            existing_names.add(sector_name.lower())

    if not new_nodes:
        return existing_nodes if existing_nodes is not None else pd.DataFrame()

    new_nodes_df = pd.DataFrame(new_nodes)

    if existing_nodes is not None and not existing_nodes.empty:
        # Add source column to existing if not present
        if "source" not in existing_nodes.columns:
            existing_nodes = existing_nodes.copy()
            existing_nodes["source"] = SOURCE_SYNTHETIC
        merged = pd.concat([existing_nodes, new_nodes_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["taxonomy_node_id"], keep="first")
        return merged

    return new_nodes_df


def add_bdc_funds_to_portfolio(
    portfolio: Optional[pd.DataFrame],
    bdc_funds: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Add BDC funds to the portfolio.

    Note: dim_portfolio doesn't directly list funds - funds reference portfolios.
    This function returns the portfolio unchanged but could be extended to create
    a portfolio_fund junction table if needed.
    """
    # In the current schema, funds aren't directly linked to portfolios in dim_portfolio
    # The relationship is implicit via fact_inferred_exposure
    # Return portfolio as-is
    if portfolio is not None:
        return portfolio
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Main Merge Function
# ---------------------------------------------------------------------------

def load_and_merge_sources() -> dict[str, pd.DataFrame]:
    """Load all data sources and merge them into unified Silver tables."""
    print("Loading data sources...")

    # Load synthetic tables
    synthetic = load_synthetic_tables()
    synthetic_counts = {
        name: len(df) if df is not None else 0
        for name, df in synthetic.items()
    }

    # Load BDC tables
    bdc = load_bdc_tables()
    bdc_counts = {
        name: len(df) if df is not None else 0
        for name, df in bdc.items()
    }

    print(f"  Synthetic tables: {sum(1 for v in synthetic.values() if v is not None)} loaded")
    print(f"  BDC tables: {sum(1 for v in bdc.values() if v is not None)} loaded")

    # Merge funds
    print("\nMerging funds...")
    merged_funds = merge_funds(synthetic["dim_fund"], bdc["funds"])

    # Merge fund reports
    print("Merging fund reports...")
    merged_reports = merge_fund_reports(synthetic["fact_fund_report"], bdc["reports"])

    # Merge companies (create new entries for BDC companies)
    print("Merging companies...")
    merged_companies = merge_companies(synthetic["dim_company"], bdc["holdings"])

    # Merge holdings
    print("Merging holdings...")
    merged_holdings = merge_holdings(synthetic["fact_reported_holding"], bdc["holdings"])

    # Add taxonomy nodes for BDC sectors
    print("Adding taxonomy nodes for BDC sectors...")
    bdc_sectors = extract_bdc_sectors(bdc["holdings"])
    merged_taxonomy = add_taxonomy_nodes_for_bdc_sectors(
        synthetic["dim_taxonomy_node"],
        bdc_sectors,
        synthetic["meta_taxonomy_version"],
    )

    # Portfolio and other tables pass through with source column
    merged_portfolio = add_bdc_funds_to_portfolio(
        add_source_column(synthetic["dim_portfolio"], SOURCE_SYNTHETIC),
        bdc["funds"],
    )
    merged_aliases = add_source_column(synthetic["dim_entity_alias"], SOURCE_SYNTHETIC)
    merged_taxonomy_version = add_source_column(synthetic["meta_taxonomy_version"], SOURCE_SYNTHETIC)

    # Prepare output
    merged = {
        "dim_fund": merged_funds,
        "fact_fund_report": merged_reports,
        "dim_company": merged_companies,
        "fact_reported_holding": merged_holdings,
        "dim_taxonomy_node": merged_taxonomy,
        "dim_portfolio": merged_portfolio,
        "dim_entity_alias": merged_aliases if merged_aliases is not None else pd.DataFrame(),
        "meta_taxonomy_version": merged_taxonomy_version if merged_taxonomy_version is not None else pd.DataFrame(),
    }

    # Calculate merge statistics
    stats = {
        "synthetic": synthetic_counts,
        "bdc": bdc_counts,
        "merged": {name: len(df) if df is not None and not df.empty else 0 for name, df in merged.items()},
    }

    return merged, stats


def write_merged_tables(merged: dict[str, pd.DataFrame]) -> None:
    """Write merged tables back to Silver directory."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    for name, df in merged.items():
        if df is not None and not df.empty:
            path = SILVER_DIR / f"{name}.csv"
            df.to_csv(path, index=False)
            print(f"  Wrote {len(df)} rows to {path}")


def print_summary(stats: dict) -> None:
    """Print summary of merge operation."""
    print("\n" + "=" * 60)
    print("DATA SOURCE MERGE SUMMARY")
    print("=" * 60)

    print("\nSource Counts:")
    print("  Synthetic:")
    for name, count in stats["synthetic"].items():
        if count > 0:
            print(f"    {name}: {count} rows")

    print("  BDC:")
    for name, count in stats["bdc"].items():
        if count > 0:
            print(f"    {name}: {count} rows")

    print("\nMerged Counts:")
    for name, count in stats["merged"].items():
        if count > 0:
            print(f"  {name}: {count} rows")

    # Calculate totals
    total_synthetic_holdings = stats["synthetic"].get("fact_reported_holding", 0)
    total_bdc_holdings = stats["bdc"].get("holdings", 0)
    total_merged_holdings = stats["merged"].get("fact_reported_holding", 0)

    total_synthetic_funds = stats["synthetic"].get("dim_fund", 0)
    total_bdc_funds = stats["bdc"].get("funds", 0)
    total_merged_funds = stats["merged"].get("dim_fund", 0)

    print("\nTotals:")
    print(f"  Holdings: {total_synthetic_holdings} (synthetic) + {total_bdc_holdings} (BDC) = {total_merged_holdings} (merged)")
    print(f"  Funds: {total_synthetic_funds} (synthetic) + {total_bdc_funds} (BDC) = {total_merged_funds} (merged)")

    print("\n" + "=" * 60)


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    """Main entry point for the data source loader."""
    print("=" * 60)
    print("Unified Data Source Loader")
    print("=" * 60)

    # Load and merge all sources
    merged, stats = load_and_merge_sources()

    # Write merged tables
    print("\nWriting merged tables to Silver layer...")
    write_merged_tables(merged)

    # Print summary
    print_summary(stats)

    print("\nDone.")


if __name__ == "__main__":
    main()
