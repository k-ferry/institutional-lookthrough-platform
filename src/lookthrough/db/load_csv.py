"""Load CSV files from data/silver/ and data/gold/ into the database.

Bulk inserts all existing CSV files into their corresponding database tables.
Runnable as: python -m src.lookthrough.db.load_csv
"""

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from .engine import get_engine, get_session_context, init_db
from .models import (
    Base,
    DimCompany,
    DimEntityAlias,
    DimFund,
    DimPortfolio,
    DimTaxonomyNode,
    EntityResolutionLog,
    FactAggregationSnapshot,
    FactAuditEvent,
    FactExposureClassification,
    FactFundReport,
    FactInferredExposure,
    FactReportedHolding,
    FactReviewQueueItem,
    GICSMapping,
    MetaTaxonomyVersion,
)

# Mapping of CSV filenames to their ORM models
CSV_MODEL_MAPPING: dict[str, type[Base]] = {
    # Silver tables
    "dim_portfolio.csv": DimPortfolio,
    "dim_fund.csv": DimFund,
    "bdc_dim_fund.csv": DimFund,  # BDC funds go to same table
    "dim_company.csv": DimCompany,
    "dim_entity_alias.csv": DimEntityAlias,
    "dim_taxonomy_node.csv": DimTaxonomyNode,
    "meta_taxonomy_version.csv": MetaTaxonomyVersion,
    "fact_fund_report.csv": FactFundReport,
    "bdc_fact_fund_report.csv": FactFundReport,  # BDC reports go to same table
    "fact_reported_holding.csv": FactReportedHolding,
    "bdc_fact_reported_holding.csv": FactReportedHolding,  # BDC holdings go to same table
    # Gold tables
    "fact_inferred_exposure.csv": FactInferredExposure,
    "fact_exposure_classification.csv": FactExposureClassification,
    "fact_aggregation_snapshot.csv": FactAggregationSnapshot,
    "fact_review_queue_item.csv": FactReviewQueueItem,
    "fact_audit_event.csv": FactAuditEvent,
    "entity_resolution_log.csv": EntityResolutionLog,
    "gics_mapping.csv": GICSMapping,
}

# Tables to truncate in dependency order (children first, then parents)
TRUNCATE_ORDER = [
    "fact_aggregation_snapshot",
    "fact_exposure_classification",
    "fact_inferred_exposure",
    "fact_review_queue_item",
    "fact_audit_event",
    "entity_resolution_log",
    "gics_mapping",
    "fact_reported_holding",
    "fact_fund_report",
    "dim_entity_alias",
    "dim_company",
    "dim_taxonomy_node",
    "meta_taxonomy_version",
    "dim_fund",
    "dim_portfolio",
]


def get_project_root() -> Path:
    """Get the project root directory."""
    # Walk up from this file to find the project root (where data/ exists)
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "data").exists():
            return parent
    raise RuntimeError("Could not find project root with data/ directory")


def delete_all_table_data() -> None:
    """Delete all data from tables in the correct order to handle foreign key constraints.

    Uses DELETE FROM instead of TRUNCATE for better compatibility and reliability.
    """
    engine = get_engine()

    with engine.connect() as conn:
        # Disable FK checks temporarily for PostgreSQL
        try:
            conn.execute(text("SET session_replication_role = 'replica';"))
        except Exception:
            pass  # May not be supported

        for table_name in TRUNCATE_ORDER:
            try:
                # Use DELETE instead of TRUNCATE - more reliable
                conn.execute(text(f'DELETE FROM "{table_name}";'))
                print(f"  Deleted all rows from {table_name}")
            except Exception as e:
                # Table might not exist yet
                print(f"  Warning: Could not delete from {table_name}: {e}")

        # Re-enable FK checks
        try:
            conn.execute(text("SET session_replication_role = 'origin';"))
        except Exception:
            pass
        conn.commit()

    print("Cleared all existing table data")


def load_csv_to_table(
    csv_path: Path, model: type[Base], seen_pks: set | None = None
) -> tuple[int, set]:
    """Load a single CSV file into its corresponding table.

    Args:
        csv_path: Path to the CSV file
        model: SQLAlchemy ORM model class
        seen_pks: Set of primary keys already inserted (for cross-file deduplication)

    Returns:
        Tuple of (number of rows loaded, updated seen_pks set)
    """
    if seen_pks is None:
        seen_pks = set()

    if not csv_path.exists():
        return 0, seen_pks

    # Read CSV
    df = pd.read_csv(csv_path)

    if df.empty:
        return 0, seen_pks

    # Deduplicate by first column (primary key), keeping first occurrence
    pk_column = df.columns[0]
    df = df.drop_duplicates(subset=[pk_column], keep="first")

    # Filter out rows with PKs we've already seen from other CSVs
    original_len = len(df)
    df = df[~df[pk_column].isin(seen_pks)]
    if len(df) < original_len:
        print(f"    Skipped {original_len - len(df)} duplicate PKs already loaded")

    if df.empty:
        return 0, seen_pks

    # Track these PKs as seen
    new_pks = set(df[pk_column].tolist())
    seen_pks = seen_pks | new_pks

    # Get the model's column names and their nullability
    model_columns = {col.name for col in model.__table__.columns}
    # Required columns = non-nullable columns AND primary key columns
    required_columns = {
        col.name for col in model.__table__.columns if not col.nullable or col.primary_key
    }

    # Filter DataFrame to only include columns that exist in the model
    available_columns = [col for col in df.columns if col in model_columns]
    df = df[available_columns]

    # Drop rows with NULL in required (non-nullable) columns
    required_in_csv = [col for col in required_columns if col in df.columns]
    if required_in_csv:
        before_drop = len(df)
        df = df.dropna(subset=required_in_csv)
        dropped = before_drop - len(df)
        if dropped > 0:
            print(f"    Dropped {dropped} rows with NULL in required columns: {required_in_csv}")

    if df.empty:
        return 0, seen_pks

    # Convert to list of dicts
    records = df.to_dict(orient="records")

    # Replace NaN/NaT with None for PostgreSQL NULL handling
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    # Bulk insert using session
    with get_session_context() as session:
        session.bulk_insert_mappings(model, records)

    return len(records), seen_pks


def load_all_csvs_to_db() -> dict[str, int]:
    """Load all CSV files from data/silver/ and data/gold/ into the database.

    Returns:
        Dictionary mapping table names to row counts loaded
    """
    project_root = get_project_root()
    silver_dir = project_root / "data" / "silver"
    gold_dir = project_root / "data" / "gold"

    # Initialize database (create tables if they don't exist)
    print("Initializing database...")
    init_db()

    # Delete existing data (more reliable than TRUNCATE)
    print("\nClearing existing data...")
    delete_all_table_data()

    # Track results and seen primary keys per table (for cross-file deduplication)
    results: dict[str, int] = {}
    seen_pks_by_table: dict[str, set] = {}

    # Process silver files
    print("\nLoading Silver tables:")
    for csv_filename, model in CSV_MODEL_MAPPING.items():
        csv_path = silver_dir / csv_filename
        if csv_path.exists():
            table_name = model.__tablename__
            seen_pks = seen_pks_by_table.get(table_name, set())
            rows, seen_pks = load_csv_to_table(csv_path, model, seen_pks)
            seen_pks_by_table[table_name] = seen_pks
            results[f"{csv_filename}"] = rows
            print(f"  {csv_filename} -> {table_name}: {rows} rows")

    # Process gold files
    print("\nLoading Gold tables:")
    for csv_filename, model in CSV_MODEL_MAPPING.items():
        csv_path = gold_dir / csv_filename
        if csv_path.exists():
            table_name = model.__tablename__
            seen_pks = seen_pks_by_table.get(table_name, set())
            rows, seen_pks = load_csv_to_table(csv_path, model, seen_pks)
            seen_pks_by_table[table_name] = seen_pks
            results[f"{csv_filename}"] = rows
            print(f"  {csv_filename} -> {table_name}: {rows} rows")

    # Print summary
    print("\n" + "=" * 50)
    print("LOAD SUMMARY")
    print("=" * 50)

    total_rows = sum(results.values())
    for filename, rows in sorted(results.items()):
        if rows > 0:
            print(f"  {filename}: {rows:,} rows")

    print(f"\nTotal: {total_rows:,} rows loaded across {len([r for r in results.values() if r > 0])} files")

    return results


def main() -> None:
    """Main entry point for running as a module."""
    print("=" * 50)
    print("CSV to PostgreSQL Loader")
    print("=" * 50)

    database_url = os.environ.get("DATABASE_URL", "postgresql://lookthrough:lookthrough@localhost:5432/lookthrough")
    print(f"Database: {database_url}")
    print()

    try:
        load_all_csvs_to_db()
        print("\nLoad completed successfully!")
    except Exception as e:
        print(f"\nError during load: {e}")
        raise


if __name__ == "__main__":
    main()
