"""Repository pattern — centralized database read/write functions.

This module provides a data access layer that abstracts SQLAlchemy internals,
allowing pipeline modules to read/write data without knowing about ORM details.

Usage:
    from src.lookthrough.db.repository import get_all, upsert_rows, bulk_insert

    # Read entire table as DataFrame
    df = get_all(DimCompany)

    # Upsert rows by key columns
    count = upsert_rows(DimCompany, records, ['company_id'])

    # Fast bulk insert (no upsert logic)
    count = bulk_insert(FactReportedHolding, records)
"""
from __future__ import annotations

import os
from typing import Any, Type

import pandas as pd
from sqlalchemy import delete, inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from .engine import get_session_context, init_db
from .models import Base


def _is_csv_mode() -> bool:
    """Check if CSV mode is enabled via environment variable or --csv flag."""
    return os.environ.get("CSV_MODE", "").lower() in ("1", "true", "yes")


def get_all(model: Type[Base]) -> pd.DataFrame:
    """
    Read entire table as a pandas DataFrame.

    Args:
        model: SQLAlchemy ORM model class (e.g., DimCompany)

    Returns:
        DataFrame with all rows from the table
    """
    with get_session_context() as session:
        query = session.query(model)
        # Get column names from the model
        mapper = inspect(model)
        columns = [col.key for col in mapper.columns]

        rows = query.all()
        if not rows:
            return pd.DataFrame(columns=columns)

        # Convert ORM objects to dicts
        data = []
        for row in rows:
            row_dict = {col: getattr(row, col) for col in columns}
            data.append(row_dict)

        return pd.DataFrame(data)


def get_filtered(model: Type[Base], filters: dict[str, Any]) -> pd.DataFrame:
    """
    Read rows matching column filters as a pandas DataFrame.

    Args:
        model: SQLAlchemy ORM model class
        filters: Dictionary of column_name -> value to filter by

    Returns:
        DataFrame with matching rows

    Example:
        df = get_filtered(DimFund, {'fund_type': 'private'})
    """
    with get_session_context() as session:
        query = session.query(model)

        for column_name, value in filters.items():
            column = getattr(model, column_name, None)
            if column is not None:
                query = query.filter(column == value)

        mapper = inspect(model)
        columns = [col.key for col in mapper.columns]

        rows = query.all()
        if not rows:
            return pd.DataFrame(columns=columns)

        data = []
        for row in rows:
            row_dict = {col: getattr(row, col) for col in columns}
            data.append(row_dict)

        return pd.DataFrame(data)


def upsert_rows(
    model: Type[Base],
    records: list[dict[str, Any]],
    key_columns: list[str],
) -> int:
    """
    Insert or update rows by key columns (PostgreSQL upsert).

    Uses PostgreSQL's ON CONFLICT ... DO UPDATE to efficiently handle
    both inserts and updates in a single statement.

    Args:
        model: SQLAlchemy ORM model class
        records: List of dicts with column values
        key_columns: List of column names that form the unique key

    Returns:
        Number of rows affected

    Example:
        count = upsert_rows(
            DimCompany,
            [{'company_id': '123', 'company_name': 'Acme Corp'}],
            ['company_id']
        )
    """
    if not records:
        return 0

    with get_session_context() as session:
        # Get all column names except the keys for the update set
        mapper = inspect(model)
        all_columns = [col.key for col in mapper.columns]
        update_columns = [c for c in all_columns if c not in key_columns]

        # Build the insert statement with ON CONFLICT DO UPDATE
        stmt = insert(model).values(records)

        # Create update dict for non-key columns
        update_dict = {col: stmt.excluded[col] for col in update_columns}

        if update_dict:
            stmt = stmt.on_conflict_do_update(
                index_elements=key_columns,
                set_=update_dict,
            )
        else:
            # If no update columns, just ignore conflicts
            stmt = stmt.on_conflict_do_nothing(index_elements=key_columns)

        result = session.execute(stmt)
        return result.rowcount if hasattr(result, 'rowcount') else len(records)


def bulk_insert(model: Type[Base], records: list[dict[str, Any]]) -> int:
    """
    Fast bulk insert without upsert logic.

    Use this when you know the records don't already exist, or when you
    want to fail on duplicates. More efficient than upsert for large inserts.

    Args:
        model: SQLAlchemy ORM model class
        records: List of dicts with column values

    Returns:
        Number of rows inserted
    """
    if not records:
        return 0

    with get_session_context() as session:
        # Use bulk_insert_mappings for efficiency
        session.bulk_insert_mappings(model, records)
        return len(records)


def delete_all(model: Type[Base]) -> int:
    """
    Clear all rows from a table.

    Args:
        model: SQLAlchemy ORM model class

    Returns:
        Number of rows deleted
    """
    with get_session_context() as session:
        stmt = delete(model)
        result = session.execute(stmt)
        return result.rowcount


def execute_query(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Execute raw SQL and return results as a DataFrame.

    Use this for complex queries that can't be expressed with the ORM,
    such as joins, aggregations, or window functions.

    Args:
        sql: Raw SQL query string
        params: Optional dict of named parameters for the query

    Returns:
        DataFrame with query results

    Example:
        df = execute_query(
            "SELECT * FROM dim_company WHERE primary_sector = :sector",
            {'sector': 'Technology'}
        )
    """
    with get_session_context() as session:
        result = session.execute(text(sql), params or {})

        # Get column names from result
        columns = result.keys()
        rows = result.fetchall()

        if not rows:
            return pd.DataFrame(columns=list(columns))

        return pd.DataFrame(rows, columns=list(columns))


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Convert a DataFrame to a list of dicts suitable for upsert/insert.

    Handles NaN values by converting them to None.

    Args:
        df: pandas DataFrame

    Returns:
        List of dicts with column values
    """
    # Replace NaN with None for database compatibility
    df_clean = df.where(pd.notnull(df), None)
    return df_clean.to_dict('records')


def ensure_tables() -> None:
    """
    Ensure all database tables exist.

    Call this at the start of the pipeline to create any missing tables.
    """
    init_db()


# Re-export init_db for convenience
__all__ = [
    'get_all',
    'get_filtered',
    'upsert_rows',
    'bulk_insert',
    'delete_all',
    'execute_query',
    'dataframe_to_records',
    'ensure_tables',
    '_is_csv_mode',
]
