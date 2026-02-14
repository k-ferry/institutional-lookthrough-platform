"""Agent tool functions that query Gold tables and return structured results.

These tools are designed to be called by an AI agent via tool-calling. Each function
reads CSVs from data/gold/ and data/silver/, runs a pandas query, and returns a
dictionary with the results.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

# Stable placeholder for unknown/missing taxonomy classification
UNKNOWN_TAXONOMY_NODE_ID = "00000000-0000-0000-0000-000000000000"

# Confidence threshold for flagging weak data
CONFIDENCE_THRESHOLD = 0.70


def _repo_root() -> Path:
    """Return repository root (4 levels up from this file)."""
    return Path(__file__).resolve().parents[3]


def _read_csv(path: Path) -> pd.DataFrame:
    """Read CSV file, returning empty DataFrame if file doesn't exist."""
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _build_taxonomy_lookup(taxonomy_df: pd.DataFrame) -> dict:
    """Build lookup tables for taxonomy nodes."""
    if taxonomy_df.empty:
        return {"node_by_id": {}, "id_by_name": {}}

    node_by_id = {}
    id_by_name = {}

    for _, row in taxonomy_df.iterrows():
        node_id = str(row["taxonomy_node_id"])
        node_name = str(row.get("node_name", ""))
        taxonomy_type = str(row.get("taxonomy_type", ""))

        node_by_id[node_id] = {
            "node_name": node_name,
            "taxonomy_type": taxonomy_type,
            "parent_node_id": str(row.get("parent_node_id", "")) if pd.notna(row.get("parent_node_id")) else None,
            "level": int(row.get("level", 0)),
            "path": str(row.get("path", "")),
        }
        # Key by (taxonomy_type, node_name) for reverse lookup
        id_by_name[(taxonomy_type, node_name.lower())] = node_id

    return {"node_by_id": node_by_id, "id_by_name": id_by_name}


def get_sector_exposure(as_of_date: Optional[str] = None) -> dict:
    """
    Get portfolio exposure breakdown by sector.

    Returns sector-level exposure data including total exposure value,
    coverage percentage, and confidence-weighted exposure. Use this to
    understand the high-level sector allocation of the portfolio.

    Args:
        as_of_date: Optional date filter (YYYY-MM-DD). Uses most recent if not provided.

    Returns:
        Dictionary with:
        - as_of_date: The effective date of the snapshot
        - total_portfolio_value_usd: Total portfolio value
        - unknown_exposure_pct: Percentage of portfolio that is unclassified
        - sectors: List of sectors with exposure details
    """
    root = _repo_root()
    gold = root / "data" / "gold"
    silver = root / "data" / "silver"

    agg = _read_csv(gold / "fact_aggregation_snapshot.csv")
    taxonomy = _read_csv(silver / "dim_taxonomy_node.csv")

    if agg.empty:
        return {"error": "No aggregation data available", "sectors": []}

    # Filter to sector taxonomy type
    sector_data = agg[agg["taxonomy_type"] == "sector"].copy()
    if sector_data.empty:
        return {"error": "No sector data available", "sectors": []}

    # Filter by date
    if as_of_date:
        sector_data = sector_data[sector_data["as_of_date"] == as_of_date]
    else:
        # Use most recent date
        as_of_date = sector_data["as_of_date"].max()
        sector_data = sector_data[sector_data["as_of_date"] == as_of_date]

    if sector_data.empty:
        return {"error": f"No sector data for date {as_of_date}", "sectors": []}

    # Build taxonomy lookup
    taxonomy_lookup = _build_taxonomy_lookup(taxonomy)

    # Calculate totals
    total_exposure = sector_data["total_exposure_value_usd"].sum()
    unknown_row = sector_data[sector_data["taxonomy_node_id"] == UNKNOWN_TAXONOMY_NODE_ID]
    unknown_exposure = unknown_row["total_exposure_value_usd"].sum() if not unknown_row.empty else 0.0
    unknown_pct = (unknown_exposure / total_exposure * 100) if total_exposure > 0 else 0.0

    # Get coverage_pct (should be same for all rows in a taxonomy_type)
    coverage_pct = sector_data["coverage_pct"].iloc[0] if not sector_data.empty else 0.0

    # Build sector list (excluding unknown)
    known_sectors = sector_data[sector_data["taxonomy_node_id"] != UNKNOWN_TAXONOMY_NODE_ID]
    sectors = []
    for _, row in known_sectors.iterrows():
        node_id = str(row["taxonomy_node_id"])
        node_info = taxonomy_lookup["node_by_id"].get(node_id, {})
        sectors.append({
            "sector_name": node_info.get("node_name", "Unknown"),
            "taxonomy_node_id": node_id,
            "total_exposure_value_usd": float(row["total_exposure_value_usd"]),
            "exposure_pct": float(row["total_exposure_value_usd"] / total_exposure * 100) if total_exposure > 0 else 0.0,
            "confidence_weighted_exposure": float(row["confidence_weighted_exposure"]),
        })

    # Sort by exposure value descending
    sectors.sort(key=lambda x: x["total_exposure_value_usd"], reverse=True)

    return {
        "as_of_date": as_of_date,
        "total_portfolio_value_usd": float(total_exposure),
        "coverage_pct": float(coverage_pct * 100),
        "unknown_exposure_pct": float(unknown_pct),
        "sector_count": len(sectors),
        "sectors": sectors,
    }


def get_industry_exposure(sector: Optional[str] = None, as_of_date: Optional[str] = None) -> dict:
    """
    Get portfolio exposure breakdown by industry.

    Returns industry-level exposure data. Industries are sub-classifications
    within sectors. Use this for more granular analysis than sector exposure.

    Args:
        sector: Optional sector name to filter industries (e.g., "Technology").
        as_of_date: Optional date filter (YYYY-MM-DD). Uses most recent if not provided.

    Returns:
        Dictionary with:
        - as_of_date: The effective date of the snapshot
        - total_exposure_usd: Total exposure in filtered industries
        - unknown_exposure_pct: Percentage that is unclassified
        - industries: List of industries with exposure details
    """
    root = _repo_root()
    gold = root / "data" / "gold"
    silver = root / "data" / "silver"

    agg = _read_csv(gold / "fact_aggregation_snapshot.csv")
    taxonomy = _read_csv(silver / "dim_taxonomy_node.csv")

    if agg.empty:
        return {"error": "No aggregation data available", "industries": []}

    # Filter to industry taxonomy type
    industry_data = agg[agg["taxonomy_type"] == "industry"].copy()
    if industry_data.empty:
        return {"error": "No industry data available", "industries": []}

    # Filter by date
    if as_of_date:
        industry_data = industry_data[industry_data["as_of_date"] == as_of_date]
    else:
        as_of_date = industry_data["as_of_date"].max()
        industry_data = industry_data[industry_data["as_of_date"] == as_of_date]

    if industry_data.empty:
        return {"error": f"No industry data for date {as_of_date}", "industries": []}

    # Build taxonomy lookup
    taxonomy_lookup = _build_taxonomy_lookup(taxonomy)

    # If sector filter provided, find industries under that sector
    if sector:
        # Find the sector node ID
        sector_node_id = taxonomy_lookup["id_by_name"].get(("sector", sector.lower()))
        if not sector_node_id:
            return {"error": f"Sector '{sector}' not found", "industries": []}

        # Find industry nodes whose parent is this sector
        valid_industry_ids = set()
        for node_id, node_info in taxonomy_lookup["node_by_id"].items():
            if node_info.get("parent_node_id") == sector_node_id:
                valid_industry_ids.add(node_id)

        # Filter to these industries
        industry_data = industry_data[industry_data["taxonomy_node_id"].isin(valid_industry_ids)]

    # Calculate totals
    total_exposure = industry_data["total_exposure_value_usd"].sum()
    unknown_row = industry_data[industry_data["taxonomy_node_id"] == UNKNOWN_TAXONOMY_NODE_ID]
    unknown_exposure = unknown_row["total_exposure_value_usd"].sum() if not unknown_row.empty else 0.0
    unknown_pct = (unknown_exposure / total_exposure * 100) if total_exposure > 0 else 0.0

    coverage_pct = industry_data["coverage_pct"].iloc[0] if not industry_data.empty else 0.0

    # Build industry list (excluding unknown)
    known_industries = industry_data[industry_data["taxonomy_node_id"] != UNKNOWN_TAXONOMY_NODE_ID]
    industries = []
    for _, row in known_industries.iterrows():
        node_id = str(row["taxonomy_node_id"])
        node_info = taxonomy_lookup["node_by_id"].get(node_id, {})

        # Get parent sector name
        parent_id = node_info.get("parent_node_id")
        parent_info = taxonomy_lookup["node_by_id"].get(parent_id, {}) if parent_id else {}

        industries.append({
            "industry_name": node_info.get("node_name", "Unknown"),
            "sector_name": parent_info.get("node_name", "Unknown"),
            "taxonomy_node_id": node_id,
            "total_exposure_value_usd": float(row["total_exposure_value_usd"]),
            "exposure_pct": float(row["total_exposure_value_usd"] / total_exposure * 100) if total_exposure > 0 else 0.0,
            "confidence_weighted_exposure": float(row["confidence_weighted_exposure"]),
        })

    # Sort by exposure value descending
    industries.sort(key=lambda x: x["total_exposure_value_usd"], reverse=True)

    return {
        "as_of_date": as_of_date,
        "sector_filter": sector,
        "total_exposure_usd": float(total_exposure),
        "coverage_pct": float(coverage_pct * 100),
        "unknown_exposure_pct": float(unknown_pct),
        "industry_count": len(industries),
        "industries": industries,
    }


def get_geography_exposure(as_of_date: Optional[str] = None) -> dict:
    """
    Get portfolio exposure breakdown by geography (country).

    Returns geographic distribution of portfolio exposure. Use this to
    understand country-level concentration and international diversification.

    Args:
        as_of_date: Optional date filter (YYYY-MM-DD). Uses most recent if not provided.

    Returns:
        Dictionary with:
        - as_of_date: The effective date of the snapshot
        - total_portfolio_value_usd: Total portfolio value
        - unknown_exposure_pct: Percentage with unknown geography
        - countries: List of countries with exposure details
    """
    root = _repo_root()
    gold = root / "data" / "gold"
    silver = root / "data" / "silver"

    agg = _read_csv(gold / "fact_aggregation_snapshot.csv")
    taxonomy = _read_csv(silver / "dim_taxonomy_node.csv")

    if agg.empty:
        return {"error": "No aggregation data available", "countries": []}

    # Filter to geography taxonomy type
    geo_data = agg[agg["taxonomy_type"] == "geography"].copy()
    if geo_data.empty:
        return {"error": "No geography data available", "countries": []}

    # Filter by date
    if as_of_date:
        geo_data = geo_data[geo_data["as_of_date"] == as_of_date]
    else:
        as_of_date = geo_data["as_of_date"].max()
        geo_data = geo_data[geo_data["as_of_date"] == as_of_date]

    if geo_data.empty:
        return {"error": f"No geography data for date {as_of_date}", "countries": []}

    # Build taxonomy lookup
    taxonomy_lookup = _build_taxonomy_lookup(taxonomy)

    # Calculate totals
    total_exposure = geo_data["total_exposure_value_usd"].sum()
    unknown_row = geo_data[geo_data["taxonomy_node_id"] == UNKNOWN_TAXONOMY_NODE_ID]
    unknown_exposure = unknown_row["total_exposure_value_usd"].sum() if not unknown_row.empty else 0.0
    unknown_pct = (unknown_exposure / total_exposure * 100) if total_exposure > 0 else 0.0

    coverage_pct = geo_data["coverage_pct"].iloc[0] if not geo_data.empty else 0.0

    # Build country list (excluding unknown, level 2 = countries)
    known_geo = geo_data[geo_data["taxonomy_node_id"] != UNKNOWN_TAXONOMY_NODE_ID]
    countries = []
    for _, row in known_geo.iterrows():
        node_id = str(row["taxonomy_node_id"])
        node_info = taxonomy_lookup["node_by_id"].get(node_id, {})

        # Only include level 2 nodes (countries)
        if node_info.get("level") != 2:
            continue

        # Get parent region name
        parent_id = node_info.get("parent_node_id")
        parent_info = taxonomy_lookup["node_by_id"].get(parent_id, {}) if parent_id else {}

        countries.append({
            "country_name": node_info.get("node_name", "Unknown"),
            "region_name": parent_info.get("node_name", "Unknown"),
            "taxonomy_node_id": node_id,
            "total_exposure_value_usd": float(row["total_exposure_value_usd"]),
            "exposure_pct": float(row["total_exposure_value_usd"] / total_exposure * 100) if total_exposure > 0 else 0.0,
            "confidence_weighted_exposure": float(row["confidence_weighted_exposure"]),
        })

    # Sort by exposure value descending
    countries.sort(key=lambda x: x["total_exposure_value_usd"], reverse=True)

    return {
        "as_of_date": as_of_date,
        "total_portfolio_value_usd": float(total_exposure),
        "coverage_pct": float(coverage_pct * 100),
        "unknown_exposure_pct": float(unknown_pct),
        "country_count": len(countries),
        "countries": countries,
    }


def get_fund_exposure(fund_name: Optional[str] = None) -> dict:
    """
    Get portfolio exposure breakdown by fund.

    Returns exposure totals for each fund in the portfolio, including
    the percentage of unknown/unclassified holdings per fund.

    Args:
        fund_name: Optional fund name to filter (case-insensitive partial match).

    Returns:
        Dictionary with:
        - total_portfolio_value_usd: Total portfolio value
        - fund_count: Number of funds
        - funds: List of funds with exposure details and unknown percentages
    """
    root = _repo_root()
    gold = root / "data" / "gold"
    silver = root / "data" / "silver"

    exposures = _read_csv(gold / "fact_inferred_exposure.csv")
    funds = _read_csv(silver / "dim_fund.csv")

    if exposures.empty:
        return {"error": "No exposure data available", "funds": []}

    # Build fund lookup
    fund_lookup = {}
    if not funds.empty:
        for _, row in funds.iterrows():
            fund_lookup[str(row["fund_id"])] = {
                "fund_name": str(row.get("fund_name", "")),
                "manager_name": str(row.get("manager_name", "")),
                "strategy": str(row.get("strategy", "")),
                "fund_type": str(row.get("fund_type", "")),
            }

    # Aggregate by fund
    fund_agg = exposures.groupby("fund_id").agg({
        "exposure_value_usd": "sum",
        "company_id": "count",  # Count of holdings
    }).reset_index()

    # Calculate unknown exposure per fund (where company_id is null)
    unknown_by_fund = exposures[exposures["company_id"].isna()].groupby("fund_id").agg({
        "exposure_value_usd": "sum"
    }).reset_index()
    unknown_by_fund.columns = ["fund_id", "unknown_exposure"]
    fund_agg = fund_agg.merge(unknown_by_fund, on="fund_id", how="left")
    fund_agg["unknown_exposure"] = fund_agg["unknown_exposure"].fillna(0)

    total_portfolio = fund_agg["exposure_value_usd"].sum()

    # Build fund list
    fund_list = []
    for _, row in fund_agg.iterrows():
        fund_id = str(row["fund_id"])
        fund_info = fund_lookup.get(fund_id, {})
        fund_name_actual = fund_info.get("fund_name", fund_id)

        # Apply filter if provided
        if fund_name and fund_name.lower() not in fund_name_actual.lower():
            continue

        unknown_pct = (row["unknown_exposure"] / row["exposure_value_usd"] * 100) if row["exposure_value_usd"] > 0 else 0.0

        fund_list.append({
            "fund_id": fund_id,
            "fund_name": fund_name_actual,
            "manager_name": fund_info.get("manager_name", "Unknown"),
            "strategy": fund_info.get("strategy", "Unknown"),
            "fund_type": fund_info.get("fund_type", "Unknown"),
            "total_exposure_value_usd": float(row["exposure_value_usd"]),
            "exposure_pct": float(row["exposure_value_usd"] / total_portfolio * 100) if total_portfolio > 0 else 0.0,
            "holding_count": int(row["company_id"]),
            "unknown_exposure_pct": float(unknown_pct),
        })

    # Sort by exposure value descending
    fund_list.sort(key=lambda x: x["total_exposure_value_usd"], reverse=True)

    return {
        "total_portfolio_value_usd": float(total_portfolio),
        "fund_count": len(fund_list),
        "fund_name_filter": fund_name,
        "funds": fund_list,
    }


def get_company_exposure(company_name: Optional[str] = None, top_n: int = 20) -> dict:
    """
    Get portfolio exposure breakdown by company.

    Returns the top N companies by exposure value, with classification
    confidence scores when available. Use this to understand concentration
    in individual holdings.

    Args:
        company_name: Optional company name to search (case-insensitive partial match).
        top_n: Number of top companies to return (default 20).

    Returns:
        Dictionary with:
        - total_exposure_usd: Total exposure in returned companies
        - company_count: Number of companies returned
        - companies: List of companies with exposure and confidence details
    """
    root = _repo_root()
    gold = root / "data" / "gold"
    silver = root / "data" / "silver"

    exposures = _read_csv(gold / "fact_inferred_exposure.csv")
    companies = _read_csv(silver / "dim_company.csv")
    classifications = _read_csv(gold / "fact_exposure_classification.csv")

    if exposures.empty:
        return {"error": "No exposure data available", "companies": []}

    # Build company lookup
    company_lookup = {}
    if not companies.empty:
        for _, row in companies.iterrows():
            company_lookup[str(row["company_id"])] = {
                "company_name": str(row.get("company_name", "")),
                "primary_sector": str(row.get("primary_sector", "")) if pd.notna(row.get("primary_sector")) else None,
                "primary_industry": str(row.get("primary_industry", "")) if pd.notna(row.get("primary_industry")) else None,
                "primary_country": str(row.get("primary_country", "")) if pd.notna(row.get("primary_country")) else None,
            }

    # Build classification confidence lookup
    confidence_lookup = {}
    if not classifications.empty:
        # Get industry classification confidence per company
        industry_class = classifications[classifications["taxonomy_type"] == "industry"]
        for _, row in industry_class.iterrows():
            company_id = str(row["company_id"]) if pd.notna(row.get("company_id")) else None
            if company_id:
                confidence_lookup[company_id] = float(row.get("confidence", 0.0))

    # Aggregate by company
    company_agg = exposures.groupby(["company_id", "raw_company_name"]).agg({
        "exposure_value_usd": "sum",
        "fund_id": "nunique",  # Number of funds holding this company
    }).reset_index()
    company_agg.columns = ["company_id", "raw_company_name", "total_exposure", "fund_count"]

    # Apply company name filter if provided
    if company_name:
        mask = company_agg["raw_company_name"].str.lower().str.contains(company_name.lower(), na=False)
        company_agg = company_agg[mask]

    # Sort by exposure and take top N
    company_agg = company_agg.sort_values("total_exposure", ascending=False).head(top_n)

    total_exposure = company_agg["total_exposure"].sum()

    # Build company list
    company_list = []
    for _, row in company_agg.iterrows():
        company_id = str(row["company_id"]) if pd.notna(row["company_id"]) else None
        company_info = company_lookup.get(company_id, {}) if company_id else {}
        confidence = confidence_lookup.get(company_id) if company_id else None

        company_list.append({
            "company_id": company_id,
            "company_name": company_info.get("company_name") or str(row["raw_company_name"]),
            "raw_company_name": str(row["raw_company_name"]),
            "primary_sector": company_info.get("primary_sector"),
            "primary_industry": company_info.get("primary_industry"),
            "primary_country": company_info.get("primary_country"),
            "total_exposure_value_usd": float(row["total_exposure"]),
            "fund_count": int(row["fund_count"]),
            "classification_confidence": confidence,
        })

    return {
        "total_exposure_usd": float(total_exposure),
        "company_count": len(company_list),
        "company_name_filter": company_name,
        "top_n": top_n,
        "companies": company_list,
    }


def get_review_queue(status: str = "pending", priority: Optional[str] = None) -> dict:
    """
    Get items in the review queue requiring human attention.

    Returns review queue items filtered by status and priority. These are
    holdings or classifications that need manual review due to low confidence,
    unresolved entities, or other data quality issues.

    Args:
        status: Filter by status ("pending", "resolved", "dismissed"). Default "pending".
        priority: Optional priority filter ("high", "medium", "low").

    Returns:
        Dictionary with:
        - total_items: Total number of matching items
        - items_by_priority: Count of items by priority level
        - items_by_reason: Count of items by reason
        - items: List of review queue items with details
    """
    root = _repo_root()
    gold = root / "data" / "gold"

    queue = _read_csv(gold / "fact_review_queue_item.csv")

    if queue.empty:
        return {"total_items": 0, "items_by_priority": {}, "items_by_reason": {}, "items": []}

    # Apply filters
    filtered = queue[queue["status"] == status].copy()
    if priority:
        filtered = filtered[filtered["priority"] == priority]

    if filtered.empty:
        return {"total_items": 0, "items_by_priority": {}, "items_by_reason": {}, "items": []}

    # Calculate summary stats
    priority_counts = filtered["priority"].value_counts().to_dict()
    reason_counts = filtered["reason"].value_counts().to_dict()

    # Build item list
    items = []
    for _, row in filtered.iterrows():
        items.append({
            "queue_item_id": str(row["queue_item_id"]),
            "company_id": str(row["company_id"]) if pd.notna(row.get("company_id")) else None,
            "raw_company_name": str(row["raw_company_name"]) if pd.notna(row.get("raw_company_name")) else None,
            "reason": str(row["reason"]),
            "priority": str(row["priority"]),
            "status": str(row["status"]),
            "exposure_id": str(row["exposure_id"]) if pd.notna(row.get("exposure_id")) else None,
            "reported_holding_id": str(row["reported_holding_id"]) if pd.notna(row.get("reported_holding_id")) else None,
            "created_at": str(row["created_at"]),
        })

    # Sort by priority (high first)
    priority_order = {"high": 0, "medium": 1, "low": 2}
    items.sort(key=lambda x: priority_order.get(x["priority"], 3))

    return {
        "status_filter": status,
        "priority_filter": priority,
        "total_items": len(items),
        "items_by_priority": priority_counts,
        "items_by_reason": reason_counts,
        "items": items,
    }


def get_portfolio_summary(as_of_date: Optional[str] = None) -> dict:
    """
    Get a high-level summary of the portfolio.

    Returns key metrics including total value, number of funds and companies,
    overall coverage, confidence levels, pending review items, and top sectors.
    This is the default tool for general questions about portfolio status.

    Args:
        as_of_date: Optional date filter (YYYY-MM-DD). Uses most recent if not provided.

    Returns:
        Dictionary with:
        - as_of_date: Effective date of the summary
        - total_portfolio_value_usd: Total portfolio value
        - fund_count: Number of funds
        - company_count: Number of unique companies
        - coverage_pct: Percentage of portfolio with known classifications
        - avg_confidence: Average classification confidence
        - pending_review_count: Number of items needing review
        - top_sectors: Top 3 sectors by exposure
    """
    root = _repo_root()
    gold = root / "data" / "gold"
    silver = root / "data" / "silver"

    exposures = _read_csv(gold / "fact_inferred_exposure.csv")
    agg = _read_csv(gold / "fact_aggregation_snapshot.csv")
    queue = _read_csv(gold / "fact_review_queue_item.csv")
    classifications = _read_csv(gold / "fact_exposure_classification.csv")
    taxonomy = _read_csv(silver / "dim_taxonomy_node.csv")

    if exposures.empty:
        return {"error": "No exposure data available"}

    # Determine as_of_date
    if as_of_date:
        exposures_filtered = exposures[exposures["as_of_date"] == as_of_date]
    else:
        as_of_date = exposures["as_of_date"].max()
        exposures_filtered = exposures[exposures["as_of_date"] == as_of_date]

    if exposures_filtered.empty:
        return {"error": f"No exposure data for date {as_of_date}"}

    # Basic counts
    total_value = exposures_filtered["exposure_value_usd"].sum()
    fund_count = exposures_filtered["fund_id"].nunique()
    company_count = exposures_filtered["company_id"].nunique()

    # Coverage from aggregation snapshot
    coverage_pct = 0.0
    if not agg.empty:
        sector_data = agg[(agg["taxonomy_type"] == "sector") & (agg["as_of_date"] == as_of_date)]
        if not sector_data.empty:
            coverage_pct = sector_data["coverage_pct"].iloc[0] * 100

    # Average confidence from classifications
    avg_confidence = 0.0
    if not classifications.empty:
        avg_confidence = classifications["confidence"].mean() * 100

    # Pending review items
    pending_count = 0
    high_priority_count = 0
    if not queue.empty:
        pending = queue[queue["status"] == "pending"]
        pending_count = len(pending)
        high_priority_count = len(pending[pending["priority"] == "high"])

    # Top sectors
    top_sectors = []
    taxonomy_lookup = _build_taxonomy_lookup(taxonomy)
    if not agg.empty:
        sector_data = agg[(agg["taxonomy_type"] == "sector") & (agg["as_of_date"] == as_of_date)]
        known_sectors = sector_data[sector_data["taxonomy_node_id"] != UNKNOWN_TAXONOMY_NODE_ID]
        known_sectors = known_sectors.sort_values("total_exposure_value_usd", ascending=False).head(3)
        for _, row in known_sectors.iterrows():
            node_id = str(row["taxonomy_node_id"])
            node_info = taxonomy_lookup["node_by_id"].get(node_id, {})
            top_sectors.append({
                "sector_name": node_info.get("node_name", "Unknown"),
                "exposure_value_usd": float(row["total_exposure_value_usd"]),
                "exposure_pct": float(row["total_exposure_value_usd"] / total_value * 100) if total_value > 0 else 0.0,
            })

    return {
        "as_of_date": as_of_date,
        "total_portfolio_value_usd": float(total_value),
        "fund_count": int(fund_count),
        "company_count": int(company_count),
        "coverage_pct": float(coverage_pct),
        "avg_confidence_pct": float(avg_confidence),
        "pending_review_count": int(pending_count),
        "high_priority_review_count": int(high_priority_count),
        "top_sectors": top_sectors,
    }


def get_confidence_distribution(taxonomy_type: str = "sector") -> dict:
    """
    Get confidence statistics per taxonomy bucket.

    Returns confidence distribution metrics (mean, min, max) for each
    taxonomy node, along with the percentage of classifications below
    the confidence threshold (0.70). Use this to identify where data
    quality is weakest and needs attention.

    Args:
        taxonomy_type: Type of taxonomy to analyze ("sector", "industry", "geography").

    Returns:
        Dictionary with:
        - taxonomy_type: The taxonomy type analyzed
        - overall_stats: Overall confidence statistics
        - buckets: List of taxonomy buckets with confidence metrics
    """
    root = _repo_root()
    gold = root / "data" / "gold"
    silver = root / "data" / "silver"

    classifications = _read_csv(gold / "fact_exposure_classification.csv")
    taxonomy = _read_csv(silver / "dim_taxonomy_node.csv")

    if classifications.empty:
        return {"error": "No classification data available", "buckets": []}

    # Filter to requested taxonomy type
    filtered = classifications[classifications["taxonomy_type"] == taxonomy_type].copy()
    if filtered.empty:
        return {"error": f"No classifications for taxonomy type '{taxonomy_type}'", "buckets": []}

    # Build taxonomy lookup
    taxonomy_lookup = _build_taxonomy_lookup(taxonomy)

    # Overall stats
    overall_mean = filtered["confidence"].mean()
    overall_min = filtered["confidence"].min()
    overall_max = filtered["confidence"].max()
    overall_below_threshold = (filtered["confidence"] < CONFIDENCE_THRESHOLD).mean() * 100

    # Stats per taxonomy node
    bucket_stats = filtered.groupby("taxonomy_node_id").agg({
        "confidence": ["mean", "min", "max", "count"],
    }).reset_index()
    bucket_stats.columns = ["taxonomy_node_id", "mean_confidence", "min_confidence", "max_confidence", "count"]

    # Calculate pct below threshold per bucket
    below_threshold = filtered[filtered["confidence"] < CONFIDENCE_THRESHOLD].groupby("taxonomy_node_id").size()
    bucket_stats = bucket_stats.merge(
        below_threshold.reset_index(name="below_threshold_count"),
        on="taxonomy_node_id",
        how="left"
    )
    bucket_stats["below_threshold_count"] = bucket_stats["below_threshold_count"].fillna(0)
    bucket_stats["pct_below_threshold"] = bucket_stats["below_threshold_count"] / bucket_stats["count"] * 100

    # Build bucket list
    buckets = []
    for _, row in bucket_stats.iterrows():
        node_id = str(row["taxonomy_node_id"])
        node_info = taxonomy_lookup["node_by_id"].get(node_id, {})

        buckets.append({
            "taxonomy_node_id": node_id,
            "node_name": node_info.get("node_name", "Unknown"),
            "classification_count": int(row["count"]),
            "mean_confidence": float(row["mean_confidence"]),
            "min_confidence": float(row["min_confidence"]),
            "max_confidence": float(row["max_confidence"]),
            "pct_below_threshold": float(row["pct_below_threshold"]),
        })

    # Sort by pct below threshold descending (worst quality first)
    buckets.sort(key=lambda x: x["pct_below_threshold"], reverse=True)

    return {
        "taxonomy_type": taxonomy_type,
        "confidence_threshold": CONFIDENCE_THRESHOLD,
        "overall_stats": {
            "mean_confidence": float(overall_mean),
            "min_confidence": float(overall_min),
            "max_confidence": float(overall_max),
            "pct_below_threshold": float(overall_below_threshold),
            "total_classifications": len(filtered),
        },
        "bucket_count": len(buckets),
        "buckets": buckets,
    }


# ============================================================================
# TOOLS_REGISTRY
# ============================================================================

def _get_docstring(func) -> str:
    """Extract the first paragraph of a function's docstring."""
    doc = func.__doc__ or ""
    # Get first paragraph (up to first blank line)
    lines = []
    for line in doc.strip().split("\n"):
        if line.strip() == "":
            break
        lines.append(line.strip())
    return " ".join(lines)


TOOLS_REGISTRY: list[dict] = [
    {
        "name": "get_sector_exposure",
        "function": get_sector_exposure,
        "description": _get_docstring(get_sector_exposure),
    },
    {
        "name": "get_industry_exposure",
        "function": get_industry_exposure,
        "description": _get_docstring(get_industry_exposure),
    },
    {
        "name": "get_geography_exposure",
        "function": get_geography_exposure,
        "description": _get_docstring(get_geography_exposure),
    },
    {
        "name": "get_fund_exposure",
        "function": get_fund_exposure,
        "description": _get_docstring(get_fund_exposure),
    },
    {
        "name": "get_company_exposure",
        "function": get_company_exposure,
        "description": _get_docstring(get_company_exposure),
    },
    {
        "name": "get_review_queue",
        "function": get_review_queue,
        "description": _get_docstring(get_review_queue),
    },
    {
        "name": "get_portfolio_summary",
        "function": get_portfolio_summary,
        "description": _get_docstring(get_portfolio_summary),
    },
    {
        "name": "get_confidence_distribution",
        "function": get_confidence_distribution,
        "description": _get_docstring(get_confidence_distribution),
    },
]
