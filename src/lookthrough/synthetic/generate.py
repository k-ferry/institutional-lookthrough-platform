"""V1 Synthetic Data Generator for Institutional Look-Through Platform.

Generates realistic synthetic data with intentional imperfections:
- Inconsistent company naming (aliases)
- Missing fields (sector/country/value)
- Partial coverage (only top holdings reported)
- Conflicting classifications
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SECTORS_AND_INDUSTRIES: dict[str, list[str]] = {
    "Technology": ["Software", "Hardware", "Semiconductors"],
    "Healthcare": ["Pharmaceuticals", "Biotechnology", "Medical Devices"],
    "Financials": ["Banks", "Insurance", "Asset Management"],
    "Consumer Discretionary": ["Retail", "Automobiles", "Leisure"],
    "Consumer Staples": ["Food & Beverage", "Household Products"],
    "Industrials": ["Aerospace & Defense", "Machinery", "Transportation"],
    "Energy": ["Oil & Gas", "Renewable Energy"],
    "Materials": ["Chemicals", "Metals & Mining", "Construction Materials"],
    "Utilities": ["Electric Utilities", "Gas Utilities", "Water Utilities"],
    "Real Estate": ["REITs", "Real Estate Services"],
}

COUNTRIES = [
    "United States", "United Kingdom", "Germany", "France", "Japan",
    "Canada", "Australia", "Switzerland", "Netherlands", "Sweden",
]

REGIONS = {
    "North America": ["United States", "Canada"],
    "Europe": ["United Kingdom", "Germany", "France", "Switzerland", "Netherlands", "Sweden"],
    "Asia Pacific": ["Japan", "Australia"],
}

FUND_STRATEGIES = [
    "Venture Capital", "Growth Equity", "Buyout", "Credit",
    "Real Assets", "Large Cap Equity", "Small Cap Equity", "International Equity"
]

COMPANY_NAME_PREFIXES = [
    "Alpha", "Beta", "Gamma", "Delta", "Omega", "Nova", "Apex", "Vertex",
    "Prime", "Core", "Nexus", "Zenith", "Atlas", "Titan", "Quantum",
    "Fusion", "Synergy", "Vector", "Matrix", "Pinnacle", "Summit", "Vanguard",
    "Pioneer", "Infinity", "Stellar", "Horizon", "Phoenix", "Eclipse", "Catalyst",
]

COMPANY_NAME_SUFFIXES = [
    "Technologies", "Solutions", "Systems", "Labs", "Corp", "Inc", "Holdings",
    "Group", "Partners", "Ventures", "Capital", "Industries", "Dynamics",
    "Innovations", "Networks", "Analytics", "Therapeutics", "Biosciences",
]


@dataclass(frozen=True)
class Paths:
    repo_root: Path
    data_silver: Path


def load_config(config_path: Path) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dirs(paths: Paths) -> None:
    paths.data_silver.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# ID Generators
# ---------------------------------------------------------------------------

def make_uuid(seed_str: str) -> str:
    """Deterministic UUID from seed string."""
    return str(uuid.UUID(hashlib.md5(seed_str.encode()).hexdigest()))


# ---------------------------------------------------------------------------
# Taxonomy Generation
# ---------------------------------------------------------------------------

def generate_taxonomy(rng: np.random.Generator) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate meta_taxonomy_version and dim_taxonomy_node."""
    version_id = make_uuid("taxonomy_v1")

    version_df = pd.DataFrame([{
        "taxonomy_version_id": version_id,
        "version_name": "v1.0",
        "source_uri": "synthetic://v1",
        "created_at": date.today().isoformat(),
    }])

    nodes = []
    node_counter = 0

    # Sector nodes
    for sector_name in SECTORS_AND_INDUSTRIES:
        node_counter += 1
        sector_node_id = make_uuid(f"sector_{sector_name}")
        nodes.append({
            "taxonomy_node_id": sector_node_id,
            "taxonomy_version_id": version_id,
            "taxonomy_type": "sector",
            "node_name": sector_name,
            "parent_node_id": None,
            "path": f"/{sector_name}",
            "level": 1,
        })

        # Industry nodes under sector
        for industry_name in SECTORS_AND_INDUSTRIES[sector_name]:
            node_counter += 1
            nodes.append({
                "taxonomy_node_id": make_uuid(f"industry_{industry_name}"),
                "taxonomy_version_id": version_id,
                "taxonomy_type": "industry",
                "node_name": industry_name,
                "parent_node_id": sector_node_id,
                "path": f"/{sector_name}/{industry_name}",
                "level": 2,
            })

    # Geography nodes
    for region_name in REGIONS:
        region_node_id = make_uuid(f"region_{region_name}")
        nodes.append({
            "taxonomy_node_id": region_node_id,
            "taxonomy_version_id": version_id,
            "taxonomy_type": "geography",
            "node_name": region_name,
            "parent_node_id": None,
            "path": f"/{region_name}",
            "level": 1,
        })

        for country_name in REGIONS[region_name]:
            nodes.append({
                "taxonomy_node_id": make_uuid(f"country_{country_name}"),
                "taxonomy_version_id": version_id,
                "taxonomy_type": "geography",
                "node_name": country_name,
                "parent_node_id": region_node_id,
                "path": f"/{region_name}/{country_name}",
                "level": 2,
            })

    nodes_df = pd.DataFrame(nodes)
    return version_df, nodes_df


# ---------------------------------------------------------------------------
# Portfolio Generation
# ---------------------------------------------------------------------------

def generate_portfolio(cfg: dict) -> pd.DataFrame:
    """Generate dim_portfolio (single portfolio for V1)."""
    portfolio_cfg = cfg["v1"]["portfolio"]
    return pd.DataFrame([{
        "portfolio_id": make_uuid("portfolio_demo"),
        "portfolio_name": portfolio_cfg["portfolio_name"],
        "base_currency": portfolio_cfg["base_currency"],
        "owner_type": portfolio_cfg["owner_type"],
        "created_at": date.today().isoformat(),
    }])


# ---------------------------------------------------------------------------
# Fund Generation
# ---------------------------------------------------------------------------

def generate_funds(cfg: dict, rng: np.random.Generator) -> pd.DataFrame:
    """Generate dim_fund."""
    n_private = cfg["v1"]["funds"]["private_funds"]
    n_public = cfg["v1"]["funds"]["public_vehicles"]

    funds = []
    managers = [
        "Blackstone", "KKR", "Carlyle", "Apollo", "TPG",
        "Vanguard", "BlackRock", "Fidelity"
    ]

    for i in range(n_private):
        vintage = rng.integers(2015, 2023)
        funds.append({
            "fund_id": make_uuid(f"fund_private_{i}"),
            "fund_name": f"{managers[i % len(managers)]} Private Fund {chr(65 + i)}",
            "manager_name": managers[i % len(managers)],
            "fund_type": "private",
            "strategy": FUND_STRATEGIES[i % 5],
            "vintage_year": int(vintage),
            "base_currency": "USD",
        })

    for i in range(n_public):
        funds.append({
            "fund_id": make_uuid(f"fund_public_{i}"),
            "fund_name": f"{managers[(n_private + i) % len(managers)]} {FUND_STRATEGIES[5 + i]} Fund",
            "manager_name": managers[(n_private + i) % len(managers)],
            "fund_type": "public",
            "strategy": FUND_STRATEGIES[5 + i],
            "vintage_year": None,
            "base_currency": "USD",
        })

    return pd.DataFrame(funds)


# ---------------------------------------------------------------------------
# Company Generation
# ---------------------------------------------------------------------------

def generate_companies(
    cfg: dict,
    taxonomy_nodes: pd.DataFrame,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate dim_company and dim_entity_alias."""
    n_companies = cfg["v1"]["counts"]["companies"]
    alias_rate = cfg["v1"]["noise"]["alias_rate"]
    missing_sector_rate = cfg["v1"]["noise"]["missing_sector_text_rate"]
    missing_country_rate = cfg["v1"]["noise"]["missing_country_text_rate"]

    # Get all industries and countries from taxonomy
    industries = taxonomy_nodes[
        taxonomy_nodes["taxonomy_type"] == "industry"
    ]["node_name"].tolist()

    countries = COUNTRIES

    companies = []
    aliases = []

    for i in range(n_companies):
        company_id = make_uuid(f"company_{i}")

        # Generate company name
        prefix = rng.choice(COMPANY_NAME_PREFIXES)
        suffix = rng.choice(COMPANY_NAME_SUFFIXES)
        base_name = f"{prefix} {suffix}"

        # Add number suffix to avoid duplicates
        if i >= len(COMPANY_NAME_PREFIXES) * len(COMPANY_NAME_SUFFIXES):
            base_name = f"{base_name} {i // 100}"
        else:
            # Add slight variation
            base_name = f"{prefix}{i % 100:02d} {suffix}" if i > 50 else base_name

        # Assign industry and country
        industry = rng.choice(industries)
        country = rng.choice(countries)

        # Find sector for this industry
        industry_row = taxonomy_nodes[taxonomy_nodes["node_name"] == industry].iloc[0]
        sector_row = taxonomy_nodes[
            taxonomy_nodes["taxonomy_node_id"] == industry_row["parent_node_id"]
        ].iloc[0]

        # Apply missing data noise
        sector_text = None if rng.random() < missing_sector_rate else sector_row["node_name"]
        country_text = None if rng.random() < missing_country_rate else country

        companies.append({
            "company_id": company_id,
            "company_name": base_name,
            "primary_sector": sector_text,
            "primary_industry": industry if sector_text else None,
            "primary_country": country_text,
            "industry_taxonomy_node_id": industry_row["taxonomy_node_id"],
            "country_taxonomy_node_id": make_uuid(f"country_{country}"),
            "website": f"https://www.{base_name.lower().replace(' ', '')}.com",
            "created_at": date.today().isoformat(),
        })

        # Generate aliases for some companies
        if rng.random() < alias_rate:
            alias_variants = _generate_alias_variants(base_name, rng)
            for alias_text in alias_variants:
                aliases.append({
                    "alias_id": make_uuid(f"alias_{company_id}_{alias_text}"),
                    "entity_type": "company",
                    "entity_id": company_id,
                    "alias_text": alias_text,
                    "confidence": round(float(rng.uniform(0.7, 0.95)), 2),
                    "source": "synthetic",
                })

    return pd.DataFrame(companies), pd.DataFrame(aliases)


def _generate_alias_variants(company_name: str, rng: np.random.Generator) -> list[str]:
    """Generate realistic name variants for a company."""
    variants = []
    parts = company_name.split()

    # Abbreviation
    if len(parts) >= 2:
        abbrev = "".join(p[0] for p in parts)
        variants.append(abbrev)

    # First word only
    if len(parts) >= 2:
        variants.append(parts[0])

    # Typo variant (swap two letters)
    if len(company_name) > 4 and rng.random() < 0.5:
        idx = rng.integers(1, len(company_name) - 2)
        typo = company_name[:idx] + company_name[idx + 1] + company_name[idx] + company_name[idx + 2:]
        variants.append(typo)

    return variants[:2]  # Max 2 aliases per company


# ---------------------------------------------------------------------------
# Fund Report Generation
# ---------------------------------------------------------------------------

def generate_fund_reports(
    cfg: dict,
    funds_df: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate fact_fund_report (quarterly reports)."""
    n_quarters = cfg["v1"]["counts"]["quarters"]
    coverage_min = cfg["v1"]["reporting"]["coverage_pct_min"]
    coverage_max = cfg["v1"]["reporting"]["coverage_pct_max"]

    # Generate quarter end dates (most recent 8 quarters)
    today = date.today()
    quarter_ends = []
    for q in range(n_quarters):
        # Go back q quarters
        months_back = (q + 1) * 3
        year = today.year - (today.month - months_back - 1) // 12
        month = ((today.month - months_back - 1) % 12) + 1
        # Adjust to quarter end
        quarter_month = ((month - 1) // 3 + 1) * 3
        if quarter_month > 12:
            quarter_month = 12
        quarter_end = date(year, quarter_month, 1) + timedelta(days=31)
        quarter_end = date(quarter_end.year, quarter_end.month, 1) - timedelta(days=1)
        quarter_ends.append(quarter_end)

    quarter_ends = sorted(quarter_ends)

    reports = []
    for _, fund in funds_df.iterrows():
        for period_end in quarter_ends:
            coverage = rng.uniform(coverage_min, coverage_max)
            reports.append({
                "fund_report_id": make_uuid(f"report_{fund['fund_id']}_{period_end}"),
                "fund_id": fund["fund_id"],
                "report_period_end": period_end.isoformat(),
                "received_date": (period_end + timedelta(days=int(rng.integers(30, 90)))).isoformat(),
                "document_id": None,  # No bronze document for synthetic
                "coverage_estimate": round(float(coverage), 2),
                "nav_usd": int(rng.integers(50_000_000, 500_000_000)),
            })

    return pd.DataFrame(reports)


# ---------------------------------------------------------------------------
# Holdings Generation
# ---------------------------------------------------------------------------

def generate_holdings(
    cfg: dict,
    fund_reports_df: pd.DataFrame,
    companies_df: pd.DataFrame,
    funds_df: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate fact_reported_holding with realistic imperfections."""
    value_populated_rate = cfg["v1"]["reporting"]["value_populated_rate"]
    pct_nav_populated_rate = cfg["v1"]["reporting"]["pct_nav_populated_rate"]
    conflicting_sector_rate = cfg["v1"]["noise"]["conflicting_sector_rate"]

    holdings = []
    company_list = companies_df.to_dict("records")
    n_companies = len(company_list)

    for _, report in fund_reports_df.iterrows():
        fund = funds_df[funds_df["fund_id"] == report["fund_id"]].iloc[0]
        nav = report["nav_usd"]
        coverage = report["coverage_estimate"]

        # Determine number of holdings for this fund
        if fund["fund_type"] == "private":
            n_holdings = rng.integers(15, 40)
        else:
            n_holdings = rng.integers(30, 80)

        # Select companies for this fund (with some overlap across quarters)
        fund_seed = int(hashlib.md5(fund["fund_id"].encode()).hexdigest()[:8], 16)
        rng_fund = np.random.default_rng(fund_seed)

        # Core holdings (consistent across quarters) + some variation
        n_core = int(n_holdings * 0.7)
        core_indices = rng_fund.choice(n_companies, size=min(n_core, n_companies), replace=False)
        variable_indices = rng.choice(n_companies, size=min(n_holdings - n_core, n_companies), replace=False)
        selected_indices = list(set(core_indices) | set(variable_indices))[:n_holdings]

        # Generate weights (power law distribution for realistic concentration)
        raw_weights = rng.pareto(1.5, len(selected_indices))
        weights = raw_weights / raw_weights.sum() * coverage

        for idx, (company_idx, weight) in enumerate(zip(selected_indices, weights)):
            company = company_list[company_idx]
            holding_value = nav * weight

            # Apply noise
            reported_value = holding_value if rng.random() < value_populated_rate else None
            reported_pct = weight * 100 if rng.random() < pct_nav_populated_rate else None

            # Raw company name (might be alias or have typos)
            raw_name = company["company_name"]
            if rng.random() < 0.1:
                # Use abbreviation or variant
                parts = raw_name.split()
                if len(parts) >= 2:
                    raw_name = "".join(p[0] for p in parts) if rng.random() < 0.5 else parts[0]

            # Reported sector (might conflict)
            reported_sector = company["primary_sector"]
            if rng.random() < conflicting_sector_rate and reported_sector:
                # Assign wrong sector
                all_sectors = list(SECTORS_AND_INDUSTRIES.keys())
                wrong_sectors = [s for s in all_sectors if s != reported_sector]
                reported_sector = rng.choice(wrong_sectors)

            holdings.append({
                "reported_holding_id": make_uuid(f"holding_{report['fund_report_id']}_{idx}"),
                "fund_report_id": report["fund_report_id"],
                "company_id": company["company_id"] if rng.random() > 0.05 else None,  # 5% unresolved
                "raw_company_name": raw_name,
                "reported_sector": reported_sector,
                "reported_country": company["primary_country"],
                "reported_value_usd": round(reported_value, 2) if reported_value else None,
                "reported_pct_nav": round(reported_pct, 2) if reported_pct else None,
                "extraction_method": "synthetic",
                "extraction_confidence": round(float(rng.uniform(0.85, 0.99)), 2),
                "document_id": None,
                "page_number": None,
                "row_number": idx + 1,
            })

    return pd.DataFrame(holdings)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    config_path = repo_root / "src" / "lookthrough" / "synthetic" / "config.yaml"
    cfg = load_config(config_path)

    paths = Paths(
        repo_root=repo_root,
        data_silver=repo_root / "data" / "silver",
    )
    ensure_dirs(paths)

    # Initialize RNG
    seed = cfg.get("seed", 42)
    rng = np.random.default_rng(seed)

    print(f"Generating synthetic data with seed={seed}")
    print(f"Counts: {cfg['v1']['counts']}")

    # Generate taxonomy
    print("Generating taxonomy...")
    taxonomy_version_df, taxonomy_nodes_df = generate_taxonomy(rng)

    # Generate portfolio
    print("Generating portfolio...")
    portfolio_df = generate_portfolio(cfg)

    # Generate funds
    print("Generating funds...")
    funds_df = generate_funds(cfg, rng)

    # Generate companies and aliases
    print("Generating companies and aliases...")
    companies_df, aliases_df = generate_companies(cfg, taxonomy_nodes_df, rng)

    # Generate fund reports
    print("Generating fund reports...")
    reports_df = generate_fund_reports(cfg, funds_df, rng)

    # Generate holdings
    print("Generating holdings...")
    holdings_df = generate_holdings(cfg, reports_df, companies_df, funds_df, rng)

    # Save all outputs
    print(f"\nWriting outputs to: {paths.data_silver}")

    taxonomy_version_df.to_csv(paths.data_silver / "meta_taxonomy_version.csv", index=False)
    taxonomy_nodes_df.to_csv(paths.data_silver / "dim_taxonomy_node.csv", index=False)
    portfolio_df.to_csv(paths.data_silver / "dim_portfolio.csv", index=False)
    funds_df.to_csv(paths.data_silver / "dim_fund.csv", index=False)
    companies_df.to_csv(paths.data_silver / "dim_company.csv", index=False)
    aliases_df.to_csv(paths.data_silver / "dim_entity_alias.csv", index=False)
    reports_df.to_csv(paths.data_silver / "fact_fund_report.csv", index=False)
    holdings_df.to_csv(paths.data_silver / "fact_reported_holding.csv", index=False)

    # Print summary
    print("\nGenerated files:")
    print(f"  - meta_taxonomy_version.csv: {len(taxonomy_version_df)} rows")
    print(f"  - dim_taxonomy_node.csv: {len(taxonomy_nodes_df)} rows")
    print(f"  - dim_portfolio.csv: {len(portfolio_df)} rows")
    print(f"  - dim_fund.csv: {len(funds_df)} rows")
    print(f"  - dim_company.csv: {len(companies_df)} rows")
    print(f"  - dim_entity_alias.csv: {len(aliases_df)} rows")
    print(f"  - fact_fund_report.csv: {len(reports_df)} rows")
    print(f"  - fact_reported_holding.csv: {len(holdings_df)} rows")

    print("\nDone.")


if __name__ == "__main__":
    main()
