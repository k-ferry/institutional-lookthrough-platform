"""Parse BDC 10-K HTML filings to extract schedule of investments.

This module parses SEC 10-K filings from Business Development Companies (BDCs)
to extract portfolio holdings and map them to the platform's fact_reported_holding schema.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import re
import uuid
import warnings
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup, Tag

# Suppress XML parsing warning - our BDC filings are HTML-ish
warnings.filterwarnings("ignore", category=UserWarning, module="bs4")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Constants
BRONZE_DIR = Path("data/bronze/filings")
SILVER_DIR = Path("data/silver")
EXTRACTION_METHOD = "html_table_parse"
EXTRACTION_CONFIDENCE = 0.85

# Maximum reasonable fair value for a single holding in actual USD
# BDC holdings rarely exceed $5B; values above this are likely share counts
MAX_REASONABLE_HOLDING_VALUE_USD = 5_000_000_000  # $5 billion


def detect_value_denomination(content: str) -> int:
    """Detect if values in the filing are in thousands or millions.

    Returns multiplier to convert to actual USD:
    - 1_000_000 if values are in millions
    - 1_000 if values are in thousands
    - 1 if values appear to be actual amounts

    Note: Filings may use different denominations for different tables.
    We count occurrences and use the more frequent one, which is typically
    the Schedule of Investments denomination.
    """
    text = content.lower()

    # Count occurrences of each denomination pattern
    millions_patterns = [
        r"\(in\s+millions?\)",
        r"amounts?\s+in\s+millions",
        r"dollars?\s+in\s+millions",
        r"\(\$\s*in\s+millions\)",
    ]

    thousands_patterns = [
        r"\(in\s+thousands?\)",
        r"amounts?\s+in\s+thousands",
        r"dollars?\s+in\s+thousands",
        r"\(\s*\$\s*in\s*thousands\s*\)",
    ]

    millions_count = sum(len(re.findall(p, text)) for p in millions_patterns)
    thousands_count = sum(len(re.findall(p, text)) for p in thousands_patterns)

    logger.debug(f"Denomination detection: millions={millions_count}, thousands={thousands_count}")

    # Use the more frequent denomination (Schedule of Investments is typically
    # the largest table and will have the most denomination references)
    if thousands_count > millions_count:
        return 1_000
    elif millions_count > thousands_count:
        return 1_000_000
    elif millions_count > 0:
        # Equal counts - prefer millions (more common in large BDCs)
        return 1_000_000
    elif thousands_count > 0:
        return 1_000
    else:
        # No explicit denomination found - default to millions
        logger.warning("Could not detect denomination, defaulting to millions")
        return 1_000_000


@dataclass
class ParsedHolding:
    """Intermediate representation of a holding extracted from HTML."""

    company_name: str
    business_description: Optional[str]
    investment_type: Optional[str]
    investment_date: Optional[str]
    maturity_date: Optional[str]
    interest_rate: Optional[str]
    principal: Optional[float]
    cost: Optional[float]
    fair_value: Optional[float]
    shares_units: Optional[str]
    row_number: int
    as_of_date: Optional[str] = None


# Company names that indicate header rows, not actual holdings
HEADER_COMPANY_NAMES = {
    "company",
    "portfolio company",
    "issuer",
    "issuer name",
    "",
}


def generate_deterministic_uuid(seed: str) -> str:
    """Generate a deterministic UUID from a seed string."""
    hash_bytes = hashlib.md5(seed.encode()).digest()
    return str(uuid.UUID(bytes=hash_bytes))


def clean_numeric(value: str) -> Optional[float]:
    """Clean and parse a numeric value from HTML text.

    Handles: commas, parentheses (negative), dollar signs, dashes (zero), unicode chars
    """
    if not value:
        return None

    # Strip whitespace and common characters
    cleaned = value.strip()

    # Handle em-dash, en-dash, or other dash variants meaning zero/null
    if cleaned in ("—", "–", "-", "−", "�", "", "$—", "$–", "$-", "—"):
        return None

    # Also check for single character non-numeric values
    if len(cleaned) == 1 and not cleaned.isdigit():
        return None

    # Check for parentheses (negative value)
    is_negative = "(" in cleaned and ")" in cleaned

    # Remove non-numeric characters except decimal point and minus
    cleaned = re.sub(r"[^\d.\-]", "", cleaned)

    if not cleaned or cleaned == "-" or cleaned == ".":
        return None

    try:
        result = float(cleaned)
        return -result if is_negative else result
    except ValueError:
        return None


def is_date_like(value: str) -> bool:
    """Check if a value looks like a date (MM/DD/YYYY or similar)."""
    if not value:
        return False
    # Match patterns like 8/16/2029, 12/31/2024, etc.
    if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", value.strip()):
        return True
    # Also match MM/YYYY format (used for acquisition/maturity dates)
    if re.match(r"^\d{1,2}/\d{4}$", value.strip()):
        return True
    return False


def is_equity_investment(investment_type: str) -> bool:
    """Check if investment type is equity (not debt).

    Equity positions have share counts in a different column than debt principal,
    and we need to be careful not to confuse share counts with fair values.
    """
    if not investment_type:
        return False
    inv_lower = investment_type.lower()
    equity_keywords = [
        "common stock",
        "common equity",
        "common units",
        "preferred stock",
        "preferred equity",
        "preferred units",
        "preferred member",
        "class a",
        "class b",
        "class c",
        "warrants",
        "warrant",
        "member units",
        "llc interest",
        "llc equity",
        "limited partnership",
        "lp interest",
        "equity interest",
        "series a",
        "series b",
    ]
    return any(kw in inv_lower for kw in equity_keywords)


def detect_schedule_date(table: Tag, soup: BeautifulSoup, content: str) -> Optional[str]:
    """Detect the as-of date for a schedule table by looking in the table content.

    Returns date in YYYY-MM-DD format or None if not found.
    """
    # Look for date in the table's text content (often in header rows)
    table_text = table.get_text().lower()

    # Look for "December 31, YYYY" pattern
    date_patterns = [
        (r"december\s*31,?\s*(202\d)", "12-31"),
        (r"september\s*30,?\s*(202\d)", "09-30"),
        (r"june\s*30,?\s*(202\d)", "06-30"),
        (r"march\s*31,?\s*(202\d)", "03-31"),
    ]

    for pattern, month_day in date_patterns:
        matches = list(re.finditer(pattern, table_text))
        if matches:
            # Use the first match found in the table
            year = matches[0].group(1)
            return f"{year}-{month_day}"

    return None


def find_schedule_of_investments_tables(soup: BeautifulSoup) -> list[tuple[int, Tag]]:
    """Find ALL tables that contain Schedule of Investments data.

    BDC filings typically have the schedule split across many tables (one per page).
    Different BDCs use different header formats:
    - MAIN: "Portfolio Company", "Fair Value"
    - ARCC: "Company", "Fair Value", "Amortized Cost"
    - OBDC: "($ in thousands)Company", "Industry", "Type of Investment", "Fair Value"

    Returns list of (table_index, table) tuples to track position in document.
    """
    tables = []
    all_tables = soup.find_all("table")

    for i, table in enumerate(all_tables):
        # Check first few rows for characteristic headers
        rows = table.find_all("tr")[:5]
        text = " ".join(row.get_text() for row in rows).lower()

        # Check for various header patterns used by different BDCs
        has_company_col = (
            "portfolio company" in text
            or ("company" in text and ("investment" in text or "business description" in text))
            or ("company" in text and "industry" in text)  # OBDC format
        )
        has_value_col = "fair value" in text or "amortized cost" in text

        if has_company_col and has_value_col:
            # Additional check: must have some actual data rows
            if len(table.find_all("tr")) > 3:
                tables.append((i, table))

    return tables


def filter_current_year_tables(
    tables: list[tuple[int, Tag]], report_year: int, content: str
) -> list[Tag]:
    """Filter schedule tables to only include current year (not prior year comparative).

    10-K filings often include comparative schedules for both current and prior year.
    We only want to parse the current year data to avoid duplicates.

    Strategy: Find where prior year schedule starts by looking for "December 31, {prior_year}"
    and exclude tables after that point.
    """
    if not tables:
        return []

    prior_year = report_year - 1

    # Find the position of prior year schedule header in the HTML
    # Look for patterns like "As of December 31, 2024" or "December 31, 2024"
    prior_year_patterns = [
        f"as of december 31, {prior_year}",
        f"december 31, {prior_year}",
        f"as of december 31,{prior_year}",
    ]

    content_lower = content.lower()
    prior_year_positions = []

    for pattern in prior_year_patterns:
        pos = 0
        while True:
            pos = content_lower.find(pattern, pos)
            if pos == -1:
                break
            prior_year_positions.append(pos)
            pos += 1

    if not prior_year_positions:
        # No prior year schedule found - use all tables
        return [table for _, table in tables]

    # Find the minimum position where prior year appears after some schedule tables
    # This indicates the start of the comparative schedule
    min_table_index = tables[0][0]
    max_table_index = tables[-1][0]

    # Estimate that prior year schedule starts around halfway through
    # If there are explicit headers, use them
    # For now, use a simple heuristic: take roughly first half of tables
    # This is imperfect but better than taking all duplicates

    # Better approach: deduplicate by company+investment_type after parsing
    # For now, return all tables and we'll deduplicate later
    return [table for _, table in tables]


def deduplicate_holdings(holdings: list[ParsedHolding]) -> list[ParsedHolding]:
    """Remove duplicate holdings from list.

    10-K filings often contain both current and prior year schedules.
    Strategy: Use (company, investment_type, fair_value) as key to identify unique holdings.

    This approach:
    - Keeps multiple tranches of same investment type (different values)
    - May keep some prior year entries if values changed significantly
    - Removes exact duplicates (same company, type, and value)

    The alternative of using only (company, type) as key incorrectly removes
    multiple tranches of the same loan type, which is common in BDC portfolios.
    """
    seen = set()
    unique_holdings = []

    for holding in holdings:
        # Create a key that identifies unique holdings
        # Include fair_value to preserve multiple tranches with different values
        # Round fair_value to reduce false positives from minor changes
        rounded_value = round(holding.fair_value, -1) if holding.fair_value else None

        key = (
            holding.company_name.lower().strip(),
            (holding.investment_type or "").lower().strip(),
            rounded_value,
        )

        if key not in seen:
            seen.add(key)
            unique_holdings.append(holding)

    return unique_holdings


def extract_holdings_from_table(
    table: Tag, current_company: str, current_description: str, start_row_number: int,
    as_of_date: Optional[str] = None
) -> tuple[list[ParsedHolding], str, str, int]:
    """Extract holdings from a Schedule of Investments table.

    Returns tuple of (holdings, last_company, last_description, next_row_number)

    This handles multiple formats:
    1. MAIN format: Company row, then separate investment detail rows
    2. ARCC format: Company + investment on same row
    3. OBDC format: Flat rows where each row is a complete holding (company + industry + investment + values)
    """
    holdings = []
    rows = table.find_all("tr")
    row_number = start_row_number

    # Detect OBDC-style flat format (header contains "Industry" as separate column)
    first_rows_text = " ".join(row.get_text() for row in rows[:3]).lower()
    is_flat_format = "industry" in first_rows_text and "type of investment" in first_rows_text

    for row in rows:
        cells = row.find_all(["td", "th"])
        cell_texts = [cell.get_text(strip=True) for cell in cells]

        # Skip completely empty rows
        non_empty = [t for t in cell_texts if t]
        if not non_empty:
            continue

        # Skip header rows (contain "Portfolio Company"/"Company", "Fair Value", etc.)
        row_text = " ".join(cell_texts).lower()
        is_header = (
            (
                "portfolio company" in row_text
                or "company (1)" in row_text
                or "thousands)company" in row_text  # OBDC format
            )
            and ("fair value" in row_text or "amortized cost" in row_text)
        )
        if is_header:
            continue

        # Skip section headers like "Control Investments", "Affiliate Investments"
        if is_section_header(cell_texts):
            continue

        # Detect subtotal rows (just numbers in last few columns)
        if is_subtotal_row(cell_texts):
            continue

        # For flat format (OBDC), extract company + investment from same row
        if is_flat_format:
            holding = try_extract_flat_holding(cell_texts, row_number, as_of_date)
            if holding:
                holdings.append(holding)
                row_number += 1
            continue

        # For hierarchical format (MAIN/ARCC):
        # Detect company name rows
        company_info = try_extract_company_info(cell_texts)
        if company_info:
            current_company, current_description = company_info
            # In ARCC format, company + investment are on same row
            # Try to also extract investment from this row
            holding = try_extract_investment(
                cell_texts, current_company, current_description, row_number, as_of_date
            )
            if holding:
                holdings.append(holding)
                row_number += 1
            continue

        # Try to extract investment details (continuation rows)
        if current_company:
            holding = try_extract_investment(
                cell_texts, current_company, current_description, row_number, as_of_date
            )
            if holding:
                holdings.append(holding)
                row_number += 1

    return holdings, current_company, current_description, row_number


def is_section_header(cell_texts: list[str]) -> bool:
    """Check if row is a section header like 'Control Investments'."""
    text = " ".join(cell_texts).lower()
    section_keywords = [
        "control investments",
        "affiliate investments",
        "non-control/non-affiliate",
        "non-affiliate investments",
        "total investments",
        "total control",
        "total affiliate",
        "subtotal",
    ]
    return any(kw in text for kw in section_keywords)


def try_extract_company_info(cell_texts: list[str]) -> Optional[tuple[str, Optional[str]]]:
    """Try to extract company name and description from a row.

    Handles two formats:
    1. MAIN format: Company row has NO investment type (separate rows for investments)
    2. ARCC format: Company + investment type on same row (check first cell for company name)

    Company rows have:
    - Company name in one of the first few cells
    - Often a business description nearby
    - NOT purely numeric values
    """
    # Filter to non-empty cells
    non_empty = [(i, t) for i, t in enumerate(cell_texts) if t.strip()]

    if not non_empty:
        return None

    # Investment type keywords
    investment_types = [
        # MAIN format
        "secured debt",
        "unsecured debt",
        "subordinated",
        "preferred",
        "common stock",
        "common equity",
        "warrants",
        "member units",
        "llc interest",
        "class a",
        "class b",
        # ARCC format
        "first lien",
        "second lien",
        "senior secured",
        "senior subordinated",
        "junior secured",
        "unitranche",
        "mezzanine",
        "limited partnership",
        "llc equity",
        "revolving",
    ]

    row_text = " ".join(cell_texts).lower()
    has_investment_type = any(inv_type in row_text for inv_type in investment_types)

    # For MAIN-style format: company rows don't have investment types
    # For ARCC-style format: company + investment are on same row
    # We distinguish by checking if the FIRST non-empty cell looks like a company name

    first_cell_text = non_empty[0][1].strip() if non_empty else ""

    # Check if first cell looks like a company name (not investment type, not numeric)
    first_cell_is_company = (
        len(first_cell_text) > 4
        and not re.match(r"^[\d,.\-\$\(\)%\s]+$", first_cell_text)
        and not is_date_like(first_cell_text)
        and not any(inv_type in first_cell_text.lower() for inv_type in investment_types)
        and not first_cell_text.lower().startswith(("total", "subtotal"))
    )

    if has_investment_type and not first_cell_is_company:
        # This is an investment-only row (MAIN style continuation)
        return None

    # Look for company name pattern in first cells
    for idx, text in non_empty[:3]:
        text = text.strip()

        # Skip purely numeric
        if re.match(r"^[\d,.\-\$\(\)%\s]+$", text):
            continue

        # Skip short strings (footnotes like "(10)")
        if len(text) < 4:
            continue

        # Skip date patterns
        if is_date_like(text):
            continue

        # Skip investment type keywords in first position
        if any(inv_type in text.lower() for inv_type in investment_types):
            continue

        # This looks like a company name
        company = re.sub(r"\s*\(\d+\)\s*", " ", text).strip()  # Remove footnote markers

        # Try to find description in nearby cell
        description = None
        for desc_idx, desc_text in non_empty:
            if desc_idx != idx and desc_idx > idx:
                desc_text = desc_text.strip()
                # Skip investment types as descriptions
                if any(inv_type in desc_text.lower() for inv_type in investment_types):
                    continue
                if (
                    len(desc_text) > 5
                    and not re.match(r"^[\d,.\-\$\(\)%\s]+$", desc_text)
                    and not is_date_like(desc_text)
                ):
                    description = desc_text
                    break

        return company, description

    return None


def is_subtotal_row(cell_texts: list[str]) -> bool:
    """Check if row is a subtotal row (just has 1-2 numbers, no text)."""
    non_empty = [t.strip() for t in cell_texts if t.strip()]

    if len(non_empty) > 3:
        return False

    # All non-empty should be numeric-looking
    for text in non_empty:
        # Allow numbers, commas, dollar signs, parens, dashes
        if not re.match(r"^[\d,.\-\$\(\)\s—–]+$", text):
            return False

    return len(non_empty) >= 1


def try_extract_flat_holding(
    cell_texts: list[str],
    row_number: int,
    as_of_date: Optional[str] = None,
) -> Optional[ParsedHolding]:
    """Extract a complete holding from a flat-format row (OBDC style).

    OBDC format: Company, Industry, Type of Investment, Interest Rate, Maturity, %, Principal, Cost, Fair Value
    Each row is a complete holding with all information.
    """
    non_empty = [(i, t.strip()) for i, t in enumerate(cell_texts) if t.strip()]

    if len(non_empty) < 5:
        return None

    # First cell should be company name (may include footnotes and address)
    first_cell = non_empty[0][1] if non_empty else ""

    # Skip if first cell is numeric or looks like a header
    if re.match(r"^[\d,.\-\$\(\)%\s]+$", first_cell):
        return None
    if len(first_cell) < 3:
        return None

    # Extract company name (remove footnote markers and truncated address)
    company_name = re.sub(r"\s*\(\d+\)\s*", " ", first_cell).strip()
    # Truncate at common address patterns
    for pattern in [" LLC ", " Inc. ", " Corp. ", " LP ", " Ltd "]:
        if pattern in company_name:
            idx = company_name.find(pattern) + len(pattern) - 1
            company_name = company_name[:idx].strip()
            break

    # Second cell is usually Industry
    industry = None
    if len(non_empty) > 1:
        ind_text = non_empty[1][1]
        if not re.match(r"^[\d,.\-\$\(\)%\s]+$", ind_text) and len(ind_text) > 2:
            industry = ind_text

    # Look for investment type
    investment_type = None
    investment_types = [
        "first lien",
        "second lien",
        "senior secured",
        "unsecured",
        "subordinated",
        "mezzanine",
        "equity",
        "preferred",
        "common",
        "warrant",
        "revolving",
        "term loan",
    ]

    for idx, text in non_empty:
        text_lower = text.lower()
        for inv_type in investment_types:
            if inv_type in text_lower:
                investment_type = text
                break
        if investment_type:
            break

    if not investment_type:
        return None

    # Extract dates
    investment_date = None
    maturity_date = None
    interest_rate = None

    for idx, text in non_empty:
        # Date patterns: MM/YYYY or M/YYYY
        if re.match(r"^\d{1,2}/\d{4}$", text):
            if maturity_date is None:
                maturity_date = text
        elif re.match(r"^\d+\.\d+%", text) or re.match(r"^\d+%", text):
            interest_rate = text

    # Extract numeric values from the end
    numeric_values = []
    for idx, text in reversed(non_empty):
        if re.match(r"^\d{1,2}/\d{4}$", text):  # Skip date
            continue
        if "%" in text:  # Skip percentage
            continue
        if re.match(r"^[a-zA-Z]", text) and text not in ("$", "€", "£", "A$"):
            continue
        if re.match(r"^\(\d+\)(\(\d+\))*$", text):  # Skip footnotes
            continue

        val = clean_numeric(text)
        if val is not None:
            numeric_values.insert(0, val)
            if len(numeric_values) >= 3:
                break

    if not numeric_values:
        return None

    # Filter out very small values that might be percentages
    value_candidates = [v for v in numeric_values if abs(v) >= 1 or v == 0]
    if not value_candidates:
        value_candidates = numeric_values

    fair_value = value_candidates[-1] if len(value_candidates) >= 1 else None
    cost = value_candidates[-2] if len(value_candidates) >= 2 else None
    principal = value_candidates[-3] if len(value_candidates) >= 3 else None

    return ParsedHolding(
        company_name=company_name,
        business_description=industry,
        investment_type=investment_type,
        investment_date=investment_date,
        maturity_date=maturity_date,
        interest_rate=interest_rate,
        principal=principal,
        cost=cost,
        fair_value=fair_value,
        shares_units=None,
        row_number=row_number,
        as_of_date=as_of_date,
    )


def try_extract_investment(
    cell_texts: list[str],
    company_name: str,
    business_description: Optional[str],
    row_number: int,
    as_of_date: Optional[str] = None,
) -> Optional[ParsedHolding]:
    """Try to extract investment details from a row.

    Investment rows have:
    - Investment type (Secured Debt, Preferred, etc.) in an early cell
    - Numeric values at the end (Principal, Cost, Fair Value)
    - Dates for investment date and maturity
    """
    non_empty = [(i, t.strip()) for i, t in enumerate(cell_texts) if t.strip()]

    if len(non_empty) < 3:
        return None

    # Check for investment type
    investment_type = None
    investment_types = [
        # MAIN format
        "secured debt",
        "unsecured debt",
        "subordinated debt",
        "subordinated note",
        "preferred stock",
        "preferred equity",
        "preferred member",
        "common stock",
        "common equity",
        "warrants",
        "member units",
        "llc interest",
        "equity",
        "debt",
        "note",
        "class a",
        "class b",
        # ARCC format
        "first lien",
        "second lien",
        "senior secured",
        "senior subordinated",
        "junior secured",
        "unitranche",
        "mezzanine",
        "limited partnership",
        "llc equity",
        "revolving",
        # OBDC format
        "unsecured facility",
        "secured loan",
        "term loan",
    ]

    for idx, text in non_empty:
        text_lower = text.lower()
        for inv_type in investment_types:
            if inv_type in text_lower:
                investment_type = re.sub(r"\s*\(\d+\)\s*", " ", text).strip()
                break
        if investment_type:
            break

    if not investment_type:
        return None

    # Extract dates
    investment_date = None
    maturity_date = None
    interest_rate = None
    shares_units = None

    for idx, text in non_empty:
        if is_date_like(text):
            if investment_date is None:
                investment_date = text
            else:
                maturity_date = text
        elif re.match(r"^\d+\.\d+%$", text) or re.match(r"^\d+%$", text):
            interest_rate = text
        elif re.match(r"^[\d,]+$", text) and len(text) > 2:
            # Could be shares/units or a value - check position
            # Shares usually appear before the financial values
            pass

    # Extract numeric values from the row
    # Look for the last 3-4 numeric values (Principal, Cost, Fair Value, and sometimes % of Net Assets)
    # Skip dates, percentages, footnotes, and reference codes
    numeric_values = []
    for idx, text in reversed(non_empty):
        # Skip dates
        if is_date_like(text):
            continue
        # Skip percentages
        if "%" in text:
            continue
        # Skip text values (but allow $ which precedes numbers)
        if re.match(r"^[a-zA-Z]", text) and text != "$":
            continue
        # Skip footnote references like (2)(9), (13), etc.
        if re.match(r"^\(\d+\)(\(\d+\))*$", text):
            continue
        # Skip SOFR references
        if "SOFR" in text:
            continue

        val = clean_numeric(text)
        if val is not None:
            numeric_values.insert(0, val)
            if len(numeric_values) >= 4:  # Get up to 4 in case % of Net Assets is included
                break

    if not numeric_values:
        return None

    # Assign values: last is fair_value, second-to-last is cost, third is principal
    # Skip very small values that might be % of Net Assets (< 1)
    # Filter out potential % values
    value_candidates = [v for v in numeric_values if abs(v) >= 1 or v == 0]
    if not value_candidates:
        value_candidates = numeric_values

    fair_value = value_candidates[-1] if len(value_candidates) >= 1 else None
    cost = value_candidates[-2] if len(value_candidates) >= 2 else None
    principal = value_candidates[-3] if len(value_candidates) >= 3 else None

    # Additional check: if fair value looks like a date (MMDDYYYY pattern)
    if fair_value and fair_value > 1000000:
        fv_str = str(int(fair_value))
        if len(fv_str) >= 7 and fv_str[-4:].startswith("20"):
            # This is probably a date like 8162029 (8/16/2029)
            return None

    return ParsedHolding(
        company_name=company_name,
        business_description=business_description,
        investment_type=investment_type,
        investment_date=investment_date,
        maturity_date=maturity_date,
        interest_rate=interest_rate,
        principal=principal,
        cost=cost,
        fair_value=fair_value,
        shares_units=shares_units,
        row_number=row_number,
        as_of_date=as_of_date,
    )


def parse_bdc_filing(filename: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Parse a BDC 10-K filing and extract holdings.

    Args:
        filename: Name of the HTML file in data/bronze/filings/

    Returns:
        Tuple of (holdings_df, fund_df, fund_report_df)
    """
    filepath = BRONZE_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"Filing not found: {filepath}")

    logger.info(f"Parsing BDC filing: {filename}")

    # Read and parse HTML
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    soup = BeautifulSoup(content, "lxml")

    # Detect value denomination (thousands vs millions)
    value_multiplier = detect_value_denomination(content)
    logger.info(f"Detected value denomination: {'millions' if value_multiplier == 1_000_000 else 'thousands'}")

    # Extract fund name from filename or document
    fund_name = extract_fund_name(soup, filename)
    logger.info(f"Detected fund: {fund_name}")

    # Extract report date (this is the current year's date)
    report_date = extract_report_date(soup, filename)
    report_year = int(report_date[:4])
    logger.info(f"Report date: {report_date}")

    # Find ALL schedule of investments tables
    tables = find_schedule_of_investments_tables(soup)
    logger.info(f"Found {len(tables)} Schedule of Investments tables")

    # Filter tables to current year only (skip prior year comparative schedules)
    # Look for tables that are part of the current year schedule
    current_year_tables = filter_current_year_tables(tables, report_year, content)
    logger.info(f"Filtered to {len(current_year_tables)} current year tables")

    # Parse all tables, maintaining company context across tables
    all_holdings = []
    current_company = ""
    current_description = None
    row_number = 1

    for i, table in enumerate(current_year_tables):
        # Detect the as-of date for this table by looking at table content
        # Default to report_date if not found
        table_date = detect_schedule_date(table, soup, content) or report_date
        if table_date != report_date:
            logger.debug(f"  Table {i + 1}: detected date {table_date}")

        holdings, current_company, current_description, row_number = extract_holdings_from_table(
            table, current_company, current_description, row_number, table_date
        )
        if holdings:
            logger.info(f"  Table {i + 1}: extracted {len(holdings)} holdings")
            all_holdings.extend(holdings)

    logger.info(f"Total holdings before filtering: {len(all_holdings)}")

    # Filter out header rows that were incorrectly parsed as holdings
    header_filtered = [
        h for h in all_holdings
        if h.company_name.lower().strip() not in HEADER_COMPANY_NAMES
    ]
    if len(header_filtered) < len(all_holdings):
        logger.info(f"Filtered {len(all_holdings) - len(header_filtered)} header rows")
    all_holdings = header_filtered

    logger.info(f"Total holdings before deduplication: {len(all_holdings)}")

    # Deduplicate holdings - 10-K filings often contain both current and prior year schedules
    # Keep the first occurrence of each company+investment_type combination
    # (current year appears before prior year in the filing)
    all_holdings = deduplicate_holdings(all_holdings)
    logger.info(f"Total holdings after deduplication: {len(all_holdings)}")

    if not all_holdings:
        logger.warning("No holdings extracted from filing!")

    # Generate IDs
    fund_id = generate_deterministic_uuid(f"bdc-{fund_name}")
    fund_report_id = generate_deterministic_uuid(f"bdc-report-{filename}")

    # Create fund record
    fund_df = pd.DataFrame(
        [
            {
                "fund_id": fund_id,
                "fund_name": fund_name,
                "manager_name": fund_name,  # BDC is self-managed
                "fund_type": "BDC",
                "strategy": "Direct Lending",
                "vintage_year": None,
                "base_currency": "USD",
            }
        ]
    )

    # Create fund report record
    # Apply detected denomination multiplier
    total_fair_value = sum(h.fair_value for h in all_holdings if h.fair_value) * value_multiplier

    fund_report_df = pd.DataFrame(
        [
            {
                "fund_report_id": fund_report_id,
                "fund_id": fund_id,
                "report_period_end": report_date,
                "received_date": datetime.now().strftime("%Y-%m-%d"),
                "document_id": None,
                "coverage_estimate": 1.0,  # Full coverage for direct filings
                "nav_usd": total_fair_value,
            }
        ]
    )

    # Create holdings records, filtering out invalid values
    holdings_records = []
    skipped_count = 0
    for holding in all_holdings:
        # Apply detected denomination multiplier to convert to actual USD
        fair_value_usd = holding.fair_value * value_multiplier if holding.fair_value else None

        # Sanity check: fair value should be reasonable (not a share count)
        # Values > $5B are likely share counts being misidentified
        if fair_value_usd and fair_value_usd > MAX_REASONABLE_HOLDING_VALUE_USD:
            logger.debug(
                f"Skipping holding with unreasonable value ${fair_value_usd/1e9:.1f}B: "
                f"{holding.company_name[:40]}"
            )
            skipped_count += 1
            continue

        holdings_records.append(
            {
                "reported_holding_id": str(uuid.uuid4()),
                "fund_report_id": fund_report_id,
                "company_id": None,  # Will be resolved by entity resolution
                "raw_company_name": holding.company_name,
                "reported_sector": holding.business_description,  # Use business description as sector
                "reported_country": None,  # BDC filings rarely include country
                "reported_value_usd": fair_value_usd,
                "reported_pct_nav": None,  # Can be computed later
                "extraction_method": EXTRACTION_METHOD,
                "extraction_confidence": EXTRACTION_CONFIDENCE,
                "document_id": None,
                "page_number": None,
                "row_number": holding.row_number,
                "as_of_date": holding.as_of_date,
            }
        )

    if skipped_count > 0:
        logger.info(f"Skipped {skipped_count} holdings with unreasonable values (likely share counts)")

    holdings_df = pd.DataFrame(holdings_records)

    return holdings_df, fund_df, fund_report_df


def extract_fund_name(soup: BeautifulSoup, filename: str) -> str:
    """Extract fund/company name from the filing."""
    # Try to find company name in the document
    # Look for patterns like "MAIN STREET CAPITAL CORPORATION"
    title_patterns = [
        r"([A-Z][A-Z\s&]+(?:CORPORATION|CORP\.|INC\.|LLC|LP|CAPITAL|PARTNERS))",
    ]

    text = soup.get_text()

    for pattern in title_patterns:
        matches = re.findall(pattern, text[:5000])  # Check first 5000 chars
        if matches:
            # Return the longest match (likely the full company name)
            return max(matches, key=len).strip()

    # Fallback: use filename
    name = filename.replace("_10K_", " ").replace("_", " ").replace(".html", "")
    return name.split()[0] + " Capital"


def extract_report_date(soup: BeautifulSoup, filename: str) -> str:
    """Extract report date from the filing."""
    # Try to find date in filename first (e.g., MAIN_10K_2025.html -> 2024-12-31)
    year_match = re.search(r"(\d{4})", filename)
    if year_match:
        year = int(year_match.group(1))
        # 10-K filings are for the prior year end
        return f"{year - 1}-12-31"

    # Try to find in document
    text = soup.get_text()
    date_patterns = [
        r"December\s*31,\s*(\d{4})",
        r"(\d{4})-12-31",
        r"fiscal year ended.*?(\d{4})",
    ]

    for pattern in date_patterns:
        match = re.search(pattern, text[:10000], re.IGNORECASE)
        if match:
            year = int(match.group(1))
            return f"{year}-12-31"

    # Default to current year - 1
    return f"{datetime.now().year - 1}-12-31"


def print_summary(holdings_df: pd.DataFrame) -> None:
    """Print summary statistics about extracted holdings."""
    print("\n" + "=" * 60)
    print("BDC FILING EXTRACTION SUMMARY")
    print("=" * 60)

    print(f"\nTotal holdings extracted: {len(holdings_df)}")

    if not holdings_df.empty and "reported_value_usd" in holdings_df.columns:
        total_value = holdings_df["reported_value_usd"].sum()
        print(f"Total fair value: ${total_value:,.0f}")

        # Count unique companies
        unique_companies = holdings_df["raw_company_name"].nunique()
        print(f"Unique portfolio companies: {unique_companies}")

        # Check for potential duplicates (same company + same value)
        potential_dupes = holdings_df.duplicated(
            subset=["raw_company_name", "reported_value_usd"], keep=False
        ).sum()
        if potential_dupes > 0:
            print(f"\nNote: {potential_dupes} potential duplicate rows detected")
            print("  (Filing may contain both current and prior year schedules)")

        # Check for very large values (> $1B)
        large_values = holdings_df[holdings_df["reported_value_usd"] > 1_000_000_000]
        if not large_values.empty:
            print(f"\nWarning: {len(large_values)} holdings have values > $1B (verify parsing)")

        # Check for negative values
        negative_values = holdings_df[holdings_df["reported_value_usd"] < 0]
        if not negative_values.empty:
            print(f"Note: {len(negative_values)} holdings have negative values (write-downs)")

        # Top sectors (business descriptions)
        if "reported_sector" in holdings_df.columns:
            sectors = holdings_df["reported_sector"].dropna()
            if not sectors.empty:
                sector_counts = Counter(sectors)
                print("\nTop 10 sectors/industries:")
                for sector, count in sector_counts.most_common(10):
                    sector_holdings = holdings_df[holdings_df["reported_sector"] == sector]
                    sector_value = sector_holdings["reported_value_usd"].sum()
                    print(f"  {sector[:50]:50s} {count:4d} holdings  ${sector_value:>15,.0f}")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Parse BDC 10-K filing to extract holdings")
    parser.add_argument(
        "--file",
        type=str,
        required=True,
        help="Name of HTML filing in data/bronze/filings/ (e.g., MAIN_10K_2025.html)",
    )
    args = parser.parse_args()

    try:
        holdings_df, fund_df, fund_report_df = parse_bdc_filing(args.file)

        # Write outputs to silver layer with bdc_ prefix
        # Append to existing files if they exist, then deduplicate
        SILVER_DIR.mkdir(parents=True, exist_ok=True)

        holdings_path = SILVER_DIR / "bdc_fact_reported_holding.csv"
        fund_path = SILVER_DIR / "bdc_dim_fund.csv"
        report_path = SILVER_DIR / "bdc_fact_fund_report.csv"

        # Append holdings and deduplicate by reported_holding_id
        if holdings_path.exists():
            existing_holdings = pd.read_csv(holdings_path)
            holdings_df = pd.concat([existing_holdings, holdings_df], ignore_index=True)
            holdings_df = holdings_df.drop_duplicates(subset=["reported_holding_id"], keep="last")
        holdings_df.to_csv(holdings_path, index=False)

        # Append funds and deduplicate by fund_id
        if fund_path.exists():
            existing_funds = pd.read_csv(fund_path)
            fund_df = pd.concat([existing_funds, fund_df], ignore_index=True)
            fund_df = fund_df.drop_duplicates(subset=["fund_id"], keep="last")
        fund_df.to_csv(fund_path, index=False)

        # Append fund reports and deduplicate by fund_report_id
        if report_path.exists():
            existing_reports = pd.read_csv(report_path)
            fund_report_df = pd.concat([existing_reports, fund_report_df], ignore_index=True)
            fund_report_df = fund_report_df.drop_duplicates(subset=["fund_report_id"], keep="last")
        fund_report_df.to_csv(report_path, index=False)

        logger.info(f"Wrote {len(holdings_df)} total holdings to {holdings_path}")
        logger.info(f"Wrote {len(fund_df)} total funds to {fund_path}")
        logger.info(f"Wrote {len(fund_report_df)} total fund reports to {report_path}")

        print_summary(holdings_df)

    except Exception as e:
        logger.error(f"Error parsing filing: {e}")
        raise


if __name__ == "__main__":
    main()
