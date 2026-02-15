#!/usr/bin/env python3
"""
Download BDC (Business Development Company) 10-K and 10-Q filings from SEC EDGAR.

These filings contain detailed schedules of investments that are useful for testing
the PDF/document ingestion pipeline.

Usage:
    python scripts/download_bdc_filings.py

Note: Requires internet access. Update the USER_EMAIL before running.
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from edgar import Company, set_identity

# ============================================================================
# CONFIGURATION - Update this email before running
# ============================================================================
USER_EMAIL = "kf72171p@pace.edu"  # TODO: Update with your email

# BDCs to download filings for
BDCS = [
    {"ticker": "ARCC", "name": "Ares Capital Corporation"},
    {"ticker": "MAIN", "name": "Main Street Capital Corporation"},
    {"ticker": "OBDC", "name": "Blue Owl Capital Corporation"},
]

# Filing types to download
FILING_TYPES = ["10-K"]

# Output directory
OUTPUT_DIR = project_root / "data" / "bronze" / "filings"


def ensure_directories():
    """Create output directories if they don't exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}")


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def download_filing_html(filing, ticker: str, filing_type: str) -> tuple[Path | None, int]:
    """
    Download filing HTML content.

    Returns:
        Tuple of (file_path, file_size) or (None, 0) if failed.
    """
    try:
        # Get the filing year from the filing date
        filing_date = filing.filing_date
        year = filing_date.year if hasattr(filing_date, "year") else str(filing_date)[:4]

        # Create filename
        filename = f"{ticker}_{filing_type.replace('-', '')}_{year}.html"
        filepath = OUTPUT_DIR / filename

        # Get the HTML content
        html_content = filing.html()

        if html_content:
            # Write to file
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(html_content)

            file_size = filepath.stat().st_size
            return filepath, file_size
        else:
            print(f"  Warning: No HTML content available for {ticker} {filing_type}")
            return None, 0

    except Exception as e:
        print(f"  Error downloading HTML for {ticker} {filing_type}: {e}")
        return None, 0


def download_filing_pdf(filing, ticker: str, filing_type: str) -> tuple[Path | None, int]:
    """
    Try to download filing as PDF if available.

    Returns:
        Tuple of (file_path, file_size) or (None, 0) if not available.
    """
    try:
        # Get the filing year
        filing_date = filing.filing_date
        year = filing_date.year if hasattr(filing_date, "year") else str(filing_date)[:4]

        # Create filename
        filename = f"{ticker}_{filing_type.replace('-', '')}_{year}.pdf"
        filepath = OUTPUT_DIR / filename

        # Try to get PDF - edgartools may have different methods
        # Check if filing has attachments or documents
        if hasattr(filing, "attachments"):
            for attachment in filing.attachments:
                if hasattr(attachment, "document_type") and "pdf" in str(attachment.document_type).lower():
                    content = attachment.download()
                    with open(filepath, "wb") as f:
                        f.write(content)
                    file_size = filepath.stat().st_size
                    return filepath, file_size

        # Alternative: check documents
        if hasattr(filing, "documents"):
            for doc in filing.documents:
                doc_name = str(getattr(doc, "name", "") or getattr(doc, "filename", "")).lower()
                if doc_name.endswith(".pdf"):
                    content = doc.download()
                    with open(filepath, "wb") as f:
                        f.write(content)
                    file_size = filepath.stat().st_size
                    return filepath, file_size

        print(f"  Note: No PDF available for {ticker} {filing_type} (HTML downloaded instead)")
        return None, 0

    except Exception as e:
        print(f"  Note: PDF not available for {ticker} {filing_type}: {e}")
        return None, 0


def download_bdc_filings():
    """Main function to download all BDC filings."""
    print("=" * 60)
    print("BDC Filing Downloader - SEC EDGAR")
    print("=" * 60)

    # Check if user updated email
    if USER_EMAIL == "kf72171p@pace.edu":
        print("\nWARNING: Please update USER_EMAIL in the script before running.")
        print("The SEC EDGAR API requires a valid email address as identity.")
        print("Edit scripts/download_bdc_filings.py and set USER_EMAIL.\n")
        response = input("Continue anyway with placeholder email? (y/n): ")
        if response.lower() != "y":
            print("Exiting. Please update USER_EMAIL and run again.")
            return

    # Set identity for SEC EDGAR
    set_identity(USER_EMAIL)
    print(f"\nUsing identity: {USER_EMAIL}")

    # Ensure output directories exist
    ensure_directories()

    # Track downloads
    downloads = []

    print("\n" + "-" * 60)
    print("Downloading filings...")
    print("-" * 60)

    for bdc in BDCS:
        ticker = bdc["ticker"]
        name = bdc["name"]

        print(f"\n{name} ({ticker})")
        print("-" * 40)

        try:
            # Get company by ticker
            company = Company(ticker)
            print(f"  Found: {company.name}")

            for filing_type in FILING_TYPES:
                print(f"\n  Fetching {filing_type}...")

                try:
                    # Get filings of this type
                    filings = company.get_filings(form=filing_type)

                    if filings and len(filings) > 0:
                        # Get most recent filing
                        latest_filing = filings[0]

                        filing_date = latest_filing.filing_date
                        print(f"  Latest {filing_type}: {filing_date}")

                        # Download HTML
                        html_path, html_size = download_filing_html(
                            latest_filing, ticker, filing_type
                        )
                        if html_path:
                            downloads.append({
                                "ticker": ticker,
                                "type": filing_type,
                                "format": "HTML",
                                "path": html_path,
                                "size": html_size,
                            })
                            print(f"  Downloaded HTML: {html_path.name} ({format_file_size(html_size)})")

                        # Try to download PDF
                        pdf_path, pdf_size = download_filing_pdf(
                            latest_filing, ticker, filing_type
                        )
                        if pdf_path:
                            downloads.append({
                                "ticker": ticker,
                                "type": filing_type,
                                "format": "PDF",
                                "path": pdf_path,
                                "size": pdf_size,
                            })
                            print(f"  Downloaded PDF: {pdf_path.name} ({format_file_size(pdf_size)})")
                    else:
                        print(f"  No {filing_type} filings found for {ticker}")

                except Exception as e:
                    print(f"  Error fetching {filing_type} for {ticker}: {e}")

        except Exception as e:
            print(f"  Error accessing company {ticker}: {e}")

    # Print summary
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)

    if downloads:
        total_size = sum(d["size"] for d in downloads)

        print(f"\nFiles downloaded: {len(downloads)}")
        print(f"Total size: {format_file_size(total_size)}")
        print(f"\nLocation: {OUTPUT_DIR}\n")

        print("Files:")
        print("-" * 60)
        for d in downloads:
            print(f"  {d['path'].name:<40} {format_file_size(d['size']):>10}")
    else:
        print("\nNo files were downloaded.")
        print("Check your internet connection and try again.")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    download_bdc_filings()
