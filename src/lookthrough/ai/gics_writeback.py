"""
GICS Write-back — re-applies existing gics_mapping rows to dim_company.

Called by the pipeline runner after load_sources and before entity_resolution
to restore primary_sector / primary_industry on dim_company rows that were
cleared when dim_company was rebuilt from scratch.

Does NOT call the Claude API. Reads gics_mapping from PostgreSQL and applies
it using the same logic as map_to_gics._update_dim_company_from_gics().

Usage:
    python -m src.lookthrough.ai.gics_writeback
"""

import argparse

from src.lookthrough.db.models import GICSMapping
from src.lookthrough.db.repository import _is_csv_mode, get_all
from src.lookthrough.ai.map_to_gics import _update_dim_company_from_gics


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-apply GICS mappings to dim_company.")
    parser.add_argument("--csv", action="store_true", help="CSV mode (write-back is a no-op)")
    args = parser.parse_args()

    if args.csv or _is_csv_mode():
        print("CSV mode: GICS write-back skipped.")
        return

    existing = get_all(GICSMapping)
    if existing.empty:
        print("No GICS mappings in database — skipping write-back.")
        return

    print(f"Found {len(existing)} GICS mappings. Applying to dim_company...")
    _update_dim_company_from_gics([], existing)


if __name__ == "__main__":
    main()
