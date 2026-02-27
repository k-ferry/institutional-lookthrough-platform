"""Analyze BDC totals and check for issues."""

import sys
sys.path.insert(0, 'src')
from lookthrough.ingestion.parse_bdc_filing import parse_bdc_filing

for filename in ['MAIN_10K_2025.html', 'OBDC_10K_2025.html', 'ARCC_10K_2026.html']:
    ticker = filename.split('_')[0]
    print(f'\n{"="*60}')
    print(f'{ticker}')
    print("="*60)

    holdings_df, _, _ = parse_bdc_filing(filename)

    # Check for 'Company' entries (parsing errors)
    company_entries = holdings_df[holdings_df.raw_company_name == 'Company']
    if len(company_entries) > 0:
        print(f'WARNING: {len(company_entries)} entries with name "Company" (parsing error)')

    # Filter to real companies
    real = holdings_df[holdings_df.raw_company_name != 'Company']

    print(f'Holdings: {len(real)}')
    print(f'Total: ${real.reported_value_usd.sum()/1e9:.2f}B')
    print(f'Unique companies: {real.raw_company_name.nunique()}')

    # Check for likely duplicates (same company, multiple entries with ~same value)
    dup_count = 0
    for company in real.raw_company_name.unique():
        subset = real[real.raw_company_name == company]
        if len(subset) > 6:  # More than 6 holdings for same company is suspicious
            dup_count += len(subset) - 3  # Estimate excess

    if dup_count > 0:
        print(f'Estimated excess holdings (possible prior year): ~{dup_count}')
