"""Run BDC parser on all filings and show combined summary."""

import sys
sys.path.insert(0, 'src')

from lookthrough.ingestion.parse_bdc_filing import parse_bdc_filing
import pandas as pd

all_holdings = []
all_funds = []

filenames = ['MAIN_10K_2025.html', 'OBDC_10K_2025.html', 'ARCC_10K_2026.html']

for filename in filenames:
    print(f'\nProcessing {filename}...')
    holdings_df, fund_df, _ = parse_bdc_filing(filename)
    all_holdings.append(holdings_df)
    all_funds.append((filename, len(holdings_df), holdings_df.reported_value_usd.sum()))

print('\n' + '='*60)
print('SUMMARY OF ALL BDC FILINGS')
print('='*60)

for filename, count, total in all_funds:
    ticker = filename.split('_')[0]
    print(f'{ticker:6s}: {count:5d} holdings, Total: ${total/1e9:.2f}B')

combined = pd.concat(all_holdings, ignore_index=True)
print(f'\nCOMBINED: {len(combined)} holdings, Total: ${combined.reported_value_usd.sum()/1e9:.2f}B')
print(f'Median holding value: ${combined.reported_value_usd.median()/1e6:.1f}M')
print(f'Unique companies: {combined.raw_company_name.nunique()}')

# Save combined
combined.to_csv('data/silver/bdc_fact_reported_holding.csv', index=False)
print(f'\nSaved combined holdings to data/silver/bdc_fact_reported_holding.csv')
