"""Check ARCC holdings by as_of_date."""

import sys
sys.path.insert(0, 'src')
from lookthrough.ingestion.parse_bdc_filing import parse_bdc_filing
import pandas as pd

holdings_df, _, _ = parse_bdc_filing('ARCC_10K_2026.html')

print('=== ARCC Holdings by as_of_date ===')
date_counts = holdings_df.groupby('as_of_date', dropna=False).agg({
    'reported_holding_id': 'count',
    'reported_value_usd': 'sum'
}).rename(columns={'reported_holding_id': 'count', 'reported_value_usd': 'total_value'})

for date, row in date_counts.iterrows():
    date_str = date if pd.notna(date) else 'Unknown'
    print(f'{date_str}: {int(row["count"]):5d} holdings, Total: ${row["total_value"]/1e9:.2f}B')

print(f'\nTotal: {len(holdings_df)} holdings')
print(f'Unique companies: {holdings_df.raw_company_name.nunique()}')
print(f'Median holding: ${holdings_df.reported_value_usd.median()/1e6:.1f}M')

# Check for header rows that should have been filtered
header_names = ['Company', 'Portfolio Company', 'Issuer', '']
warnings = []
for name in header_names:
    count = len(holdings_df[holdings_df.raw_company_name == name])
    if count > 0:
        warnings.append(f'{count} rows with name "{name}"')

if warnings:
    print('\nWARNINGS:')
    for w in warnings:
        print(f'  {w}')
else:
    print('\nNo header rows found (filtering worked)')
