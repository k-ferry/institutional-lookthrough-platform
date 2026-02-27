"""Analyze MAIN holdings in detail."""

import sys
sys.path.insert(0, 'src')

from lookthrough.ingestion.parse_bdc_filing import parse_bdc_filing
import pandas as pd

# Run MAIN specifically
holdings_df, fund_df, _ = parse_bdc_filing('MAIN_10K_2025.html')

print('=== MAIN Holdings Analysis ===')
print(f'Total holdings: {len(holdings_df)}')
print(f'Holdings with fair value: {holdings_df.reported_value_usd.notna().sum()}')
print(f'Holdings with null value: {holdings_df.reported_value_usd.isna().sum()}')
total = holdings_df.reported_value_usd.sum()
print(f'Total fair value: ${total/1e6:.1f}M')
print()

# Value distribution
values = holdings_df.reported_value_usd.dropna()
print('Value distribution:')
print(f'  Min: ${values.min()/1000:.0f}K')
print(f'  25th percentile: ${values.quantile(0.25)/1000:.0f}K')
print(f'  Median: ${values.median()/1000:.0f}K')
print(f'  75th percentile: ${values.quantile(0.75)/1000:.0f}K')
print(f'  Max: ${values.max()/1e6:.1f}M')
print()

# Top 10 holdings
print('Top 10 holdings:')
top = holdings_df.nlargest(10, 'reported_value_usd')
for _, row in top.iterrows():
    v = row.reported_value_usd
    print(f'  ${v/1e6:>6.1f}M  {row.raw_company_name[:45]}')
