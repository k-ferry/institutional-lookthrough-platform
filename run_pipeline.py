"""
End-to-end pipeline runner for the Institutional Look-Through Platform.

Supports both PostgreSQL (default) and CSV data modes:
- Default: Read/write from PostgreSQL database
- CSV mode: Use --csv flag for backward compatibility with CSV files

Usage:
    python run_pipeline.py              # full pipeline using PostgreSQL
    python run_pipeline.py --csv        # full pipeline using CSV files
    python run_pipeline.py --classify   # with AI classification (requires ANTHROPIC_API_KEY)
    python run_pipeline.py --classify --limit 50  # classify up to 50 companies
    python run_pipeline.py --13f        # with 13F SEC EDGAR ingestion
    python run_pipeline.py --pdf        # with PDF fund document ingestion
    python run_pipeline.py --13f --pdf --classify  # all optional steps
"""
import argparse
import os
import subprocess
import sys
import time

from src.lookthrough.db.repository import ensure_tables


STEPS = [
    # Synthetic data generation disabled — real fund data only
    # {
    #     'name': 'Synthetic Data Generation',
    #     'cmd': [sys.executable, '-m', 'src.lookthrough.synthetic.generate'],
    #     'always': True,
    # },
    {
        'name': 'Load Data Sources',
        'cmd': [sys.executable, '-m', 'src.lookthrough.ingestion.load_sources'],
        'always': True,
    },
    {
        'name': '13F Ingestion',
        'cmd': [sys.executable, '-m', 'src.lookthrough.ingestion.parse_13f_filing'],
        'always': False,
        'flag': 'thirteenf',
    },
    {
        'name': 'PDF Document Ingestion',
        'cmd': [sys.executable, '-m', 'src.lookthrough.ingestion.ingest_pdf_documents'],
        'always': False,
        'flag': 'pdf',
    },
    {
        'name': 'Entity Resolution',
        'cmd': [sys.executable, '-m', 'src.lookthrough.inference.entity_resolution'],
        'always': True,
    },
    {
        'name': 'Company Consolidation',
        'cmd': [sys.executable, '-m', 'src.lookthrough.inference.entity_resolution', '--consolidate'],
        'always': True,
    },
    {
        'name': 'GICS Write-back',
        'cmd': [sys.executable, '-m', 'src.lookthrough.ai.gics_writeback'],
        'always': True,
    },
    {
        'name': 'Exposure Inference',
        'cmd': [sys.executable, '-m', 'src.lookthrough.inference.exposure'],
        'always': True,
    },
    {
        'name': 'AI Classification (GICS)',
        'cmd': [sys.executable, '-m', 'src.lookthrough.ai.classify_companies'],
        'always': False,
    },
    {
        'name': 'AI Classification (Country)',
        'cmd': [sys.executable, '-m', 'src.lookthrough.ai.classify_companies', '--taxonomy-type', 'country'],
        'always': False,
    },
    {
        'name': 'GICS Sector Mapping',
        'cmd': [sys.executable, '-m', 'src.lookthrough.ai.map_to_gics'],
        'always': False,
    },
    {
        'name': 'Aggregation',
        'cmd': [sys.executable, '-m', 'src.lookthrough.inference.aggregate'],
        'always': True,
    },
    {
        'name': 'LP Exposure Scaling',
        'cmd': [sys.executable, '-m', 'src.lookthrough.inference.scale_exposure'],
        'always': True,
    },
    {
        'name': 'Review Queue',
        'cmd': [sys.executable, '-m', 'src.lookthrough.governance.review_queue'],
        'always': True,
    },
    {
        'name': 'Audit Trail',
        'cmd': [sys.executable, '-m', 'src.lookthrough.governance.audit'],
        'always': True,
    },
]


def main():
    parser = argparse.ArgumentParser(description='Run the look-through exposure pipeline.')
    parser.add_argument('--classify', action='store_true', help='Include AI classification step (requires ANTHROPIC_API_KEY)')
    parser.add_argument('--limit', type=int, default=20, help='Max companies to classify (default: 20)')
    parser.add_argument('--csv', action='store_true', help='Use CSV mode instead of PostgreSQL')
    parser.add_argument('--13f', action='store_true', dest='thirteenf', help='Include 13F SEC EDGAR ingestion step')
    parser.add_argument('--pdf', action='store_true', dest='pdf', help='Include PDF fund document ingestion step')
    args = parser.parse_args()

    # Set CSV_MODE environment variable so all modules can access it
    if args.csv:
        os.environ['CSV_MODE'] = '1'
    else:
        os.environ.pop('CSV_MODE', None)

    print('=' * 60)
    print('Institutional Look-Through Platform — Pipeline Run')
    print('=' * 60)
    print(f"Data mode: {'CSV' if args.csv else 'PostgreSQL'}")

    # Initialize database tables (if using PostgreSQL mode)
    if not args.csv:
        print('\n--- Database Initialization ---')
        try:
            ensure_tables()
            print('Database tables created/verified.')
        except Exception as e:
            print(f'WARNING: Could not initialize database: {e}')
            print('Falling back to CSV mode.')
            os.environ['CSV_MODE'] = '1'
            args.csv = True

    for step in STEPS:
        if not step['always']:
            flag_name = step.get('flag', 'classify')
            if not getattr(args, flag_name, False):
                flag_arg = '--13f' if flag_name == 'thirteenf' else f'--{flag_name}'
                print(f'\nSkipping: {step["name"]} (use {flag_arg} to enable)')
                continue

        cmd = list(step['cmd'])

        # Add --csv flag if in CSV mode
        if args.csv:
            cmd.append('--csv')

        # Add classification limit if applicable
        if step['name'].startswith('AI Classification') and args.classify:
            cmd.extend(['--limit', str(args.limit)])

        print(f'\n--- {step["name"]} ---')
        start = time.time()
        result = subprocess.run(cmd, cwd='.', capture_output=False)
        elapsed = time.time() - start

        if result.returncode != 0:
            print(f'FAILED: {step["name"]} (exit code {result.returncode})')
            sys.exit(1)

        print(f'Completed: {step["name"]} ({elapsed:.1f}s)')

    print('\n' + '=' * 60)
    print('Pipeline complete.')
    print('=' * 60)


if __name__ == '__main__':
    main()
