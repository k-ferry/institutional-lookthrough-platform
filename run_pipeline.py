"""
End-to-end pipeline runner for the Institutional Look-Through Platform.

Usage:
    python run_pipeline.py              # full pipeline (no AI classification)
    python run_pipeline.py --classify   # full pipeline with AI classification (requires ANTHROPIC_API_KEY)
    python run_pipeline.py --classify --limit 50  # classify up to 50 companies
"""
import argparse
import subprocess
import sys
import time


STEPS = [
    {
        'name': 'Synthetic Data Generation',
        'cmd': [sys.executable, '-m', 'src.lookthrough.synthetic.generate'],
        'always': True,
    },
    {
        'name': 'Exposure Inference',
        'cmd': [sys.executable, '-m', 'src.lookthrough.inference.exposure'],
        'always': True,
    },
    {
        'name': 'AI Classification',
        'cmd': [sys.executable, '-m', 'src.lookthrough.ai.classify_companies'],
        'always': False,
    },
    {
        'name': 'Aggregation',
        'cmd': [sys.executable, '-m', 'src.lookthrough.inference.aggregate'],
        'always': True,
    },
]


def main():
    parser = argparse.ArgumentParser(description='Run the look-through exposure pipeline.')
    parser.add_argument('--classify', action='store_true', help='Include AI classification step (requires ANTHROPIC_API_KEY)')
    parser.add_argument('--limit', type=int, default=20, help='Max companies to classify (default: 20)')
    args = parser.parse_args()

    print('=' * 60)
    print('Institutional Look-Through Platform — Pipeline Run')
    print('=' * 60)

    for step in STEPS:
        if not step['always'] and not args.classify:
            print(f'\nSkipping: {step["name"]} (use --classify to enable)')
            continue

        cmd = list(step['cmd'])
        if step['name'] == 'AI Classification' and args.classify:
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
