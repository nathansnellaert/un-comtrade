import argparse
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment
from ingest import trade_data as ingest_trade


def main():
    parser = argparse.ArgumentParser(description="UN Comtrade Connector")
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_trade.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        # Transforms will be added after profiling
        print("  No transforms implemented yet")


if __name__ == "__main__":
    main()
