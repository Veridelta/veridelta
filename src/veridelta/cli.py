# Copyright 2026 Nicholas Harder
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface for Veridelta."""

import argparse
import sys
from pathlib import Path

from veridelta.config import load_config
from veridelta.engine import DataIngestor, DiffEngine
from veridelta.exceptions import ConfigError
from veridelta.models import DiffConfig, DiffSummary, SourceConfig


def print_summary(summary: DiffSummary) -> None:
    """Prints a formatted summary to the console."""
    print("\n" + "=" * 50)
    print("Veridelta Execution Summary")
    print("=" * 50)
    print(f"Source Rows:   {summary.total_rows_source:,}")
    print(f"Target Rows:   {summary.total_rows_target:,}")
    print("-" * 50)
    print(f"Added Rows:    {summary.added_count:,}")
    print(f"Removed Rows:  {summary.removed_count:,}")
    print(f"Changed Rows:  {summary.changed_count:,}")
    print("=" * 50)

    if summary.is_match:
        print("STATUS: SUCCESS (Matches within threshold)")
    else:
        print("STATUS: FAILED (Differences exceed threshold)")
    print("=" * 50 + "\n")


def run(args: argparse.Namespace) -> int:
    """Executes the comparison workflow."""
    config_path = args.config

    try:
        print(f"Loading configuration from {config_path}...")

        diff_config: DiffConfig
        source_config: SourceConfig
        target_config: SourceConfig
        diff_config, source_config, target_config = load_config(config_path)

        print("Ingesting and aligning datasets...")
        ingestor = DataIngestor(diff_config, source_config, target_config)
        source_df, target_df = ingestor.get_dataframes()

        print("Executing semantic diff...")
        engine = DiffEngine(diff_config, source_df, target_df)
        summary = engine.run()

        print_summary(summary)

        if diff_config.output_path and not summary.is_match:
            print(f"Artifacts saved to: {Path(diff_config.output_path).absolute()}\n")

        return 0 if summary.is_match else 1

    except ConfigError as e:
        print(f"\nConfiguration Error:\n{e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"\nUnexpected System Error:\n{e}", file=sys.stderr)
        return 1


def main() -> None:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="veridelta",
        description="Semantic diffing for mission-critical data pipelines.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a Veridelta comparison.")
    run_parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="veridelta.yaml",
        help="Path to the YAML configuration file (default: veridelta.yaml)",
    )

    args = parser.parse_args()

    if args.command == "run":
        exit_code = run(args)
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
