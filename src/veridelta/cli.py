# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface for Veridelta.

This module provides the terminal entry points for running Veridelta
comparisons in CI/CD pipelines and local environments.
"""

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from veridelta.config import load_config
from veridelta.engine import DataIngestor, DiffEngine
from veridelta.exceptions import ConfigError

if TYPE_CHECKING:
    from veridelta.models import DiffConfig, SourceConfig


def run(args: argparse.Namespace) -> int:
    """Executes the comparison workflow based on CLI arguments.

    Args:
        args (argparse.Namespace): The parsed command-line arguments containing
            the path to the configuration file.

    Returns:
        int: The system exit code. Returns 0 for a successful match within
            the configured threshold, and 1 for failures or system errors.
    """
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

        print(f"\n{summary.report_summary}\n")

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
    """Main entry point for the Veridelta CLI.

    Parses arguments and dispatches to the appropriate command handler.
    Exits the system with the returned status code to integrate seamlessly
    with pipeline orchestrators.
    """
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
