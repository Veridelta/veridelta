#!/usr/bin/env python3
# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface for the Veridelta execution engine.

This module provides the entry point for terminal execution, argument parsing,
and graceful error handling to ensure seamless integration with CI/CD pipelines.
"""

import argparse
import sys


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        args (list[str] | None): Optional list of arguments to parse.

    Returns:
        argparse.Namespace: The parsed namespace containing CLI parameters.
    """
    import importlib.metadata

    try:
        __version__ = importlib.metadata.version("veridelta")
    except importlib.metadata.PackageNotFoundError:
        __version__ = "unknown"

    parser = argparse.ArgumentParser(
        prog="veridelta",
        description="Veridelta: Semantic diffing for mission-critical data pipelines.",
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show the installed version of Veridelta and exit.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Execute a comparison pipeline.")
    run_parser.add_argument(
        "-c",
        "--config",
        type=str,
        required=True,
        help="Path to the Veridelta YAML configuration file.",
    )

    return parser.parse_args(args)


def run(args: argparse.Namespace) -> int:
    """Execute the core Veridelta CLI workflow.

    Args:
        args (argparse.Namespace): The parsed command-line arguments.

    Returns:
        int: Exit code (0 for match/success, 1 for mismatch/error).
    """
    from veridelta.config import load_config
    from veridelta.engine import DiffEngine
    from veridelta.exceptions import ConfigError

    try:
        print(f"Loading configuration from {args.config}...")
        diff_cfg, src_cfg, tgt_cfg = load_config(args.config)

        print("Ingesting and aligning datasets...")
        summary = DiffEngine(diff_cfg).execute(src_cfg, tgt_cfg)

        print("\n" + summary.report_summary)

        if summary.is_match:
            return 0
        else:
            if getattr(diff_cfg, "output_path", None):
                print(f"\nArtifacts saved to: {diff_cfg.output_path}")
            return 1

    except ConfigError as e:
        print(f"\nConfiguration Error:\n{e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"\nUnexpected System Error:\n{e}", file=sys.stderr)
        return 1


def main() -> None:
    """Primary entry point for the command-line interface."""
    args = parse_args()

    if args.command == "run":
        sys.exit(run(args))


if __name__ == "__main__":
    main()
