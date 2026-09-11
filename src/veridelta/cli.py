# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface for Veridelta.

This module provides the terminal entry points for running Veridelta
comparisons in CI/CD pipelines and local environments.

Progress and diagnostics go to stderr; only the requested result goes to
stdout. That keeps `veridelta run --json | jq` working without the caller
having to strip chatter out of the stream first.
"""

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from veridelta import __version__
from veridelta.config import load_config
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError, VerideltaError
from veridelta.report import DEFAULT_MAX_ROWS, write_html

if TYPE_CHECKING:
    from veridelta.models import DiffConfig, SourceRef

EXIT_MATCH = 0
"""Datasets agreed within `threshold`."""

EXIT_MISMATCH = 1
"""Datasets drifted, or the run could not complete. CI treats both as failure."""


def _progress(message: str, *, quiet: bool) -> None:
    """Write a progress line to stderr.

    Args:
        message (str): Line to emit.
        quiet (bool): Suppress the line entirely.
    """
    if not quiet:
        print(message, file=sys.stderr)


def run(args: argparse.Namespace) -> int:
    """Executes the comparison workflow based on CLI arguments.

    Args:
        args (argparse.Namespace): Parsed command-line arguments carrying the
            config path and the output options.

    Returns:
        int: `EXIT_MATCH` when the comparison falls within `threshold`,
            `EXIT_MISMATCH` for drift or any failure.
    """
    # `--json` changes what stdout carries; `--quiet` controls stderr. Keeping
    # them separate means `run --json` can still report where it wrote files.
    quiet = bool(args.quiet)

    try:
        _progress(f"Loading configuration from {args.config}...", quiet=quiet)
        diff_config: DiffConfig
        source_config: SourceRef
        target_config: SourceRef
        diff_config, source_config, target_config = load_config(args.config)

        _progress("Executing semantic diff...", quiet=quiet)
        result = DiffEngine.run_from_configs(diff_config, source_config, target_config)
        summary = result.summary

        if args.json:
            print(summary.model_dump_json(indent=2))
        else:
            print(f"\n{summary.report_summary}")

        if diff_config.output_path and summary.artifacts_written:
            _progress(
                f"Artifacts saved to: {Path(diff_config.output_path).absolute()}", quiet=quiet
            )

        if args.html:
            written = write_html(result, args.html, max_rows=args.html_max_rows)
            _progress(f"HTML report saved to: {written.absolute()}", quiet=quiet)

        return EXIT_MATCH if summary.is_match else EXIT_MISMATCH

    except ConfigError as e:
        print(f"\nConfiguration Error\n{e}", file=sys.stderr)
        print(
            "\nThis is a problem with the configuration file, not with the data. "
            "Correct the setting above and run again.",
            file=sys.stderr,
        )
        return EXIT_MISMATCH
    except VerideltaError as e:
        print(f"\n{type(e).__name__}\n{e}", file=sys.stderr)
        return EXIT_MISMATCH
    except Exception as e:
        # Anything reaching here came from Polars or a driver, where the message
        # alone rarely says what the user should do about it.
        print(f"\nUnexpected System Error\n{type(e).__name__}: {e}", file=sys.stderr)
        print(
            "\nThis is a bug or an unsupported input. Please report it at "
            "https://github.com/Veridelta/veridelta/issues with the configuration "
            "file and this message.",
            file=sys.stderr,
        )
        return EXIT_MISMATCH


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser.

    Returns:
        argparse.ArgumentParser: Parser covering every subcommand and flag.
    """
    parser = argparse.ArgumentParser(
        prog="veridelta",
        description="Semantic diffing for mission-critical data pipelines.",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"veridelta {__version__}",
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
    run_parser.add_argument(
        "--json",
        action="store_true",
        help="Print the summary as JSON on stdout instead of a formatted report.",
    )
    run_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress messages on stderr.",
    )
    run_parser.add_argument(
        "--html",
        type=str,
        default=None,
        metavar="PATH",
        help="Also write a standalone HTML report to PATH.",
    )
    run_parser.add_argument(
        "--html-max-rows",
        type=int,
        default=DEFAULT_MAX_ROWS,
        metavar="N",
        help=f"Rows to embed per HTML table before truncating (default: {DEFAULT_MAX_ROWS}).",
    )
    return parser


def main() -> None:
    """Main entry point for the Veridelta CLI.

    Parses arguments and dispatches to the appropriate command handler.
    Exits the system with the returned status code to integrate seamlessly
    with pipeline orchestrators.
    """
    args = build_parser().parse_args()

    if args.command == "run":
        sys.exit(run(args))


if __name__ == "__main__":
    main()
