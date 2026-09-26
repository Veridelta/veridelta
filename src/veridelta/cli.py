# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface for Veridelta.

This module provides the terminal entry points for running Veridelta
comparisons in CI/CD pipelines and local environments.

Progress and diagnostics go to stderr; only the requested result goes to
stdout. That keeps `veridelta run --json | jq` working without the caller
having to strip chatter out of the stream first, and lets the rules that
`veridelta crosswalk` prints be redirected straight into a file.
"""

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from veridelta import __version__
from veridelta.config import load_config
from veridelta.engine import DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_SUPPORT, DiffEngine
from veridelta.exceptions import ConfigError, VerideltaError
from veridelta.report import DEFAULT_MAX_ROWS, write_html

if TYPE_CHECKING:
    from veridelta.models import DiffConfig, DiffRule, SourceRef, ValueMapProposal

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


def _row_limit(text: str) -> int:
    """Parse `--html-max-rows`, which must be a whole number of zero or more.

    Args:
        text (str): Raw argument value.

    Returns:
        int: The row limit.

    Raises:
        argparse.ArgumentTypeError: If the value is not a non-negative integer.
    """
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a whole number, got {text!r}") from None
    if value < 0:
        raise argparse.ArgumentTypeError(f"must be zero or more, got {value}")
    return value


def _number(text: str) -> float:
    """Parse a numeric threshold.

    Args:
        text (str): Raw argument value.

    Returns:
        float: The parsed number.

    Raises:
        argparse.ArgumentTypeError: If the value is not a number.
    """
    try:
        return float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a number, got {text!r}") from None


def _confidence(text: str) -> float:
    """Parse `--min-confidence`, a share above one half and at most one.

    Args:
        text (str): Raw argument value.

    Returns:
        float: The confidence floor.

    Raises:
        argparse.ArgumentTypeError: If the value is out of range, NaN included.
    """
    value = _number(text)
    if not 0.5 < value <= 1:
        raise argparse.ArgumentTypeError(f"must be above 0.5 and at most 1, got {text!r}")
    return value


def _share(text: str) -> float:
    """Parse `--sample-fraction`, a share above zero and at most one.

    Args:
        text (str): Raw argument value.

    Returns:
        float: The share of source rows to read.

    Raises:
        argparse.ArgumentTypeError: If the value is out of range, NaN included.
    """
    value = _number(text)
    if not 0 < value <= 1:
        raise argparse.ArgumentTypeError(f"must be above 0 and at most 1, got {text!r}")
    return value


def _support(text: str) -> int:
    """Parse `--min-support`, a whole number of at least one.

    Args:
        text (str): Raw argument value.

    Returns:
        int: The agreeing rows a proposal needs.

    Raises:
        argparse.ArgumentTypeError: If the value is not a positive integer.
    """
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a whole number, got {text!r}") from None
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def _report_failure(exc: Exception) -> int:
    """Explain on stderr why a command stopped.

    Args:
        exc (Exception): What stopped the command.

    Returns:
        int: `EXIT_MISMATCH`, since CI treats a failure like drift.
    """
    if isinstance(exc, ConfigError):
        print(f"\nConfiguration Error\n{exc}", file=sys.stderr)
        print(
            "\nThis is a problem with the configuration file, not with the data. "
            "Correct the setting above and run again.",
            file=sys.stderr,
        )
    elif isinstance(exc, VerideltaError):
        print(f"\n{type(exc).__name__}\n{exc}", file=sys.stderr)
    else:
        # Anything reaching here came from Polars or a driver, where the message
        # alone rarely says what the user should do about it.
        print(f"\nUnexpected System Error\n{type(exc).__name__}: {exc}", file=sys.stderr)
        print(
            "\nThis is a bug or an unsupported input. Please report it at "
            "https://github.com/Veridelta/veridelta/issues with the configuration "
            "file and this message.",
            file=sys.stderr,
        )
    return EXIT_MISMATCH


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

    except Exception as exc:
        return _report_failure(exc)


def _proposal_yaml(proposals: Sequence["ValueMapProposal"]) -> str:
    """Render proposals as a `rules:` block to paste into a configuration.

    Args:
        proposals (Sequence[ValueMapProposal]): Proposals to render.

    Returns:
        str: YAML whose `rules` hold one rule per proposed column. Values that
            YAML would read as another type, such as `Y` or `1`, are quoted.
    """
    rules = [
        proposal.to_rule().model_dump(mode="json", exclude_none=True, exclude_defaults=True)
        for proposal in proposals
    ]
    rendered: str = yaml.safe_dump({"rules": rules}, sort_keys=False)
    return rendered


def _merge_advice(rule: "DiffRule", column: str) -> str:
    """Say where a proposed map for `column` belongs, given the rule governing it.

    Only one rule governs a column, so a second rule pasted after it changes
    nothing. Merging into a rule that governs other columns too would apply
    the map to all of them, so such a rule has to give the column up first.

    Args:
        rule (DiffRule): Rule that governs the column today.
        column (str): Column the proposal is for.

    Returns:
        str: One sentence of advice.
    """
    if len(rule.column_names) > 1 or (rule.pattern is not None and rule.column_names):
        return (
            f"That rule also governs other columns, so move '{column}' into a rule of its "
            "own with the same settings and add these entries there."
        )
    if rule.pattern is not None:
        return (
            f"That rule matches by pattern, so add a rule listing '{column}' by name, with "
            "the same settings and these entries: a rule naming a column wins over a pattern."
        )
    return "Add these entries to its value_map instead of pasting a second rule."


def _report_proposals(
    proposals: Sequence["ValueMapProposal"], rules: Sequence["DiffRule"], *, quiet: bool
) -> None:
    """Explain on stderr what was proposed and why.

    The evidence is progress chatter, so `--quiet` drops it. A note that a
    rule already governs a column is printed regardless, since pasting the
    printed rule as it stands would then change nothing.

    Args:
        proposals (Sequence[ValueMapProposal]): Proposals to explain.
        rules (Sequence[DiffRule]): The configuration's rules.
        quiet (bool): Suppress the evidence.
    """
    if not proposals:
        _progress("No value_map entries met the thresholds.", quiet=quiet)
    for proposal in proposals:
        count = len(proposal.entries)
        _progress(
            f"{proposal.column}: {count} new value_map {'entry' if count == 1 else 'entries'}",
            quiet=quiet,
        )
        for entry in proposal.entries:
            _progress(
                f"  {entry.source_value!r} -> {entry.target_value!r}: "
                f"{entry.agreeing_rows:,} of {entry.rows:,} rows ({entry.confidence:.1%})",
                quiet=quiet,
            )
        index = proposal.governing_rule_index
        if index is not None:
            print(
                f"Note: rules[{index}] already governs '{proposal.column}'. "
                + _merge_advice(rules[index], proposal.column),
                file=sys.stderr,
            )


def crosswalk(args: argparse.Namespace) -> int:
    """Propose `value_map` rules from how the configured datasets line up.

    Args:
        args (argparse.Namespace): Parsed command-line arguments carrying the
            config path, the thresholds, and the output options.

    Returns:
        int: `EXIT_MATCH` once proposals are computed, whether or not any were
            found, and `EXIT_MISMATCH` for any failure.
    """
    quiet = bool(args.quiet)
    try:
        _progress(f"Loading configuration from {args.config}...", quiet=quiet)
        diff_config, source_config, target_config = load_config(args.config)

        _progress("Lining up source and target values...", quiet=quiet)
        proposals = DiffEngine.propose_value_maps_from_configs(
            diff_config,
            source_config,
            target_config,
            min_confidence=args.min_confidence,
            min_support=args.min_support,
            sample_fraction=args.sample_fraction,
        )
    except Exception as exc:
        return _report_failure(exc)

    if args.json:
        print(json.dumps([proposal.model_dump(mode="json") for proposal in proposals], indent=2))
        return EXIT_MATCH

    _report_proposals(proposals, diff_config.rules, quiet=quiet)
    if proposals:
        print(_proposal_yaml(proposals), end="")
    return EXIT_MATCH


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
        type=_row_limit,
        default=DEFAULT_MAX_ROWS,
        metavar="N",
        help=f"Rows to embed per HTML table before truncating (default: {DEFAULT_MAX_ROWS}).",
    )

    crosswalk_parser = subparsers.add_parser(
        "crosswalk",
        help="Propose value_map rules from how source and target values line up.",
    )
    crosswalk_parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="veridelta.yaml",
        help="Path to the YAML configuration file (default: veridelta.yaml)",
    )
    crosswalk_parser.add_argument(
        "--min-confidence",
        type=_confidence,
        default=DEFAULT_MIN_CONFIDENCE,
        metavar="SHARE",
        help=(
            "Share of a source value's rows that must agree on one target value, above 0.5 "
            f"(default: {DEFAULT_MIN_CONFIDENCE})."
        ),
    )
    crosswalk_parser.add_argument(
        "--min-support",
        type=_support,
        default=DEFAULT_MIN_SUPPORT,
        metavar="N",
        help=f"Agreeing rows an entry needs (default: {DEFAULT_MIN_SUPPORT}).",
    )
    crosswalk_parser.add_argument(
        "--sample-fraction",
        type=_share,
        default=1.0,
        metavar="SHARE",
        help="Share of source rows to read, chosen by primary key (default: 1.0).",
    )
    crosswalk_parser.add_argument(
        "--json",
        action="store_true",
        help="Print the proposals and their evidence as JSON on stdout instead of YAML.",
    )
    crosswalk_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress and evidence on stderr.",
    )
    return parser


def main() -> None:
    """Main entry point for the Veridelta CLI.

    Parses arguments and dispatches to the appropriate command handler.
    Exits the system with the returned status code to integrate seamlessly
    with pipeline orchestrators.
    """
    args = build_parser().parse_args()

    # Built at call time, so a patched handler is the one dispatched.
    commands: dict[str, Callable[[argparse.Namespace], int]] = {
        "run": run,
        "crosswalk": crosswalk,
    }
    sys.exit(commands[args.command](args))


if __name__ == "__main__":
    main()
