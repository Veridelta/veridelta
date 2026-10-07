# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The `veridelta` command line.

Progress and diagnostics go to stderr, and only the requested result goes to
stdout. So `veridelta run --json | jq` needs no filtering, and the rules that
`veridelta crosswalk` and `veridelta suggest` print can be redirected straight into a file. Under
`--json`, a command that cannot finish prints its error there as one JSON
object, and exits with `EXIT_ERROR` rather than the code for drift. With
`--verbose`, Veridelta's own log records join the progress on stderr.
"""

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from polars.exceptions import PanicException

from veridelta import __version__
from veridelta.config import config_json_schema, load_config
from veridelta.engine import (
    DEFAULT_MAX_SHARE,
    DEFAULT_MIN_CONFIDENCE,
    DEFAULT_MIN_SUPPORT,
    DiffEngine,
)
from veridelta.exceptions import ConfigError, VerideltaError
from veridelta.mcp_server import DEFAULT_ROW_CAP, Settings, serve
from veridelta.models import Baseline
from veridelta.outputs import OUTPUTS, error_report, output_json_schema, validation_report
from veridelta.report import DEFAULT_MAX_ROWS, write_html, write_markdown
from veridelta.telemetry import send_otlp_metrics, write_otlp_metrics

if TYPE_CHECKING:
    from veridelta.models import DiffRule, RuleSuggestion, ValueMapProposal

EXIT_MATCH = 0
"""Datasets agreed within `threshold`."""

EXIT_MISMATCH = 1
"""Datasets drifted, or `validate` found an error. CI treats it as failure."""

EXIT_ERROR = 3
"""The command could not finish, such as on a configuration error or an unreachable source."""

_LOG_FORMAT = "%(levelname)s %(name)s: %(message)s"
"""How `--verbose` prints a record: its level, the logger that wrote it, and the message."""


def _progress(message: str, *, quiet: bool) -> None:
    """Write a progress line to stderr."""
    if not quiet:
        print(message, file=sys.stderr)


@contextmanager
def _verbose_logging(enabled: bool) -> Generator[None, None, None]:
    """Print Veridelta's own log records to stderr at INFO while a command runs.

    Only the `veridelta` logger gets the handler, so the drivers' loggers stay
    quiet. The handler is built here, so it writes to the `sys.stderr` of the
    moment. Removing it afterwards means a second command in the same process
    prints each record once.
    """
    if not enabled:
        yield
        return
    logger = logging.getLogger("veridelta")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)


def _whole_number(text: str) -> int:
    """Parse a whole number."""
    try:
        return int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a whole number, got {text!r}") from None


def _row_limit(text: str) -> int:
    """Parse a row cap, such as `--html-max-rows`, which must be a whole number of zero or more."""
    value = _whole_number(text)
    if value < 0:
        raise argparse.ArgumentTypeError(f"must be zero or more, got {value}")
    return value


def _number(text: str) -> float:
    """Parse a numeric threshold."""
    try:
        return float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a number, got {text!r}") from None


def _confidence(text: str) -> float:
    """Parse `--min-confidence`, a share above one half and at most one."""
    value = _number(text)
    if not 0.5 < value <= 1:
        raise argparse.ArgumentTypeError(f"must be above 0.5 and at most 1, got {text!r}")
    return value


def _share(text: str) -> float:
    """Parse a share above zero and at most one, such as `--sample-fraction`."""
    value = _number(text)
    if not 0 < value <= 1:
        raise argparse.ArgumentTypeError(f"must be above 0 and at most 1, got {text!r}")
    return value


def _at_least_one(text: str) -> int:
    """Parse a whole number of at least one, such as `--min-support` or `--max-rows`."""
    value = _whole_number(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def _directory(text: str) -> Path:
    """Parse `--root`, a folder that exists, resolved."""
    path = Path(text).resolve()
    if not path.is_dir():
        raise argparse.ArgumentTypeError(f"not a directory: {text!r}")
    return path


def _report_failure(exc: BaseException, *, as_json: bool) -> int:
    """Explain on stderr why a command stopped, and as JSON on stdout under `--json`.

    Args:
        exc (BaseException): What stopped the command.
        as_json (bool): Whether stdout carries JSON, so the error goes there as
            one object too: `{"error": {"type": ..., "message": ...}}`.

    Returns:
        int: `EXIT_ERROR`.
    """
    if isinstance(exc, ConfigError):
        message = (
            f"\nConfiguration Error\n{exc}\n\nThis is a problem with the configuration "
            "file, not with the data. Correct the setting above and run again."
        )
    elif isinstance(exc, VerideltaError):
        message = f"\n{type(exc).__name__}\n{exc}"
    else:
        # Anything reaching here came from Polars or a driver, where the message
        # alone rarely says what the user should do about it.
        message = (
            f"\nUnexpected System Error\n{type(exc).__name__}: {exc}\n\nThis is a bug or "
            "an unsupported input. Please report it at "
            "https://github.com/Veridelta/veridelta/issues with the configuration file "
            "and this message."
        )
    print(message, file=sys.stderr)
    if as_json:
        print(json.dumps(error_report(exc), indent=2))
    return EXIT_ERROR


def run(args: argparse.Namespace) -> int:
    """Run the comparison a configuration file describes.

    Args:
        args (argparse.Namespace): Parsed command-line arguments carrying the
            config path and the output options.

    Returns:
        int: `EXIT_MATCH` when the comparison falls within `threshold`,
            `EXIT_MISMATCH` for drift, and `EXIT_ERROR` when it cannot finish.
    """
    # `--json` changes what stdout carries; `--quiet` controls stderr. Keeping
    # them separate means `run --json` can still report where it wrote files.
    quiet = bool(args.quiet)

    try:
        _progress(f"Loading configuration from {args.config}...", quiet=quiet)
        diff_config, source_config, target_config = load_config(args.config)

        baseline = None if args.baseline is None else Baseline.read(args.baseline)
        _progress("Comparing...", quiet=quiet)
        result = DiffEngine.run_from_configs(
            diff_config, source_config, target_config, baseline=baseline
        )
        summary = result.summary

        if not args.json:
            print(f"\n{summary.report_summary}")

        if diff_config.output_path and summary.artifacts_written:
            _progress(
                f"Artifacts saved to: {Path(diff_config.output_path).absolute()}", quiet=quiet
            )

        if args.html:
            written = write_html(result, args.html, max_rows=args.html_max_rows)
            _progress(f"HTML report saved to: {written.absolute()}", quiet=quiet)

        if args.markdown:
            summary_file = write_markdown(result, args.markdown, max_rows=args.markdown_max_rows)
            _progress(f"Markdown summary saved to: {summary_file.absolute()}", quiet=quiet)

        # One timestamp, so the file and the export sent are the same bytes.
        observed = time.time_ns()
        if args.otel:
            metrics_file = write_otlp_metrics(
                result,
                args.otel,
                config_path=args.config,
                source=source_config,
                target=target_config,
                time_unix_nano=observed,
            )
            _progress(f"OpenTelemetry metrics saved to: {metrics_file.absolute()}", quiet=quiet)

        if args.otel_send:
            endpoint = send_otlp_metrics(
                result,
                config_path=args.config,
                source=source_config,
                target=target_config,
                time_unix_nano=observed,
            )
            _progress(f"OpenTelemetry metrics sent to: {endpoint}", quiet=quiet)

        if args.json:
            # Last, so a file that cannot be written leaves only its error on stdout.
            print(summary.model_dump_json(indent=2))
        return EXIT_MATCH if summary.is_match else EXIT_MISMATCH

    except Exception as exc:
        return _report_failure(exc, as_json=bool(args.json))


def _merge_advice(rule: "DiffRule", column: str) -> str:
    """Say where a proposed map for `column` belongs, given the rule governing it."""
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
    """Explain on stderr what was proposed and why."""
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
            # This note ignores `quiet`: only one rule governs a column, so pasting the
            # printed rule as it stands changes nothing.
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
            found, and `EXIT_ERROR` when they cannot be.
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
        return _report_failure(exc, as_json=bool(args.json))

    if args.json:
        print(json.dumps([proposal.model_dump(mode="json") for proposal in proposals], indent=2))
        return EXIT_MATCH

    _report_proposals(proposals, diff_config.rules, quiet=quiet)
    if proposals:
        rules = [
            proposal.to_rule().model_dump(mode="json", exclude_none=True, exclude_defaults=True)
            for proposal in proposals
        ]
        # YAML quotes any value it would read as another type, such as `Y` or `1`.
        print(yaml.safe_dump({"rules": rules}, sort_keys=False), end="")
    return EXIT_MATCH


def _key_text(keys: dict[str, object]) -> str:
    """Name one row by its primary keys, such as `id=3`."""
    return ", ".join(f"{name}={value!r}" for name, value in keys.items())


def _setting_text(value: object) -> str:
    """Write one suggested setting's value as the YAML rule writes it, such as `0.005`."""
    if isinstance(value, float):
        return f"{value:g}"
    return value if isinstance(value, str) else json.dumps(value)


def _report_suggestions(suggestions: Sequence["RuleSuggestion"], *, quiet: bool) -> None:
    """Explain on stderr what each suggested rule explains, and where it goes."""
    if not suggestions:
        _progress("No rule explains the differences within --max-share.", quiet=quiet)
    for suggestion in suggestions:
        settings = ", ".join(
            f"{name} {_setting_text(value)}" for name, value in suggestion.settings.items()
        )
        gap = (
            ""
            if suggestion.largest_gap is None
            else f", the largest gap {suggestion.largest_gap:.6g}"
        )
        _progress(
            f"{suggestion.column}: {settings} explains {suggestion.explained:,} of "
            f"{suggestion.differing:,} differing rows{gap}",
            quiet=quiet,
        )
        examples = "; ".join(_key_text(keys) for keys in suggestion.examples)
        _progress(f"  for example {examples}", quiet=quiet)
        index = suggestion.governing_rule_index
        if index is not None:
            # This note ignores `quiet`, as crosswalk's does: a rule pasted after the one
            # that governs the column changes nothing.
            print(
                f"Note: rules[{index}] governs '{suggestion.column}' today. The rule below "
                "keeps its settings for this column: put it first in rules.",
                file=sys.stderr,
            )


def suggest(args: argparse.Namespace) -> int:
    """Suggest rules that would explain the differences between the configured datasets.

    Args:
        args (argparse.Namespace): Parsed command-line arguments carrying the
            config path, `max_share`, and the output options.

    Returns:
        int: `EXIT_MATCH` once suggestions are computed, whether or not any were
            found, and `EXIT_ERROR` when they cannot be.
    """
    quiet = bool(args.quiet)
    try:
        _progress(f"Loading configuration from {args.config}...", quiet=quiet)
        diff_config, source_config, target_config = load_config(args.config)

        _progress("Comparing, and trying each rule...", quiet=quiet)
        suggestions = DiffEngine.suggest_rules_from_configs(
            diff_config, source_config, target_config, max_share=args.max_share
        )
    except Exception as exc:
        return _report_failure(exc, as_json=bool(args.json))

    if args.json:
        print(json.dumps([item.model_dump(mode="json") for item in suggestions], indent=2))
        return EXIT_MATCH

    _report_suggestions(suggestions, quiet=quiet)
    if suggestions:
        rules = [
            item.rule.model_dump(mode="json", exclude_none=True, exclude_defaults=True)
            for item in suggestions
        ]
        print(yaml.safe_dump({"rules": rules}, sort_keys=False), end="")
    return EXIT_MATCH


def schema(args: argparse.Namespace) -> int:
    """Print a JSON Schema on stdout: the configuration file's, or one output's.

    Args:
        args (argparse.Namespace): Parsed arguments carrying `output`: `run`,
            `validate`, `crosswalk`, `suggest`, `baseline`, or `error` for what that file holds
            with `--json`, or None for configuration files.

    Returns:
        int: `EXIT_MATCH`.
    """
    printed = config_json_schema() if args.output is None else output_json_schema(args.output)
    print(json.dumps(printed, indent=2))
    return EXIT_MATCH


def mcp(args: argparse.Namespace) -> int:
    """Serve the MCP tools over stdio until the agent's host disconnects.

    The tools read configuration files only under the `--root` folders, or
    the current directory when none is given. The server runs in the first,
    so a relative path in a tool call or in a configuration file resolves
    there, as it would for a person running the command line in it. Stdout
    carries the protocol, so this command prints nothing else there. Values
    from the data stay out of every result unless `--allow-row-values` is
    given, and then a call returns at most `--max-rows` of them.

    Args:
        args (argparse.Namespace): Parsed arguments carrying the roots and the
            row-value settings.

    Returns:
        int: `EXIT_MATCH` once the host disconnects or Ctrl-C stops the server,
            and `EXIT_ERROR` when it cannot start, such as without the `mcp`
            extra.
    """
    try:
        settings = Settings(
            tuple(args.root or [Path.cwd()]),
            allow_row_values=args.allow_row_values,
            max_rows=args.max_rows,
            allow_queries=args.allow_queries,
        )
        os.chdir(settings.roots[0])
        serve(settings)
    except KeyboardInterrupt:
        # Ctrl-C is how a person stops a server they started by hand.
        return EXIT_MATCH
    except Exception as exc:
        return _report_failure(exc, as_json=False)
    return EXIT_MATCH


def _plural(count: int, noun: str) -> str:
    """Write a count with its noun, such as `1 error` or `2 warnings`."""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def validate(args: argparse.Namespace) -> int:
    """Check a configuration for what would stop a run, without reading any rows.

    Offline by default. With `--schemas`, it also connects and checks the rules
    against each side's stored columns. Findings go to stdout, one `error:` or
    `warning:` line each, or as one JSON object with `--json`. The verdict goes
    to stderr.

    Args:
        args (argparse.Namespace): Parsed arguments carrying the config path,
            `schemas`, `allow_missing_env`, `json`, and `quiet`.

    Returns:
        int: `EXIT_MATCH` when there are no errors, warnings or not,
            `EXIT_MISMATCH` when there is one, and `EXIT_ERROR` when the
            check cannot finish.
    """
    try:
        findings = DiffEngine.check_config_file(
            args.config,
            schemas=bool(args.schemas),
            allow_missing_env=bool(args.allow_missing_env),
        )
    except Exception as exc:
        return _report_failure(exc, as_json=bool(args.json))

    report = validation_report(args.config, findings)
    errors, warnings = report["errors"], report["warnings"]
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        for finding in findings:
            print(f"{finding.severity}: {finding.message}")

    if errors:
        verdict = f"{_plural(len(errors), 'error')}, {_plural(len(warnings), 'warning')}."
    elif warnings:
        verdict = f"valid, with {_plural(len(warnings), 'warning')}."
    else:
        verdict = "valid."
    _progress(f"{args.config}: {verdict}", quiet=bool(args.quiet))
    return EXIT_MISMATCH if errors else EXIT_MATCH


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser.

    Returns:
        argparse.ArgumentParser: Parser covering every subcommand and flag.
    """
    parser = argparse.ArgumentParser(
        prog="veridelta",
        description=(
            "Compare two datasets on their primary keys under rules you declare, "
            "on a laptop, in CI, or inside a warehouse."
        ),
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"veridelta {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    # `run`, `crosswalk`, `suggest`, and `validate` read the configuration file named here.
    config = argparse.ArgumentParser(add_help=False)
    config.add_argument(
        "-c",
        "--config",
        default="veridelta.yaml",
        help="Path to the YAML configuration file (default: veridelta.yaml).",
    )
    # Every command but `schema` reads data, so it can log what it reads.
    verbose = argparse.ArgumentParser(add_help=False)
    verbose.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Log connections, reads, and statements to stderr, with timings. "
        "No line holds a credential or SQL.",
    )

    run_parser = subparsers.add_parser(
        "run", parents=[config, verbose], help="Run a Veridelta comparison."
    )
    run_parser.add_argument(
        "--json",
        action="store_true",
        help="Print the summary as JSON on stdout instead of a formatted report, or the error "
        "when the run cannot finish.",
    )
    run_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress messages on stderr.",
    )
    run_parser.add_argument(
        "--baseline",
        metavar="PATH",
        help="Accept the drift the JSON file PATH lists, and fail only on drift it does not.",
    )
    run_parser.add_argument(
        "--html",
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
    run_parser.add_argument(
        "--markdown",
        metavar="PATH",
        help="Also write a short Markdown summary to PATH, for CI job summaries and PR comments.",
    )
    run_parser.add_argument(
        "--markdown-max-rows",
        type=_row_limit,
        default=0,
        metavar="N",
        help=(
            "Changed values to list in the Markdown summary, lowest keys first "
            "(default: 0, which lists none)."
        ),
    )
    run_parser.add_argument(
        "--otel",
        metavar="PATH",
        help="Also write the run's metrics to the file PATH as OTLP/JSON. --otel-send sends them.",
    )
    run_parser.add_argument(
        "--otel-send",
        action="store_true",
        help=(
            "Also send the run's metrics as OTLP/JSON to the OTLP/HTTP endpoint the "
            "OTEL_EXPORTER_OTLP_* variables set (default: http://localhost:4318/v1/metrics)."
        ),
    )

    crosswalk_parser = subparsers.add_parser(
        "crosswalk",
        parents=[config, verbose],
        help="Propose value_map rules from how source and target values line up.",
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
        type=_at_least_one,
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
        help="Print the proposals and their evidence as JSON on stdout instead of YAML, or the "
        "error when they cannot be computed.",
    )
    crosswalk_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress and evidence on stderr.",
    )
    suggest_parser = subparsers.add_parser(
        "suggest",
        parents=[config, verbose],
        help="Suggest rules that would explain the differences, each with its evidence.",
    )
    suggest_parser.add_argument(
        "--max-share",
        type=_share,
        default=DEFAULT_MAX_SHARE,
        metavar="SHARE",
        help=(
            "Largest gap a tolerance may explain, as a share of the larger of its two values, "
            f"above 0 and at most 1 (default: {DEFAULT_MAX_SHARE})."
        ),
    )
    suggest_parser.add_argument(
        "--json",
        action="store_true",
        help="Print the suggestions and their evidence as JSON on stdout instead of YAML, or "
        "the error when they cannot be computed.",
    )
    suggest_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress and evidence on stderr.",
    )
    validate_parser = subparsers.add_parser(
        "validate",
        parents=[config, verbose],
        help="Check a configuration for what would stop a run, without reading any rows.",
    )
    validate_parser.add_argument(
        "--schemas",
        action="store_true",
        help=(
            "Also connect, read each side's columns (never its rows), and check the rules "
            "against them."
        ),
    )
    validate_parser.add_argument(
        "--allow-missing-env",
        action="store_true",
        help=(
            "Read an unset ${NAME} as the text NAME and warn, instead of failing, so a "
            "file can be checked without its secrets."
        ),
    )
    validate_parser.add_argument(
        "--json",
        action="store_true",
        help="Print the findings as one JSON object on stdout, or the error when the check "
        "cannot finish.",
    )
    validate_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress the verdict line on stderr.",
    )
    schema_parser = subparsers.add_parser(
        "schema",
        help="Print the JSON Schema for configuration files, or for what a command prints with --json.",
    )
    schema_parser.add_argument(
        "output",
        nargs="?",
        choices=OUTPUTS,
        help="The output whose schema to print, instead of the configuration file's.",
    )
    mcp_parser = subparsers.add_parser(
        "mcp",
        parents=[verbose],
        help="Serve checks and comparisons to an AI agent as Model Context Protocol tools, over stdio.",
    )
    mcp_parser.add_argument(
        "--root",
        action="append",
        type=_directory,
        metavar="DIR",
        help=(
            "A folder the tools may read configuration files and data on this machine "
            "from. Repeat it for more folders. The server runs in the first (default: the "
            "current directory)."
        ),
    )
    mcp_parser.add_argument(
        "--allow-row-values",
        action="store_true",
        help=(
            "Let read_discrepancies and propose_value_maps return values from the data. "
            "Off by default."
        ),
    )
    mcp_parser.add_argument(
        "--allow-queries",
        action="store_true",
        help=(
            "Let a tool run a side's query, which runs as written. A DuckDB file still "
            "reads other files only from under the roots. Off by default."
        ),
    )
    mcp_parser.add_argument(
        "--max-rows",
        type=_at_least_one,
        default=DEFAULT_ROW_CAP,
        metavar="N",
        help=f"The most rows, or value map entries, one call returns (default: {DEFAULT_ROW_CAP}).",
    )
    return parser


def main() -> None:
    """Run the command named on the command line and exit with its status code."""
    args = build_parser().parse_args()

    # Built at call time, so a patched handler is the one dispatched.
    commands: dict[str, Callable[[argparse.Namespace], int]] = {
        "run": run,
        "crosswalk": crosswalk,
        "suggest": suggest,
        "validate": validate,
        "schema": schema,
        "mcp": mcp,
    }
    # `schema` reads no configuration and logs nothing, so it has no `--verbose`.
    with _verbose_logging(getattr(args, "verbose", False)):
        try:
            code = commands[args.command](args)
        except PanicException as exc:
            # A panic in Polars is no `Exception`, so no command catches it, and an
            # uncaught one would exit 1, the code for drift.
            code = _report_failure(exc, as_json=bool(getattr(args, "json", False)))
    sys.exit(code)


if __name__ == "__main__":
    main()
