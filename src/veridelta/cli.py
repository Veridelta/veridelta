# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The `veridelta` command line.

Progress and diagnostics go to stderr, and only the requested result goes to
stdout. So `veridelta run --json | jq` needs no filtering, and the rules that
`veridelta crosswalk` prints can be redirected straight into a file. Under
`--json`, a command that cannot finish prints its error there as one JSON
object, and exits with `EXIT_ERROR` rather than the code for drift.
"""

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from veridelta import __version__
from veridelta.config import config_json_schema, load_config
from veridelta.engine import DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_SUPPORT, DiffEngine
from veridelta.exceptions import ConfigError, VerideltaError
from veridelta.models import ConfigFinding
from veridelta.report import DEFAULT_MAX_ROWS, write_html, write_markdown
from veridelta.telemetry import write_otlp_metrics

if TYPE_CHECKING:
    from veridelta.models import DiffRule, ValueMapProposal

EXIT_MATCH = 0
"""Datasets agreed within `threshold`."""

EXIT_MISMATCH = 1
"""Datasets drifted, or `validate` found an error. CI treats it as failure."""

EXIT_ERROR = 3
"""The command could not finish, such as on a configuration error or an unreachable source."""


def _progress(message: str, *, quiet: bool) -> None:
    """Write a progress line to stderr."""
    if not quiet:
        print(message, file=sys.stderr)


def _row_limit(text: str) -> int:
    """Parse a row cap, such as `--html-max-rows`, which must be a whole number of zero or more."""
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a whole number, got {text!r}") from None
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
    """Parse `--sample-fraction`, a share above zero and at most one."""
    value = _number(text)
    if not 0 < value <= 1:
        raise argparse.ArgumentTypeError(f"must be above 0 and at most 1, got {text!r}")
    return value


def _support(text: str) -> int:
    """Parse `--min-support`, a whole number of at least one."""
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a whole number, got {text!r}") from None
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def _report_failure(exc: Exception, *, as_json: bool) -> int:
    """Explain on stderr why a command stopped, and as JSON on stdout under `--json`.

    Args:
        exc (Exception): What stopped the command.
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
        error = {"type": type(exc).__name__, "message": str(exc).strip()}
        print(json.dumps({"error": error}, indent=2))
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

        _progress("Executing semantic diff...", quiet=quiet)
        result = DiffEngine.run_from_configs(diff_config, source_config, target_config)
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

        if args.otel:
            metrics_file = write_otlp_metrics(
                result,
                args.otel,
                config_path=args.config,
                source=source_config,
                target=target_config,
            )
            _progress(f"OpenTelemetry metrics saved to: {metrics_file.absolute()}", quiet=quiet)

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


def schema(args: argparse.Namespace) -> int:
    """Print the JSON Schema for configuration files on stdout.

    Args:
        args (argparse.Namespace): Parsed arguments; the command takes none.

    Returns:
        int: `EXIT_MATCH`.
    """
    _ = args
    print(json.dumps(config_json_schema(), indent=2))
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
    unset: list[str] | None = [] if args.allow_missing_env else None
    try:
        diff_config, source_config, target_config = load_config(args.config, unset_env=unset)
        findings = DiffEngine.check_configs(
            diff_config, source_config, target_config, schemas=bool(args.schemas)
        )
    except ConfigError as exc:
        findings = [ConfigFinding(severity="error", message=str(exc).strip())]
    except Exception as exc:
        return _report_failure(exc, as_json=bool(args.json))
    unset_findings = [
        ConfigFinding(
            severity="warning",
            message=(
                f"Environment variable '{name}' is not set, so its references were checked "
                f"as the text '{name}'."
            ),
        )
        for name in unset or []
    ]
    findings = [*unset_findings, *findings]

    errors = [finding.message for finding in findings if finding.severity == "error"]
    warnings = [finding.message for finding in findings if finding.severity == "warning"]
    if args.json:
        report = {
            "config": args.config,
            "valid": not errors,
            "errors": errors,
            "warnings": warnings,
        }
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
        description="Compare two datasets under declared rules, locally or inside the warehouse.",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"veridelta {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    # Every command but `schema` reads a configuration file.
    config = argparse.ArgumentParser(add_help=False)
    config.add_argument(
        "-c",
        "--config",
        default="veridelta.yaml",
        help="Path to the YAML configuration file (default: veridelta.yaml).",
    )

    run_parser = subparsers.add_parser("run", parents=[config], help="Run a Veridelta comparison.")
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
        help="Also write the run's metrics to PATH as OTLP/JSON, for an OpenTelemetry collector.",
    )

    crosswalk_parser = subparsers.add_parser(
        "crosswalk",
        parents=[config],
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
        help="Print the proposals and their evidence as JSON on stdout instead of YAML, or the "
        "error when they cannot be computed.",
    )
    crosswalk_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress and evidence on stderr.",
    )
    validate_parser = subparsers.add_parser(
        "validate",
        parents=[config],
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
    subparsers.add_parser(
        "schema",
        help="Print the JSON Schema for configuration files, for editors and validators.",
    )
    return parser


def main() -> None:
    """Run the command named on the command line and exit with its status code."""
    args = build_parser().parse_args()

    # Built at call time, so a patched handler is the one dispatched.
    commands: dict[str, Callable[[argparse.Namespace], int]] = {
        "run": run,
        "crosswalk": crosswalk,
        "validate": validate,
        "schema": schema,
    }
    sys.exit(commands[args.command](args))


if __name__ == "__main__":
    main()
