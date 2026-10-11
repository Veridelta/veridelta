# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The JSON the command line prints with `--json`, and a published schema for each.

`veridelta schema run` prints the JSON Schema of what `veridelta run --json`
prints, and likewise for `validate`, `crosswalk`, `suggest`, and `error`, and for the
baseline file `veridelta run --baseline` reads. `make schema`
writes each under `docs/schema/`, where the docs site serves it at the URL its
`$id` names, so an agent or a script can check what it parses.
"""

from collections.abc import Callable, Iterable
from typing import Any, Final, NotRequired, cast

from pydantic import TypeAdapter

# Pydantic builds a schema from `typing.TypedDict` only on Python 3.12 and later.
from typing_extensions import TypedDict

from veridelta.config import SCHEMA_URL
from veridelta.models import (
    Baseline,
    ConfigFinding,
    DiffSummary,
    FindingSeverity,
    RuleSuggestion,
    ValueMapProposal,
)

__all__ = [
    "OUTPUTS",
    "ErrorDetail",
    "ErrorReport",
    "FindingDetail",
    "LocationDetail",
    "ValidationReport",
    "error_report",
    "output_json_schema",
    "output_schema_url",
    "validation_report",
]

_SCHEMA_FOLDER: Final = SCHEMA_URL.rsplit("/", 1)[0]
"""Where the docs site serves each published schema, beside the configuration's."""


class LocationDetail(TypedDict):
    """Where in the configuration file a finding points.

    Attributes:
        path: The keys and indexes from the file's root to the setting, such as
            `["rules", 0, "regex_replace"]`. Left out when the finding is placed
            by line.
        line: The line, counted from 1, for a file that does not parse.
        column: The column on that line, counted from 1.
        message: What is wrong at this place, when the finding bundles several.
    """

    path: NotRequired[list[str | int]]
    line: NotRequired[int]
    column: NotRequired[int]
    message: NotRequired[str]


class FindingDetail(TypedDict):
    """One finding with where it points, for an editor or an agent.

    Attributes:
        severity: `error` or `warning`.
        message: The entry in `errors` or `warnings`.
        locations: Where in the file, when known. Empty for a problem the
            file's text does not place.
    """

    severity: FindingSeverity
    message: str
    locations: list[LocationDetail]


class ValidationReport(TypedDict):
    """What `validate_config` returns, the object `veridelta validate --json` prints.

    Attributes:
        config: The configuration file, resolved.
        valid: Whether no finding is an error.
        errors: What would stop a run.
        warnings: What a run may still trip on, such as an unset variable.
        findings: Each error, then each warning, with its severity and where
            in the file it points.
    """

    config: str
    valid: bool
    errors: list[str]
    warnings: list[str]
    findings: list[FindingDetail]


def _detail(finding: ConfigFinding) -> FindingDetail:
    """Write a finding as the report prints it, each location without its unset fields."""
    return FindingDetail(
        severity=finding.severity,
        message=finding.message,
        locations=[
            cast("LocationDetail", location.model_dump(mode="json", exclude_none=True))
            for location in finding.locations
        ],
    )


def validation_report(config: str, findings: Iterable[ConfigFinding]) -> ValidationReport:
    """Sort a check's findings into what `validate --json` prints and `validate_config` returns.

    Args:
        config (str): The configuration file, as the report names it.
        findings (Iterable[ConfigFinding]): What the check found, in order.

    Returns:
        ValidationReport: The errors and the warnings, each in the order found,
            then every finding with where it points, errors first.
    """
    found = list(findings)
    errors = [finding for finding in found if finding.severity == "error"]
    warnings = [finding for finding in found if finding.severity == "warning"]
    return ValidationReport(
        config=config,
        valid=not errors,
        errors=[finding.message for finding in errors],
        warnings=[finding.message for finding in warnings],
        findings=[_detail(finding) for finding in [*errors, *warnings]],
    )


class ErrorDetail(TypedDict):
    """Why a command could not finish.

    Attributes:
        type: The error's class, such as `ConfigError` or `ConnectorError`.
        message: What went wrong, as stderr says it.
    """

    type: str
    message: str


class ErrorReport(TypedDict):
    """The one object a command prints with `--json` when it cannot finish, at exit code 3.

    Attributes:
        error: Why it could not finish.
    """

    error: ErrorDetail


def error_report(exc: BaseException) -> ErrorReport:
    """Name a failure as `--json` prints it: its type, and its message.

    Args:
        exc (BaseException): What stopped the command.

    Returns:
        ErrorReport: The error's class name and its message, stripped.
    """
    return ErrorReport(error=ErrorDetail(type=type(exc).__name__, message=str(exc).strip()))


_OUTPUTS: Final[dict[str, tuple[str, str, Callable[[], dict[str, Any]]]]] = {
    "run": (
        "veridelta run --json",
        "The summary of a comparison: the row counts, the verdict, and the drifting columns.",
        lambda: DiffSummary.model_json_schema(mode="serialization"),
    ),
    "validate": (
        "veridelta validate --json",
        "The findings of a check: whether the file is valid, its errors, its warnings, and "
        "where in the file each points.",
        lambda: TypeAdapter(ValidationReport).json_schema(),
    ),
    "crosswalk": (
        "veridelta crosswalk --json",
        "The value maps proposed for columns that hold the same values in two encodings.",
        lambda: TypeAdapter(list[ValueMapProposal]).json_schema(mode="serialization"),
    ),
    "suggest": (
        "veridelta suggest --json",
        "The rules suggested to explain the differences, each with the rows it explains.",
        lambda: TypeAdapter(list[RuleSuggestion]).json_schema(mode="serialization"),
    ),
    "baseline": (
        "veridelta run --baseline",
        "The drift a run accepts: rows by kind and primary key, and changed columns by row.",
        Baseline.model_json_schema,
    ),
    "error": (
        "An error printed with --json",
        "The one object a command prints with `--json` when it cannot finish, at exit code 3.",
        lambda: TypeAdapter(ErrorReport).json_schema(),
    ),
}

OUTPUTS: Final = tuple(_OUTPUTS)
"""The outputs with a published schema, by the name `veridelta schema` takes."""


def output_schema_url(name: str) -> str:
    """Return the URL the docs site serves an output's schema at, which is its `$id`.

    Args:
        name (str): `run`, `validate`, `crosswalk`, `suggest`, `baseline`, or `error`.

    Returns:
        str: The schema's URL.
    """
    return f"{_SCHEMA_FOLDER}/{name}.schema.json"


def output_json_schema(name: str) -> dict[str, Any]:
    """Return the JSON Schema of what a command prints with `--json`.

    Args:
        name (str): `run`, `validate`, `crosswalk`, `suggest`, `baseline`, or `error`.

    Returns:
        dict[str, Any]: A Draft 2020-12 JSON Schema, with the `$id` the docs
            site serves it at.

    Raises:
        KeyError: If `name` is not one of `OUTPUTS`.
    """
    title, description, build = _OUTPUTS[name]
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": output_schema_url(name),
        **build(),
        "title": title,
        "description": description,
    }
