# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The JSON the command line prints with `--json`, and a published schema for each.

`veridelta schema run` prints the JSON Schema of what `veridelta run --json`
prints, and likewise for `validate`, `crosswalk`, `suggest`, and `error`. `make schema`
writes each under `docs/schema/`, where the docs site serves it at the URL its
`$id` names, so an agent or a script can check what it parses.
"""

from collections.abc import Callable, Iterable
from typing import Any, Final

from pydantic import TypeAdapter

# Pydantic builds a schema from `typing.TypedDict` only on Python 3.12 and later.
from typing_extensions import TypedDict

from veridelta.config import SCHEMA_URL
from veridelta.models import ConfigFinding, DiffSummary, RuleSuggestion, ValueMapProposal

__all__ = [
    "OUTPUTS",
    "ErrorDetail",
    "ErrorReport",
    "ValidationReport",
    "error_report",
    "output_json_schema",
    "output_schema_url",
    "validation_report",
]

_SCHEMA_FOLDER: Final = SCHEMA_URL.rsplit("/", 1)[0]
"""Where the docs site serves each published schema, beside the configuration's."""


class ValidationReport(TypedDict):
    """What `validate_config` returns, the object `veridelta validate --json` prints.

    Attributes:
        config: The configuration file, resolved.
        valid: Whether no finding is an error.
        errors: What would stop a run.
        warnings: What a run may still trip on, such as an unset variable.
    """

    config: str
    valid: bool
    errors: list[str]
    warnings: list[str]


def validation_report(config: str, findings: Iterable[ConfigFinding]) -> ValidationReport:
    """Sort a check's findings into what `validate --json` prints and `validate_config` returns.

    Args:
        config (str): The configuration file, as the report names it.
        findings (Iterable[ConfigFinding]): What the check found, in order.

    Returns:
        ValidationReport: The errors and the warnings, each in the order found.
    """
    found = list(findings)
    errors = [finding.message for finding in found if finding.severity == "error"]
    warnings = [finding.message for finding in found if finding.severity == "warning"]
    return ValidationReport(config=config, valid=not errors, errors=errors, warnings=warnings)


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
        "The findings of a check: whether the file is valid, its errors, and its warnings.",
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
        name (str): `run`, `validate`, `crosswalk`, `suggest`, or `error`.

    Returns:
        str: The schema's URL.
    """
    return f"{_SCHEMA_FOLDER}/{name}.schema.json"


def output_json_schema(name: str) -> dict[str, Any]:
    """Return the JSON Schema of what a command prints with `--json`.

    Args:
        name (str): `run`, `validate`, `crosswalk`, `suggest`, or `error`.

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
