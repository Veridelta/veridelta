# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The errors Veridelta raises.

Each derives from `VerideltaError`, so one `except` clause catches them all.
"""

from collections.abc import Sequence
from typing import NamedTuple


class ConfigLocation(NamedTuple):
    """Where in a configuration file a `ConfigError` points.

    Attributes:
        path (tuple[str | int, ...] | None): The keys and indexes from the file's
            root to the setting, such as `("rules", 0, "regex_replace")`.
        line (int | None): The line, counted from 1, when the file does not parse.
        column (int | None): The column on that line, counted from 1.
        message (str | None): What is wrong at this place, when the error
            bundles several.
    """

    path: tuple[str | int, ...] | None = None
    line: int | None = None
    column: int | None = None
    message: str | None = None


def missing_extra(extra: str, needed_for: str) -> str:
    """Return the one message for an optional extra that is not installed.

    The error that carries it stays the one its caller raises: a reader's
    `ConfigError` or a connector's `ConnectorError`.

    Args:
        extra (str): The extra, such as `snowflake`.
        needed_for (str): What needs it, as the subject of a sentence, such as
            `Connecting to Snowflake`.

    Returns:
        str: What needs the extra, and the command that installs it.
    """
    return (
        f"{needed_for} needs the optional '{extra}' extra, which is not installed. "
        f"Install it with: uv add 'veridelta[{extra}]'"
    )


class VerideltaError(Exception):
    """Base class for every error Veridelta raises.

    Catch it to handle any Veridelta failure without also catching Python's own
    errors, such as `MemoryError` or `ValueError`.
    """


class ConfigError(VerideltaError):
    """Raised when a configuration is invalid or the data breaks its rules.

    Such failures include a primary key missing from a dataset, a rule the
    column types cannot satisfy, and a column that `schema_mode` forbids.

    Attributes:
        locations (tuple[ConfigLocation, ...]): Where in the file each problem
            is, when the loader knows. Empty for a failure the file's text does
            not place, such as a missing block or a key missing from the data.
    """

    def __init__(self, message: str = "", *, locations: Sequence[ConfigLocation] = ()) -> None:
        """Carry the message, and where in the file each problem is.

        Args:
            message (str): What is wrong and what to do about it.
            locations (Sequence[ConfigLocation]): Where in the file, when known.
        """
        super().__init__(message)
        self.locations = tuple(locations)


class DataIntegrityError(VerideltaError):
    """Raised when the data breaks an assumption the comparison relies on.

    Repeated primary keys in either dataset raise it before any join runs,
    since a repeated key multiplies the joined rows.
    """


class ConnectorError(VerideltaError):
    """Raised when a source connector cannot complete an operation.

    Such failures include a missing optional extra, a failed query or scan, a
    pair of backends that cannot be compared, and a call before a session opens.
    """


class DatasetError(VerideltaError):
    """Raised when a sample dataset cannot be downloaded.

    `veridelta.datasets` fetches tutorial data over the network, and a failed
    or interrupted download raises this error.
    """
