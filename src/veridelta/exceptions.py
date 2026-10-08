# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The errors Veridelta raises.

Each derives from `VerideltaError`, so one `except` clause catches them all.
"""


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
    """


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
