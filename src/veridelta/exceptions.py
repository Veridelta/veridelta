# Copyright 2026 Nicholas Harder
# SPDX-License-Identifier: Apache-2.0

"""Custom exceptions for Veridelta operations."""


class VerideltaError(Exception):
    """Base exception for all Veridelta errors."""

    pass


class ConfigError(VerideltaError):
    """Raised when configuration validation or schema enforcement fails."""

    pass


class DataIntegrityError(VerideltaError):
    """Raised when primary keys are not unique or data types are incompatible."""

    pass
