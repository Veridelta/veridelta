# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Custom exceptions for Veridelta operations.

This module defines the core exception hierarchy used throughout the
Veridelta framework. Consumers of the Python API can catch the base
`VerideltaError` to safely handle all framework-specific failures.
"""


class VerideltaError(Exception):
    """Base exception for all Veridelta-specific errors.

    Consumers should catch this exception to handle pipeline validation
    failures gracefully without silencing standard Python runtime errors
    (like `MemoryError` or `ValueError`).
    """


class ConfigError(VerideltaError):
    """Raised when configuration validation or schema enforcement fails.

    This is triggered during pipeline initialization or schema validation
    if mandatory parameters (like primary keys) are missing, or if strict
    schema constraints (e.g., `allow_removals`, `exact`) are violated by
    the provided datasets.
    """


class DataIntegrityError(VerideltaError):
    """Raised when foundational data assumptions are violated.

    This is typically raised during the pre-evaluation phase if primary
    keys are not unique within either dataset. Halting execution on this
    error prevents catastrophic join explosions and Out-Of-Memory (OOM)
    crashes during the Polars evaluation phase.
    """
