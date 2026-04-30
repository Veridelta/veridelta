# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Custom exception hierarchy for Veridelta operations.

This module defines the core exceptions used throughout the framework.
API consumers can catch the base `VerideltaError` to safely handle all
domain-specific failures without masking standard Python runtime errors.
"""


class VerideltaError(Exception):
    """Base exception for all Veridelta errors.

    Notes:
        Catch this exception at the top level of integration pipelines
        to gracefully handle domain-specific execution failures.
    """


class ConfigError(VerideltaError):
    """Exception raised for configuration and schema validation failures.

    Triggered during initialization or structural alignment if mandatory
    parameters are missing or if strict schema constraints (e.g., `exact`,
    `allow_removals`) are violated.
    """


class DataIntegrityError(VerideltaError):
    """Exception raised for violations of foundational data constraints.

    Triggered during the pre-evaluation phase if primary keys are not
    strictly unique within either dataset, preventing non-deterministic
    joins and memory exhaustion.
    """
