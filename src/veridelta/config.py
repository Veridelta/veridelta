# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Configuration parsing and validation from YAML files.

This module acts as the bridge between user-defined YAML configurations
and the strict Pydantic models required by the execution engine.
"""

from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import TypeAdapter, ValidationError

from veridelta.exceptions import ConfigError
from veridelta.models import (
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    IcebergConfig,
    SnowflakeConfig,
    SourceRef,
)

__all__ = [
    "DatabricksConfig",
    "DeltaLakeConfig",
    "IcebergConfig",
    "SnowflakeConfig",
    "SourceRef",
    "load_config",
]

_SOURCE_REF_ADAPTER: TypeAdapter[SourceRef] = TypeAdapter(SourceRef)


def _parse_source_ref(raw: Any, *, label: str) -> SourceRef:
    """Validate a YAML source/target block as a discriminated `SourceRef`.

    Args:
        raw (Any): Parsed YAML mapping for the block.
        label (str): `source` or `target`, used in error messages.

    Returns:
        SourceRef: File, warehouse, or lakehouse configuration.

    Raises:
        ConfigError: If the block is not a mapping or fails schema validation.
    """
    if not isinstance(raw, dict):
        raise ConfigError(f"The '{label}' block must be a mapping.")
    payload = cast("dict[str, Any]", dict(raw))
    if "type" not in payload:
        payload["type"] = "file"
    return _SOURCE_REF_ADAPTER.validate_python(payload)


def load_config(path: str | Path) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Loads and validates a Veridelta configuration from a YAML file.

    The parser extracts the explicit `source` and `target` definition blocks,
    then evaluates all remaining root-level YAML parameters as the master
    `DiffConfig`. File sources may omit `type` (defaults to `file`).

    Args:
        path (str | Path): The file system path to the YAML configuration.

    Returns:
        tuple[DiffConfig, SourceRef, SourceRef]: Master configuration plus
            validated source and target references.

    Raises:
        ConfigError: If the file cannot be located, contains invalid YAML syntax,
            lacks the mandatory source/target blocks, or violates the strict
            Pydantic schema definitions.
    """
    file_path = Path(path)

    if not file_path.is_file():
        raise ConfigError(f"Configuration file not found or is not a file: {file_path.absolute()}")

    try:
        with file_path.open("r", encoding="utf-8") as f:
            parsed_yaml: Any = yaml.safe_load(f)
    except yaml.YAMLError as yaml_err:
        raise ConfigError(f"Failed to parse YAML file:\n{yaml_err}") from yaml_err

    if not isinstance(parsed_yaml, dict):
        raise ConfigError("Invalid YAML structure: Root element must be a dictionary.")

    raw_config = cast("dict[str, Any]", parsed_yaml)
    if "source" not in raw_config or "target" not in raw_config:
        raise ConfigError("Configuration must contain both 'source' and 'target' blocks.")

    try:
        raw_source = raw_config.pop("source")
        raw_target = raw_config.pop("target")

        source_cfg = _parse_source_ref(raw_source, label="source")
        target_cfg = _parse_source_ref(raw_target, label="target")
        diff_cfg = DiffConfig.model_validate(raw_config)

        return diff_cfg, source_cfg, target_cfg

    except ValidationError as e:
        error_msg = "Configuration Validation Failed:\n"
        for validation_error in e.errors():
            location = " -> ".join(str(loc) for loc in validation_error["loc"])
            error_msg += f"  - [{location}]: {validation_error['msg']}\n"
        raise ConfigError(error_msg) from e
