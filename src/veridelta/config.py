# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Configuration parsing and validation from YAML files.

This module acts as the bridge between user-defined YAML configurations
and the strict Pydantic models required by the execution engine.
"""

import os
import re
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import ConfigDict, TypeAdapter, ValidationError

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

_SOURCE_REF_ADAPTER: TypeAdapter[SourceRef] = TypeAdapter(
    SourceRef, config=ConfigDict(hide_input_in_errors=True)
)
"""Validator for `source` and `target` blocks. It hides the raw input in errors,
because the blocks carry credentials and an unknown `type` fails before any
model, and its own `hide_input_in_errors`, is chosen."""


_ENV_REFERENCE = re.compile(
    r"(?P<escape>\$\$\{)"
    r"|\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>(?:[^$}]|\$(?!\{))*))?\}"
    r"|(?P<malformed>\$\{)"
)
"""An escaped `$${`, a `${NAME}` or `${NAME:-default}` reference, or any other
`${`, tried in that order. A default cannot contain `}` or `${`, so a nested
reference is reported as malformed rather than half-expanded."""


def _substitute(match: re.Match[str], location: str) -> str:
    """Resolve one `_ENV_REFERENCE` match.

    Errors name the variable and where it is used, never the surrounding text,
    which may be a literal credential.

    Args:
        match (re.Match[str]): Match inside a string of a `source` or `target` block.
        location (str): Path to that string, used in error messages.

    Returns:
        str: A literal `${`, the variable's value, or the reference's default.

    Raises:
        ConfigError: If the reference is malformed, or names an unset variable
            and has no default.
    """
    if match["escape"]:
        return "${"
    name = match["name"]
    if name is None:
        raise ConfigError(
            f"Malformed environment reference in {location}. "
            "Write ${NAME} or ${NAME:-default}, and $${ for a literal ${."
        )
    value = os.environ.get(name)
    default = match["default"]
    if default is not None and not value:
        return default
    if value is None:
        raise ConfigError(
            f"Environment variable '{name}' is not set, but {location} references it. "
            "Set it, or write ${NAME:-default} to give a fallback."
        )
    return value


def _expand_env(value: Any, location: str, parents: tuple[int, ...] = ()) -> Any:
    """Expand environment references in the strings of a `source` or `target` block.

    Mapping keys and non-string values are returned as they are, and substituted
    text is never scanned again, so a secret containing `${` arrives intact. New
    containers are built rather than the parsed YAML edited, because a YAML
    anchor can share one mapping between both blocks.

    Args:
        value (Any): Parsed YAML value.
        location (str): Path to `value`, such as `source -> password`.
        parents (tuple[int, ...]): Ids of the containers enclosing `value`.

    Returns:
        Any: `value` with every reference replaced.

    Raises:
        ConfigError: If a reference is malformed or names an unset variable
            without a default, or a YAML alias makes a container hold itself.
    """
    if id(value) in parents:
        raise ConfigError(
            f"The YAML alias at {location} refers to a mapping or list that contains it."
        )
    if isinstance(value, dict):
        mapping = cast("dict[object, object]", value)
        inner = (*parents, id(mapping))
        return {
            key: _expand_env(item, f"{location} -> {key}", inner) for key, item in mapping.items()
        }
    if isinstance(value, list):
        items = cast("list[object]", value)
        inner = (*parents, id(items))
        return [
            _expand_env(item, f"{location} -> {index}", inner) for index, item in enumerate(items)
        ]
    if isinstance(value, str):
        return _ENV_REFERENCE.sub(lambda match: _substitute(match, location), value)
    return value


def _parse_source_ref(raw: Any, *, label: str) -> SourceRef:
    """Validate a YAML source/target block as a discriminated `SourceRef`.

    Environment references in the block's strings are expanded first.

    Args:
        raw (Any): Parsed YAML mapping for the block.
        label (str): `source` or `target`, used in error messages.

    Returns:
        SourceRef: File, warehouse, or lakehouse configuration.

    Raises:
        ConfigError: If the block is not a mapping, or an environment reference
            in it is malformed or names an unset variable.
        ValidationError: If the block fails schema validation.
    """
    if not isinstance(raw, dict):
        raise ConfigError(f"The '{label}' block must be a mapping.")
    payload: dict[str, Any] = _expand_env(raw, label)
    if "type" not in payload:
        payload["type"] = "file"
    return _SOURCE_REF_ADAPTER.validate_python(payload)


def load_config(path: str | Path) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Loads and validates a Veridelta configuration from a YAML file.

    The parser extracts the explicit `source` and `target` definition blocks,
    then evaluates all remaining root-level YAML parameters as the master
    `DiffConfig`. File sources may omit `type` (defaults to `file`).

    Strings inside `source` and `target` may reference environment variables
    as `${NAME}`, or `${NAME:-default}` to fall back when the variable is
    unset or empty, so credentials can stay out of the file. `$${` writes a
    literal `${`. Root settings and rules are read verbatim, which keeps a
    `${1}` in a regex replacement intact.

    Args:
        path (str | Path): The file system path to the YAML configuration.

    Returns:
        tuple[DiffConfig, SourceRef, SourceRef]: Master configuration plus
            validated source and target references.

    Raises:
        ConfigError: If the file cannot be located, contains invalid YAML syntax,
            lacks the mandatory source/target blocks, references an unset
            environment variable or holds a malformed reference, or violates
            the strict Pydantic schema definitions.
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
