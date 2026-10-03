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
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    IcebergConfig,
    SnowflakeConfig,
    SourceRef,
)

__all__ = [
    "SCHEMA_URL",
    "DatabaseConfig",
    "DatabricksConfig",
    "DeltaLakeConfig",
    "IcebergConfig",
    "SnowflakeConfig",
    "SourceRef",
    "config_json_schema",
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


SCHEMA_URL = "https://veridelta.github.io/veridelta/schema/veridelta.schema.json"
"""Where the docs site publishes the configuration schema for editors to fetch."""

_ENV_REFERENCE_SCHEMA: dict[str, Any] = {"type": "string", "pattern": "\\$\\{"}
"""Any string holding a `${NAME}` reference, which the loader expands before validating."""

_ANNOTATIONS = ("title", "description", "default")
"""Keys a wrapped property keeps on the outside, where editors read them."""


class _RootConfig(DiffConfig):
    """The whole file: root settings plus the `source` and `target` blocks."""

    source: SourceRef
    target: SourceRef


def _is_constrained_string(prop: dict[str, Any]) -> bool:
    """Return whether a property restricts its text by `pattern` or `enum`.

    Args:
        prop (dict[str, Any]): Property schema, possibly an `anyOf` of options.

    Returns:
        bool: True when the property or one of its options carries either.
    """
    options: list[dict[str, Any]] = prop.get("anyOf", [prop])
    return any("pattern" in option or "enum" in option for option in options)


def _accept_env_reference(prop: dict[str, Any]) -> dict[str, Any]:
    """Let a constrained string field also hold a `${NAME}` reference.

    Args:
        prop (dict[str, Any]): Property schema with a `pattern` or `enum`.

    Returns:
        dict[str, Any]: The same constraint, or any string with a reference.
    """
    outer = {key: prop[key] for key in _ANNOTATIONS if key in prop}
    inner = {key: value for key, value in prop.items() if key not in _ANNOTATIONS}
    return {**outer, "anyOf": [inner, _ENV_REFERENCE_SCHEMA]}


def config_json_schema() -> dict[str, Any]:
    """Return a JSON Schema for configuration files, for editors and validators.

    It is generated from the same models `load_config` validates with, then
    adjusted where the loader does something before validating:

    - `type` is required in every warehouse, lakehouse, and database block,
      because the loader reads a block without one as a file source.
    - Patterned and enumerated strings inside `source` and `target`, such as
      `table`, also accept a `${NAME}` reference, which the loader expands.

    The schema is stricter than the loader in one way: it does not model
    Pydantic's lax coercion, so a quoted number such as `threshold: "0.1"` is
    flagged even though it loads.

    Returns:
        dict[str, Any]: A Draft 2020-12 JSON Schema.
    """
    schema = _RootConfig.model_json_schema()
    definitions: dict[str, dict[str, Any]] = schema["$defs"]
    branches: dict[str, str] = schema["properties"]["source"]["discriminator"]["mapping"]
    for tag, ref in branches.items():
        branch = definitions[ref.rsplit("/", 1)[1]]
        if tag != "file":
            branch["required"] = [*branch.get("required", []), "type"]
        for name, prop in branch["properties"].items():
            if name != "type" and _is_constrained_string(prop):
                branch["properties"][name] = _accept_env_reference(prop)
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_URL,
        **schema,
        "title": "Veridelta configuration",
        "description": "A Veridelta comparison: primary keys, rules, and the source and target.",
    }


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
