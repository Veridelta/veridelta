# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Configuration files: loading, checking, and their JSON Schema.

`load_config` reads a YAML file into the models the engine runs on, and
`config_json_schema` describes the same files for editors and validators.
"""

import os
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import ConfigDict, TypeAdapter, ValidationError

from veridelta.exceptions import ConfigError
from veridelta.models import (
    BigQueryConfig,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DuckDBConfig,
    IcebergConfig,
    SnowflakeConfig,
    SourceRef,
)

__all__ = [
    "SCHEMA_URL",
    "BigQueryConfig",
    "DatabaseConfig",
    "DatabricksConfig",
    "DeltaLakeConfig",
    "DuckDBConfig",
    "IcebergConfig",
    "SnowflakeConfig",
    "SourceRef",
    "config_json_schema",
    "load_config",
    "referenced_variables",
]

# The blocks carry credentials, and an unknown `type` fails before `SourceRef` picks a
# model, so no model's own `hide_input_in_errors` applies.
_SOURCE_REF_ADAPTER: TypeAdapter[SourceRef] = TypeAdapter(
    SourceRef, config=ConfigDict(hide_input_in_errors=True)
)
"""Validator for `source` and `target` blocks."""


_ENV_REFERENCE = re.compile(
    r"(?P<escape>\$\$\{)"
    # A default cannot hold `}` or `${`, so a nested reference is malformed, not half-expanded.
    r"|\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>(?:[^$}]|\$(?!\{))*))?\}"
    r"|(?P<malformed>\$\{)"
)
"""An escaped `$${`, a `${NAME}` or `${NAME:-default}` reference, or any other `${`."""


def _substitute(match: re.Match[str], location: str, unset: list[str] | None) -> str:
    """Resolve one `_ENV_REFERENCE` match."""
    # Errors never quote the surrounding text, which may be a literal credential.
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
    if value is None and unset is not None:
        if name not in unset:
            unset.append(name)
        return name
    if value is None:
        raise ConfigError(
            f"Environment variable '{name}' is not set, but {location} references it. "
            "Set it, or write ${NAME:-default} to give a fallback."
        )
    return value


def _expand_env(
    value: Any, location: str, parents: tuple[int, ...] = (), unset: list[str] | None = None
) -> Any:
    """Expand environment references in the strings of a `source` or `target` block."""
    if id(value) in parents:
        raise ConfigError(
            f"The YAML alias at {location} refers to a mapping or list that contains it."
        )
    # A YAML anchor can share one mapping between both blocks, so an in-place edit reaches both.
    if isinstance(value, dict):
        mapping = cast("dict[object, object]", value)
        inner = (*parents, id(mapping))
        return {
            key: _expand_env(item, f"{location} -> {key}", inner, unset)
            for key, item in mapping.items()
        }
    if isinstance(value, list):
        items = cast("list[object]", value)
        inner = (*parents, id(items))
        return [
            _expand_env(item, f"{location} -> {index}", inner, unset)
            for index, item in enumerate(items)
        ]
    if isinstance(value, str):
        # `sub` never rescans substituted text, so a secret holding `${` arrives intact.
        return _ENV_REFERENCE.sub(lambda match: _substitute(match, location, unset), value)
    return value


def referenced_variables(text: str) -> list[str]:
    """Name each environment variable a configuration's text references, once, in order.

    The text is read as written, so a file that does not load names its
    variables too, and a reference outside `source` and `target`, which the
    loader leaves as it is, counts as well.

    Args:
        text (str): The text of a configuration file.

    Returns:
        list[str]: The name in each `${NAME}` or `${NAME:-default}`. An escaped
            `$${` names none.

    Examples:
        >>> referenced_variables("{password: ${PASSWORD}, role: '${ROLE:-ANALYST}$${X}'}")
        ['PASSWORD', 'ROLE']
    """
    names: list[str] = []
    for match in _ENV_REFERENCE.finditer(text):
        name = match["name"]
        if name is not None and name not in names:
            names.append(name)
    return names


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


def _accept_env_reference(prop: dict[str, Any]) -> dict[str, Any]:
    """Let a constrained string field also hold a `${NAME}` reference."""
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
            # Text restricted by `pattern` or `enum`, directly or in an `anyOf`.
            options: list[dict[str, Any]] = prop.get("anyOf", [prop])
            if name != "type" and any("pattern" in o or "enum" in o for o in options):
                branch["properties"][name] = _accept_env_reference(prop)
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_URL,
        **schema,
        "title": "Veridelta configuration",
        "description": "A Veridelta comparison: primary keys, rules, and the source and target.",
    }


def _validation_failure(
    error: ValidationError,
    unset: Sequence[str] = (),
    *,
    block: str | None = None,
    tag: object = None,
) -> ConfigError:
    """Format a Pydantic failure as the loader's `ConfigError`.

    Args:
        error (ValidationError): The failure.
        unset (Sequence[str]): Environment variables the block named that were not set.
        block (str | None): The block the failure is in, `source` or `target`, which then
            opens each location in place of the `type` tag Pydantic puts there.
        tag (object): The block's `type`, which Pydantic names first in a location inside
            the chosen model, and not at all when no model matches it.
    """
    message = "Configuration Validation Failed:\n"
    for validation_error in error.errors():
        parts = list(validation_error["loc"])
        if block is not None:
            parts = [block, *(parts[1:] if parts and parts[0] == tag else parts)]
        location = " -> ".join(str(part) for part in parts)
        message += f"  - [{location}]: {validation_error['msg']}\n"
    if unset:
        message += (
            "  Values in this block came from unset environment variables: "
            f"{', '.join(unset)}. Each was read as its own name, which may be what "
            "failed; set them to check the block as it will run.\n"
        )
    return ConfigError(message)


def _parse_source_ref(raw: Any, *, label: str, unset: list[str] | None = None) -> SourceRef:
    """Validate a YAML source/target block as a discriminated `SourceRef`."""
    if not isinstance(raw, dict):
        raise ConfigError(f"The '{label}' block must be a mapping.")
    guessed: list[str] | None = None if unset is None else []
    payload: dict[str, Any] = _expand_env(raw, label, unset=guessed)
    if unset is not None and guessed:
        unset.extend(name for name in guessed if name not in unset)
    payload.setdefault("type", "file")
    try:
        return _SOURCE_REF_ADAPTER.validate_python(payload)
    except ValidationError as e:
        raise _validation_failure(e, guessed or (), block=label, tag=payload["type"]) from e


def load_config(
    path: str | Path, *, unset_env: list[str] | None = None
) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Load and validate a configuration file.

    The `source` and `target` blocks become source configurations, and every other
    root key belongs to the `DiffConfig`. A file source may omit `type`, which
    defaults to `file`.

    Strings inside `source` and `target` may reference environment variables as
    `${NAME}`, or `${NAME:-default}` to fall back when the variable is unset or
    empty, so credentials can stay out of the file. `$${` writes a literal `${`.
    Root settings and rules are read verbatim, which keeps a `${1}` in a regex
    replacement intact.

    Passing a list as `unset_env` checks a file without its secrets: an unset
    variable with no default then reads as its own name, so `${TABLE}` becomes
    `TABLE`, and its name is appended to the list once. A block that fails
    validation after such a guess says which variables it guessed.

    Args:
        path (str | Path): Path to the YAML file.
        unset_env (list[str] | None): Collects unset variables instead of raising
            for them. None, the default, raises.

    Returns:
        tuple[DiffConfig, SourceRef, SourceRef]: The comparison settings, then the
            source and the target.

    Raises:
        ConfigError: If the file is missing or not valid YAML, lacks a `source` or
            `target` block, references an unset environment variable, holds a
            malformed reference, or fails validation.

    Examples:
        >>> from pathlib import Path
        >>> from tempfile import TemporaryDirectory
        >>> text = "{primary_keys: [id], source: {path: a.csv}, target: {path: b.csv}}"
        >>> with TemporaryDirectory() as folder:
        ...     path = Path(folder, "veridelta.yaml")
        ...     _ = path.write_text(text)
        ...     diff, source, target = load_config(path)
        >>> diff.primary_keys, source.path
        (['id'], 'a.csv')
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

    source_cfg = _parse_source_ref(raw_config.pop("source"), label="source", unset=unset_env)
    target_cfg = _parse_source_ref(raw_config.pop("target"), label="target", unset=unset_env)
    try:
        diff_cfg = DiffConfig.model_validate(raw_config)
    except ValidationError as e:
        raise _validation_failure(e) from e
    return diff_cfg, source_cfg, target_cfg
