# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Configuration files: loading, checking, and their JSON Schema.

`load_config` reads a YAML file into the models the engine runs on, and
`config_json_schema` describes the same files for editors and validators.
"""

import os
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, cast

import yaml
from pydantic import ConfigDict, TypeAdapter, ValidationError

from veridelta.exceptions import ConfigError, ConfigLocation
from veridelta.models import DiffConfig, SourceConfig, SourceRef

__all__ = [
    "SCHEMA_URL",
    "SourceRef",
    "config_json_schema",
    "files_config",
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


_ALIAS_LIMIT = 1_000_000
"""How much text the YAML aliases in one file may repeat, a list or mapping counting one more."""


_Path = tuple[str | int, ...]
"""The keys and indexes from the file's root to a setting, as a location names them."""


def _location(path: _Path) -> str:
    """Write a path as the messages name it, such as `source -> path`."""
    return " -> ".join(str(part) for part in path)


def _step(key: object) -> str | int:
    """Return a YAML key as a path step: itself when it is text or a number, else its text."""
    return key if isinstance(key, str | int) else str(key)


@dataclass(frozen=True)
class Reference:
    """One `${NAME}` the loader expands, with where it stands and what it became."""

    name: str
    """The variable's name."""
    path: _Path
    """The setting that holds it, such as `("source", "storage_options", "token")`."""
    block_type: object
    """The block's `type` as written, `file` when it has none."""
    whole: bool
    """Whether the reference is the setting's whole text."""
    written: str
    """The setting's text, as YAML parsed it."""
    value: str | None
    """The environment's value, or None when a default or the unset name stands instead."""


OnReference = Callable[[Reference, re.Match[str]], None]
"""Called with each `${NAME}` before the loader puts its value in place."""


def _substitute(
    match: re.Match[str],
    path: _Path,
    unset: list[str] | None,
    *,
    block_type: object = "file",
    on_reference: OnReference | None = None,
) -> str:
    """Resolve one `_ENV_REFERENCE` match."""
    # Errors never quote the surrounding text, which may be a literal credential.
    if match["escape"]:
        return "${"
    name = match["name"]
    if name is None:
        raise ConfigError(
            f"Malformed environment reference in {_location(path)}. "
            "Write ${NAME} or ${NAME:-default}, and $${ for a literal ${.",
            locations=(ConfigLocation(path=path),),
        )
    value = os.environ.get(name)
    default = match["default"]
    result = value
    if default is not None and not value:
        value, result = None, default
    elif value is None and unset is not None:
        if name not in unset:
            unset.append(name)
        result = name
    if on_reference is not None:
        whole = match.group(0) == match.string
        on_reference(Reference(name, path, block_type, whole, match.string, value), match)
    if result is None:
        raise ConfigError(
            f"Environment variable '{name}' is not set, but {_location(path)} references it. "
            "Set it, or write ${NAME:-default} to give a fallback.",
            locations=(ConfigLocation(path=path),),
        )
    return result


def _expand_env(
    value: Any,
    path: _Path,
    unset: list[str] | None = None,
    *,
    block_type: object = "file",
    on_reference: OnReference | None = None,
) -> Any:
    """Expand environment references in the strings of a `source` or `target` block.

    The loader has refused any alias loop and bounded what aliases repeat, so the
    walk ends and its work grows with the file's logical size.
    """

    def expand(item: object, at: _Path) -> Any:
        return _expand_env(item, at, unset, block_type=block_type, on_reference=on_reference)

    def substitute(match: re.Match[str]) -> str:
        return _substitute(match, path, unset, block_type=block_type, on_reference=on_reference)

    # A YAML anchor can share one mapping between both blocks, so an in-place edit reaches both.
    if isinstance(value, dict):
        mapping = cast("dict[object, object]", value)
        return {key: expand(item, (*path, _step(key))) for key, item in mapping.items()}
    if isinstance(value, list):
        items = cast("list[object]", value)
        return [expand(item, (*path, index)) for index, item in enumerate(items)]
    if isinstance(value, str):
        # `sub` never rescans substituted text, so a secret holding `${` arrives intact.
        return _ENV_REFERENCE.sub(substitute, value)
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
    """Format a Pydantic failure as the loader's `ConfigError`, to raise `from None`.

    The message names each failure and where it is. The failure's own text also
    quotes each input, which can be large, so it is not chained as the cause.

    Args:
        error (ValidationError): The failure.
        unset (Sequence[str]): Environment variables the block named that were not set.
        block (str | None): The block the failure is in, `source` or `target`, which then
            opens each location in place of the `type` tag Pydantic puts there.
        tag (object): The block's `type`, which Pydantic names first in a location inside
            the chosen model, and not at all when no model matches it.
    """
    message = "Configuration Validation Failed:\n"
    locations: list[ConfigLocation] = []
    for validation_error in error.errors():
        parts = list(validation_error["loc"])
        if block is not None:
            parts = [block, *(parts[1:] if parts and parts[0] == tag else parts)]
        message += f"  - [{_location(tuple(parts))}]: {validation_error['msg']}\n"
        locations.append(ConfigLocation(path=tuple(parts), message=validation_error["msg"]))
    if unset:
        message += (
            "  Values in this block came from unset environment variables: "
            f"{', '.join(unset)}. Each was read as its own name, which may be what "
            "failed; set them to check the block as it will run.\n"
        )
    return ConfigError(message, locations=locations)


def _yaml_locations(error: yaml.YAMLError) -> tuple[ConfigLocation, ...]:
    """Place a parse error at its mark, counted from 1, when the parser left one."""
    mark = error.problem_mark if isinstance(error, yaml.MarkedYAMLError) else None
    if mark is None:
        return ()
    return (ConfigLocation(line=mark.line + 1, column=mark.column + 1),)


def _check_aliases(root: yaml.Node) -> None:
    """Refuse a document whose YAML aliases repeat more than `_ALIAS_LIMIT` of text.

    An alias shares its anchor's node, so aliases nested inside aliases multiply:
    a file under 1 KB can stand for gigabytes once any code walks or prints it. Each
    node's size is counted once, a scalar as its length and a list or mapping as one
    plus its contents, and each further visit adds that size to what aliases repeat.
    """
    # The loader refuses loops while composing, so the nodes form a graph without
    # cycles and a size cached on the first visit is the node's whole size.
    sizes: dict[yaml.Node, int] = {}
    repeated = 0

    def size(node: yaml.Node) -> int:
        nonlocal repeated
        if node in sizes:
            repeated += sizes[node]
            if repeated > _ALIAS_LIMIT:
                mark = node.start_mark
                raise ConfigError(
                    f"The YAML aliases in this file repeat more than {_ALIAS_LIMIT:,} characters, "
                    f"the most a file may repeat. The last one repeats the value at line "
                    f"{mark.line + 1}, column {mark.column + 1}.",
                    locations=(ConfigLocation(line=mark.line + 1, column=mark.column + 1),),
                )
            return sizes[node]
        if isinstance(node, yaml.MappingNode):
            pairs = cast("list[tuple[yaml.Node, yaml.Node]]", node.value)
            total = 1 + sum(size(key) + size(item) for key, item in pairs)
        elif isinstance(node, yaml.SequenceNode):
            items = cast("list[yaml.Node]", node.value)
            total = 1 + sum(size(item) for item in items)
        else:
            total = max(1, len(cast("str", node.value)))
        sizes[node] = total
        return total

    size(root)


class _AliasCheckingLoader(yaml.SafeLoader):
    """The loader of `yaml.safe_load`, refusing alias loops and aliases that repeat too much."""

    def __init__(self, stream: IO[str]) -> None:
        super().__init__(stream)
        self._open_anchors: set[str] = set()
        """The anchors of the lists and mappings being composed, which enclose the next node."""

    def compose_node(self, parent: yaml.Node | None, index: int) -> yaml.Node:
        """Compose the next node, refusing an alias to a list or mapping that encloses it."""
        # The composer only asks for a node where the next event is one.
        event = cast("yaml.NodeEvent", self.peek_event())  # type: ignore[no-untyped-call]  # The stub leaves the parser untyped.
        anchor = event.anchor
        if isinstance(event, yaml.AliasEvent) and anchor in self._open_anchors:
            # A loop has no finite size, and walking or printing it never ends or
            # multiplies. No setting needs one. The composer refuses a repeated anchor
            # name, so an open anchor always names an enclosing node.
            mark = cast("yaml.Mark", event.start_mark)
            raise ConfigError(
                f"The YAML alias at line {mark.line + 1}, column {mark.column + 1} refers to "
                "a mapping or list that contains it.",
                locations=(ConfigLocation(line=mark.line + 1, column=mark.column + 1),),
            )
        if anchor is None or isinstance(event, yaml.AliasEvent):
            return cast("yaml.Node", super().compose_node(parent, index))
        self._open_anchors.add(anchor)
        try:
            return cast("yaml.Node", super().compose_node(parent, index))
        finally:
            self._open_anchors.discard(anchor)

    def compose_document(self) -> yaml.Node:
        """Compose the next document and refuse it if its aliases repeat too much."""
        # The stub allows None, but a document always composes to a node; an empty file
        # composes no document at all.
        node = cast("yaml.Node", super().compose_document())
        _check_aliases(node)
        return node


def _read_yaml(stream: IO[str]) -> Any:
    """Parse one YAML document as `yaml.safe_load` does, after checking its aliases."""
    loader = _AliasCheckingLoader(stream)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()


def _parse_source_ref(
    raw: Any,
    *,
    label: str,
    unset: list[str] | None = None,
    on_reference: OnReference | None = None,
) -> SourceRef:
    """Validate a YAML source/target block as a discriminated `SourceRef`."""
    if not isinstance(raw, dict):
        raise ConfigError(
            f"The '{label}' block must be a mapping.", locations=(ConfigLocation(path=(label,)),)
        )
    block = cast("dict[object, object]", raw)
    guessed: list[str] | None = None if unset is None else []
    try:
        payload: dict[str, Any] = _expand_env(
            block,
            (label,),
            unset=guessed,
            block_type=block.get("type", "file"),
            on_reference=on_reference,
        )
    finally:
        # A reference the hook refuses still reports the unset names before it.
        if unset is not None and guessed:
            unset.extend(name for name in guessed if name not in unset)
    payload.setdefault("type", "file")
    try:
        return _SOURCE_REF_ADAPTER.validate_python(payload)
    except ValidationError as e:
        raise _validation_failure(e, guessed or (), block=label, tag=payload["type"]) from None


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
        ConfigError: If the file is missing or not valid YAML, holds an alias inside the
            value it names, its aliases repeat more than 1,000,000 characters, it lacks
            a `source` or `target` block, or it references an unset environment
            variable, holds a malformed reference, or fails validation.

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
    return load_config_with_hook(path, unset_env=unset_env)


def load_config_with_hook(
    path: str | Path,
    *,
    unset_env: list[str] | None = None,
    on_reference: OnReference | None = None,
) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Load a configuration file as `load_config` does, calling `on_reference` on each `${NAME}`.

    The hook sees each reference in `source` and `target` after YAML has parsed
    the string and before its value is put in place, and refuses it by raising.
    """
    file_path = Path(path)

    if not file_path.is_file():
        raise ConfigError(f"Configuration file not found or is not a file: {file_path.absolute()}")

    try:
        with file_path.open("r", encoding="utf-8") as f:
            parsed_yaml: Any = _read_yaml(f)
    except yaml.YAMLError as yaml_err:
        raise ConfigError(
            f"Failed to parse YAML file:\n{yaml_err}", locations=_yaml_locations(yaml_err)
        ) from yaml_err

    if not isinstance(parsed_yaml, dict):
        raise ConfigError("Invalid YAML structure: Root element must be a dictionary.")

    raw_config = cast("dict[str, Any]", parsed_yaml)
    if "source" not in raw_config or "target" not in raw_config:
        raise ConfigError("Configuration must contain both 'source' and 'target' blocks.")

    source_cfg = _parse_source_ref(
        raw_config.pop("source"), label="source", unset=unset_env, on_reference=on_reference
    )
    target_cfg = _parse_source_ref(
        raw_config.pop("target"), label="target", unset=unset_env, on_reference=on_reference
    )
    try:
        diff_cfg = DiffConfig.model_validate(raw_config)
    except ValidationError as e:
        raise _validation_failure(e) from None
    return diff_cfg, source_cfg, target_cfg


def files_config(
    source: str | Path, target: str | Path, primary_keys: Sequence[str]
) -> tuple[DiffConfig, SourceConfig, SourceConfig]:
    """Build the configuration that compares two files on their primary keys.

    It holds what a file with only `primary_keys` and a `path` for each side holds,
    and is checked the same way, so each file's format follows its suffix. The
    paths are read as given: a shell has already expanded its own variables, so a
    `${NAME}` in one is left as it is.

    Args:
        source (str | Path): The source file.
        target (str | Path): The target file.
        primary_keys (Sequence[str]): The columns that identify a row on both sides.

    Returns:
        tuple[DiffConfig, SourceConfig, SourceConfig]: The comparison settings, then
            the source and the target, as `load_config` returns them.

    Raises:
        ConfigError: If `primary_keys` is empty.

    Examples:
        >>> diff, source, target = files_config("legacy.csv", "modern.parquet", ["id"])
        >>> diff.primary_keys, source.path, target.format
        (['id'], 'legacy.csv', 'parquet')
    """
    sides = [SourceConfig.model_validate({"path": str(path)}) for path in (source, target)]
    try:
        diff = DiffConfig.model_validate({"primary_keys": list(primary_keys)})
    except ValidationError as e:
        raise _validation_failure(e) from None
    return diff, sides[0], sides[1]
