# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Serve Veridelta to an AI agent as Model Context Protocol tools.

`veridelta mcp` calls `serve`, which answers an agent's host over stdio through
the official MCP SDK, from the `mcp` extra. Each tool calls a function here that
returns the object its command prints with `--json`, so an agent that knows the
command line knows the tools.

The person who starts the server names the folders it may read configuration
files from, and a tool refuses a path outside them. A tool returns findings,
never a value from the file. The SDK is imported by the first `build_server()`,
not with this module, so `veridelta` imports without the extra.
"""

import importlib
import inspect
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Final, NamedTuple, TypedDict

from pydantic import Field

from veridelta import __version__
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError, VerideltaError

if TYPE_CHECKING:
    from mcp.server import MCPServer

INSTRUCTIONS: Final = (
    "Veridelta compares two datasets under the rules in a YAML configuration file. "
    "Check a file with validate_config, and fix each error it reports, before anything "
    "else. This server reads only files under the folders it was started with."
)
"""What the server tells an agent's host about itself when the host connects."""

_MCP_EXTRA = "MCP extra is not installed. Install it with: uv add 'veridelta[mcp]'"


class _Sdk(NamedTuple):
    """The two names of the MCP SDK the server uses."""

    server: Any
    """`mcp.server.MCPServer`."""

    tool_error: Any
    """`mcp.server.mcpserver.exceptions.ToolError`, whose message reaches the agent."""


sdk: _Sdk | None = None
"""The SDK, imported by the first `build_server()` rather than here, since it
brings a web stack that the rest of Veridelta never loads. Tests set this
attribute."""


def _load_sdk() -> _Sdk:
    """Return the SDK, importing it on first use.

    Raises:
        VerideltaError: If the `mcp` extra is not installed.
    """
    if sdk is not None:
        return sdk
    try:
        server = importlib.import_module("mcp.server")
        errors = importlib.import_module("mcp.server.mcpserver.exceptions")
    except ImportError:
        raise VerideltaError(_MCP_EXTRA) from None
    return _Sdk(server=server.MCPServer, tool_error=errors.ToolError)


@dataclass(frozen=True)
class Settings:
    """What the person who starts the server allows, which no tool call can change.

    Attributes:
        roots (tuple[Path, ...]): The folders a tool may read a configuration
            file from, resolved on creation. A relative path in a tool call is
            read against the first.
    """

    roots: tuple[Path, ...]

    def __post_init__(self) -> None:
        """Resolve every root, so a path compares with them as the filesystem does.

        Raises:
            ConfigError: If no root is given.
        """
        if not self.roots:
            raise ConfigError("The MCP server needs at least one folder to read from.")
        object.__setattr__(self, "roots", tuple(Path(root).resolve() for root in self.roots))


class ValidationReport(TypedDict):
    """What `validate_config` returns, the object `veridelta validate --json` prints.

    Attributes:
        config: The configuration file, resolved.
        valid: Whether no finding is an error.
        errors: What would stop a run.
        warnings: What a run may still trip on, such as an unset variable.
    """

    config: str
    valid: bool
    errors: list[str]
    warnings: list[str]


def resolve_path(settings: Settings, path: str) -> Path:
    """Resolve a path from a tool call, and refuse one outside the roots.

    A relative path is read against the first root. Resolving follows symbolic
    links and folds `..`, so neither leads out of a root.

    Args:
        settings (Settings): The roots the server was started with.
        path (str): The path from the tool call.

    Returns:
        Path: The resolved path, under one of the roots.

    Raises:
        ConfigError: If the path resolves outside every root.
    """
    resolved = (settings.roots[0] / path).resolve()
    if any(resolved.is_relative_to(root) for root in settings.roots):
        return resolved
    roots = ", ".join(str(root) for root in settings.roots)
    raise ConfigError(
        f"'{path}' is outside the folders this server reads from: {roots}. Ask the "
        "person who started it to add the folder with --root."
    )


def check_configuration(
    settings: Settings, path: str, *, schemas: bool = False, allow_missing_env: bool = False
) -> ValidationReport:
    """Check a configuration file for what would stop a run, as `validate --json` does.

    Args:
        settings (Settings): The roots the server was started with.
        path (str): The configuration file, under a root.
        schemas (bool): Whether to also connect and check the rules against each
            side's columns, which reads no rows.
        allow_missing_env (bool): Whether an unset `${NAME}` is a warning rather
            than an error, so a file can be checked without its secrets.

    Returns:
        ValidationReport: The errors and warnings, with the resolved path.

    Raises:
        ConfigError: If the path is outside the roots.
    """
    resolved = resolve_path(settings, path)
    findings = DiffEngine.check_config_file(
        resolved, schemas=schemas, allow_missing_env=allow_missing_env
    )
    errors = [finding.message for finding in findings if finding.severity == "error"]
    warnings = [finding.message for finding in findings if finding.severity == "warning"]
    return ValidationReport(
        config=str(resolved), valid=not errors, errors=errors, warnings=warnings
    )


def _failure(exc: Exception) -> str:
    """Name a failure as `run --json` does: its type, then its message."""
    return f"{type(exc).__name__}: {str(exc).strip()}"


def build_server(settings: Settings) -> "MCPServer":
    """Build the server and register its tools.

    Args:
        settings (Settings): The roots every tool is held to.

    Returns:
        MCPServer: The SDK's server, ready for `run()`.

    Raises:
        VerideltaError: If the `mcp` extra is not installed.
    """
    loaded = _load_sdk()
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    try:
        server: MCPServer = loaded.server(
            "veridelta", instructions=INSTRUCTIONS, version=__version__
        )
    finally:
        # The SDK's constructor configures the root logger, which belongs to the
        # program. The command line sets Veridelta's own logging with --verbose.
        root.handlers[:] = handlers
        root.setLevel(level)

    def validate_config(
        path: Annotated[str, Field(description="The configuration file.")],
        schemas: Annotated[
            bool, Field(description="Also read each side's columns, but no rows.")
        ] = False,
        allow_missing_env: Annotated[
            bool, Field(description="Report an unset ${NAME} as a warning, not an error.")
        ] = False,
    ) -> ValidationReport:
        """Check a Veridelta configuration file for what would stop a run.

        Reads no rows. Returns what `veridelta validate --json` prints: its errors and warnings.
        """
        try:
            return check_configuration(
                settings, path, schemas=schemas, allow_missing_env=allow_missing_env
            )
        except Exception as exc:
            raise loaded.tool_error(_failure(exc)) from exc

    for tool in (validate_config,):
        server.tool(description=inspect.getdoc(tool))(tool)
    return server


def serve(settings: Settings) -> None:
    """Answer an agent's host over stdio until it disconnects.

    Args:
        settings (Settings): The roots every tool is held to.

    Raises:
        VerideltaError: If the `mcp` extra is not installed.
    """
    build_server(settings).run(transport="stdio")
