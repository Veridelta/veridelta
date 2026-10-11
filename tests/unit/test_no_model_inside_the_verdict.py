# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold the package to the decision that no model decides a verdict.

The record in `decisions/no-model-inside-the-verdict.md` settles it: a model may
drive the command line, explain a run, or propose a rule with its evidence, and
only a rule the user declares changes what matches. These tests keep the choice
from eroding one import at a time. They scan the package's imports and its
packaging against a list of model SDKs, hold every MCP tool to a name and
arguments that propose and never apply, and run every tool and every proposal
on a folder whose bytes they then find unchanged.
"""

import ast
import hashlib
import tomllib
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any, TypeVar

import anyio
import pytest
from mcp import Client
from mcp.types import CallToolResult, Tool
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from veridelta.config import load_config
from veridelta.engine import DiffEngine
from veridelta.mcp_server import Settings, build_server

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_T = TypeVar("_T")

_ROOT = Path(__file__).resolve().parents[2]
_PACKAGE = _ROOT / "src" / "veridelta"

_MODEL_MODULES = (
    "anthropic",
    "claude_agent_sdk",
    "cohere",
    "google.genai",
    "google.generativeai",
    "huggingface_hub",
    "langchain",
    "langgraph",
    "litellm",
    "llama_index",
    "mistralai",
    "ollama",
    "openai",
    "sentence_transformers",
    "tiktoken",
    "torch",
    "transformers",
    "vertexai",
)
"""Import roots of the SDKs that call or run a language model. A name matches as
itself or as a parent package, and a root that starts with `langchain` or
`llama_index` matches too, since both ship as families of packages."""

_MODEL_DISTRIBUTIONS = frozenset(
    {
        "anthropic",
        "claude-agent-sdk",
        "cohere",
        "google-cloud-aiplatform",
        "google-genai",
        "google-generativeai",
        "huggingface-hub",
        "langchain",
        "langgraph",
        "litellm",
        "llama-index",
        "mistralai",
        "ollama",
        "openai",
        "sentence-transformers",
        "tiktoken",
        "torch",
        "transformers",
    }
)
"""The same SDKs as PyPI names them, after normalization."""

_ALLOWED: dict[str, frozenset[str]] = {}
"""Modules that may import a model SDK, by path under the repository, each with
the import roots it may use. Empty: a model drives Veridelta from outside, and
the decision record says what has to change before a path joins this map."""

_GROUPS_THAT_MAY_NAME_A_MODEL = frozenset({"evals"})
"""Dependency groups that never ship with the package and may hold an SDK: a
model that plays the user of Veridelta in an evaluation sits in one."""

_WRITING_WORDS = frozenset(
    {"accept", "add", "apply", "create", "delete", "edit", "fix", "save", "set", "update", "write"}
)
"""Verbs a tool's name would carry if it changed a file or applied a rule."""

_DECIDING_ARGUMENTS = frozenset(
    {"baseline", "rule", "rules", "threshold", "tolerance", "value_map", "value_maps"}
)
"""Arguments a tool would take if a model could hand it something to forgive."""


def _is_model_module(name: str) -> bool:
    """Say whether an imported name belongs to a model SDK."""
    root = name.split(".")[0]
    if root.startswith(("langchain", "llama_index")):
        return True
    return any(name == module or name.startswith(module + ".") for module in _MODEL_MODULES)


def _is_model_distribution(name: str) -> bool:
    """Say whether a requirement's distribution is a model SDK."""
    canonical = canonicalize_name(name)
    return canonical in _MODEL_DISTRIBUTIONS or canonical.startswith(("langchain-", "llama-index-"))


def _names_import_module(func: ast.expr) -> bool:
    """Say whether a call imports a module by name at run time."""
    if isinstance(func, ast.Name):
        return func.id in {"import_module", "__import__"}
    return isinstance(func, ast.Attribute) and func.attr == "import_module"


def _imports(tree: ast.AST) -> Iterator[str]:
    """Yield every name a syntax tree imports.

    A `from` import yields the module and each name under it, so
    `from google import genai` reads as `google.genai`. A call to
    `importlib.import_module` or `__import__` with a literal name yields that
    name; one with a variable cannot be read here and is not.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.level == 0:
                yield node.module
                yield from (f"{node.module}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Call) and _names_import_module(node.func) and node.args:
            first = node.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                yield first.value


def _model_imports(path: Path) -> set[str]:
    """Return the names a module imports that belong to a model SDK."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {name for name in _imports(tree) if _is_model_module(name)}


def _digests(folder: Path) -> dict[str, str]:
    """Return every file under a folder with the SHA-256 of its bytes."""
    return {
        path.relative_to(folder).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(folder.rglob("*"))
        if path.is_file()
    }


def _pair(tmp_path: Path) -> Path:
    """Write a pair whose differences call for one tolerance and one value map.

    Two amounts move by a rounding gap, and every status is written as a code,
    as the end-to-end test of the server has it. The configuration names its
    files by absolute path, so the test does not depend on the working folder.
    """
    root = tmp_path / "project"
    root.mkdir()
    ids = range(1, 13)
    statuses = ["active" if i % 2 else "closed" for i in ids]
    (root / "source.csv").write_text(
        "id,amount,status\n"
        + "".join(f"{i},{10 + i:.3f},{s}\n" for i, s in zip(ids, statuses, strict=True))
    )
    (root / "target.csv").write_text(
        "id,amount,status\n"
        + "".join(
            f"{i},{10 + i + (0.004 if i <= 2 else 0):.3f},{s[0].upper()}\n"
            for i, s in zip(ids, statuses, strict=True)
        )
    )
    (root / "rules.yaml").write_text(
        f"source:\n  path: {root / 'source.csv'}\ntarget:\n  path: {root / 'target.csv'}\n"
        "primary_keys: [id]\n"
    )
    return root


def _with_client(settings: Settings, work: Callable[[Client], Awaitable[_T]]) -> _T:
    """Connect the SDK's client to a server built from `settings`, in memory, and run `work`."""

    async def session() -> _T:
        async with Client(build_server(settings)) as client:
            return await work(client)

    return anyio.run(session)


def _tools(settings: Settings) -> list[Tool]:
    """List the server's tools, as a host does when it connects."""

    async def work(client: Client) -> list[Tool]:
        return (await client.list_tools()).tools

    return _with_client(settings, work)


def _call_every_tool(settings: Settings, path: str) -> dict[str, CallToolResult]:
    """Call each of the server's tools once on one configuration file."""
    arguments: dict[str, dict[str, Any]] = {
        "validate_config": {"path": path},
        "run_comparison": {"path": path},
        "describe_schema": {"path": path, "side": "target"},
        "read_discrepancies": {"path": path, "kind": "changed"},
        "propose_value_maps": {"path": path},
        "suggest_rules": {"path": path},
    }

    async def work(client: Client) -> dict[str, CallToolResult]:
        names = [tool.name for tool in (await client.list_tools()).tools]
        assert sorted(names) == sorted(arguments), "A tool joined or left the server."
        return {name: await client.call_tool(name, arguments[name]) for name in names}

    return _with_client(settings, work)


class TestNoModelInsideTheVerdict:
    """Keep the package, its packaging, and its tools to the decision record."""

    def test_the_scan_reads_an_import_however_it_is_written(self, tmp_path: Path) -> None:
        """Ensure the guard can go red: each way of importing a model SDK is seen, and nothing else is."""
        planted = tmp_path / "planted.py"
        planted.write_text(
            "import anthropic\n"
            "from google import genai\n"
            "from openai import OpenAI\n"
            "import importlib\n"
            "importlib.import_module('langchain_core.runnables')\n"
            "from google.cloud import bigquery\n"
            "import polars as pl\n"
            "from .sibling import helper\n",
            encoding="utf-8",
        )

        assert _model_imports(planted) == {
            "anthropic",
            "google.genai",
            "openai",
            "openai.OpenAI",
            "langchain_core.runnables",
        }
        assert _is_model_distribution("Claude_Agent_SDK")
        assert _is_model_distribution("langchain-openai")
        assert not _is_model_distribution("polars")

    def test_the_package_imports_no_model_sdk(self) -> None:
        """Ensure no module under the package reaches a model, by statement or by name."""
        modules = sorted(_PACKAGE.rglob("*.py"))
        found = {}
        for path in modules:
            label = path.relative_to(_ROOT).as_posix()
            names = _model_imports(path) - _ALLOWED.get(label, frozenset())
            if names:
                found[label] = sorted(names)

        assert len(modules) > 20, "The package folder moved."
        assert all((_ROOT / label).is_file() for label in _ALLOWED), "An allowed path is gone."
        assert found == {}, "A module calls a model. See decisions/no-model-inside-the-verdict.md."

    def test_the_packaging_names_no_model_sdk(self) -> None:
        """Ensure no dependency, extra, or shipped group installs a model SDK."""
        with (_ROOT / "pyproject.toml").open("rb") as file:
            pyproject = tomllib.load(file)
        project = pyproject["project"]
        shipped: dict[str, list[str]] = {"dependencies": project["dependencies"]}
        for extra, lines in project["optional-dependencies"].items():
            shipped[f"extra {extra}"] = lines
        for group, lines in pyproject.get("dependency-groups", {}).items():
            if group not in _GROUPS_THAT_MAY_NAME_A_MODEL:
                shipped[f"group {group}"] = [line for line in lines if isinstance(line, str)]

        found = {
            where: [line for line in lines if _is_model_distribution(Requirement(line).name)]
            for where, lines in shipped.items()
        }

        assert {where: lines for where, lines in found.items() if lines} == {}

    def test_every_tool_proposes_and_none_applies(self, tmp_path: Path) -> None:
        """Ensure no tool is named for writing, and none takes a rule, a tolerance, or a baseline."""
        tools = _tools(Settings((tmp_path,)))

        assert len(tools) == 6
        for tool in tools:
            assert not set(tool.name.split("_")) & _WRITING_WORDS, tool.name
            assert not set(tool.input_schema["properties"]) & _DECIDING_ARGUMENTS, tool.name

    def test_no_tool_changes_a_byte_under_its_folder(self, tmp_path: Path) -> None:
        """Ensure a run of every tool, proposals included, leaves the folder as it was."""
        root = _pair(tmp_path)
        before = _digests(root)

        results = _call_every_tool(Settings((root,), allow_row_values=True), "rules.yaml")

        assert {name for name, result in results.items() if result.is_error} == set()
        suggested = results["suggest_rules"].structured_content
        proposed = results["propose_value_maps"].structured_content
        assert suggested is not None
        assert proposed is not None
        assert [each["column"] for each in suggested["suggestions"]] == ["amount"]
        assert [each["column"] for each in proposed["proposals"]] == ["status"]
        assert _digests(root) == before

    def test_a_proposal_leaves_the_configuration_as_written(self, tmp_path: Path) -> None:
        """Ensure the engine's proposals neither write the file nor change the loaded rules."""
        root = _pair(tmp_path)
        before = _digests(root)
        diff, source, target = load_config(root / "rules.yaml")

        suggestions = DiffEngine.suggest_rules_from_configs(diff, source, target)
        proposals = DiffEngine.propose_value_maps_from_configs(diff, source, target)

        assert [suggestion.column for suggestion in suggestions] == ["amount"]
        assert [proposal.column for proposal in proposals] == ["status"]
        assert diff.rules == []
        assert _digests(root) == before
