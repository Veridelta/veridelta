# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-end test of `veridelta mcp`: the console script answers a real client over stdio.

The SDK's client starts the server as a host does, as a subprocess it speaks to
over stdin and stdout, so this test covers the command line, the protocol, and
the tool together.
"""

import json
import os
import shutil
from pathlib import Path

import anyio
import pytest
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CallToolResult

pytestmark = [pytest.mark.e2e]

_VALID = "source:\n  path: a.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n"

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "ci"


def _text(result: CallToolResult) -> str:
    """Join the text a tool call returned for the model."""
    return "".join(getattr(block, "text", "") for block in result.content)


def test_the_console_script_serves_its_tools(
    tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Ensure a host lists the tools, uses each on files under the root, and is refused elsewhere."""
    root = tmp_path / "project"
    root.mkdir()
    (root / "veridelta.yaml").write_text(_VALID)
    (root / "broken.yaml").write_text("source: {}\n")
    for name in ("legacy.csv", "modern_drift.csv"):
        shutil.copy(_FIXTURES / name, root / name)
    (root / "drift.yaml").write_text(
        "source:\n  path: legacy.csv\ntarget:\n  path: modern_drift.csv\nprimary_keys: [id]\n"
    )
    outside = tmp_path_factory.mktemp("outside") / "veridelta.yaml"
    outside.write_text(_VALID)
    # The SDK gives the child a short allow-list of variables, not this
    # environment, so the one the test runs in is passed on whole.
    server = StdioServerParameters(
        command="veridelta",
        args=["mcp", "--root", str(root), "--allow-row-values", "--max-rows", "2"],
        env=dict(os.environ),
    )
    log = tmp_path / "server-stderr.log"

    async def session() -> tuple[list[str], list[CallToolResult]]:
        with log.open("w", encoding="utf-8") as errlog:
            async with Client(stdio_client(server, errlog=errlog)) as client:
                tools = [tool.name for tool in (await client.list_tools()).tools]
                valid = await client.call_tool("validate_config", {"path": "veridelta.yaml"})
                broken = await client.call_tool("validate_config", {"path": "broken.yaml"})
                refused = await client.call_tool("validate_config", {"path": str(outside)})
                drift = await client.call_tool("run_comparison", {"path": "drift.yaml"})
                described = await client.call_tool(
                    "describe_schema", {"path": "drift.yaml", "side": "target"}
                )
                changed = await client.call_tool(
                    "read_discrepancies", {"path": "drift.yaml", "kind": "changed"}
                )
        return tools, [valid, broken, refused, drift, described, changed]

    tools, (valid, broken, refused, drift, described, changed) = anyio.run(session)

    assert tools == [
        "validate_config",
        "run_comparison",
        "describe_schema",
        "read_discrepancies",
        "propose_value_maps",
    ]
    assert valid.structured_content == {
        "config": str((root / "veridelta.yaml").resolve()),
        "valid": True,
        "errors": [],
        "warnings": [],
    }
    assert broken.structured_content is not None
    assert broken.structured_content["valid"] is False
    assert refused.is_error is True
    assert "outside the folders this server reads from" in _text(refused)
    assert json.loads(_text(valid)) == valid.structured_content
    assert drift.structured_content is not None
    assert drift.structured_content["verdict"] == "drift"
    assert drift.structured_content["exit_code"] == 1
    assert described.structured_content is not None
    assert list(described.structured_content["columns"]) == ["id", "status", "amount"]
    assert changed.structured_content is not None
    assert changed.structured_content["total"] == 1
    assert [row["id"] for row in changed.structured_content["rows"]] == [2]
    assert "Traceback" not in log.read_text(encoding="utf-8")
