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
        "suggest_rules",
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


def test_the_console_script_proposes_rules_and_value_maps(tmp_path: Path) -> None:
    """Ensure a host gets the exact rule and value map a known pair calls for, within the cap."""
    root = tmp_path / "project"
    root.mkdir()
    ids = range(1, 13)
    statuses = ["active" if i % 2 else "closed" for i in ids]
    (root / "source.csv").write_text(
        "id,amount,status\n"
        + "".join(f"{i},{10 + i:.3f},{s}\n" for i, s in zip(ids, statuses, strict=True))
    )
    # Two amounts move by a rounding gap, and every status is written as a code.
    (root / "target.csv").write_text(
        "id,amount,status\n"
        + "".join(
            f"{i},{10 + i + (0.004 if i <= 2 else 0):.3f},{s[0].upper()}\n"
            for i, s in zip(ids, statuses, strict=True)
        )
    )
    (root / "rules.yaml").write_text(
        "source:\n  path: source.csv\ntarget:\n  path: target.csv\nprimary_keys: [id]\n"
    )
    server = StdioServerParameters(
        command="veridelta",
        args=["mcp", "--root", str(root), "--allow-row-values", "--max-rows", "2"],
        env=dict(os.environ),
    )
    log = tmp_path / "server-stderr.log"

    async def session() -> tuple[CallToolResult, CallToolResult]:
        with log.open("w", encoding="utf-8") as errlog:
            async with Client(stdio_client(server, errlog=errlog)) as client:
                suggested = await client.call_tool("suggest_rules", {"path": "rules.yaml"})
                proposed = await client.call_tool("propose_value_maps", {"path": "rules.yaml"})
        return suggested, proposed

    suggested, proposed = anyio.run(session)

    assert suggested.structured_content is not None
    assert suggested.structured_content["total"] == 1
    assert suggested.structured_content["truncated"] is False
    (suggestion,) = suggested.structured_content["suggestions"]
    assert suggestion["column"] == "amount"
    assert suggestion["settings"] == {"absolute_tolerance": 0.005}
    assert (suggestion["explained"], suggestion["examples"]) == (2, [{"id": 1}, {"id": 2}])
    assert proposed.structured_content is not None
    assert proposed.structured_content["total"] == 1
    (proposal,) = proposed.structured_content["proposals"]
    assert proposal["column"] == "status"
    assert proposal["value_map"] == {"active": "A", "closed": "C"}
    assert "Traceback" not in log.read_text(encoding="utf-8")
