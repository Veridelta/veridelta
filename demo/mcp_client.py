# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Call the tools of `veridelta mcp` as an agent's host does, and print each call and answer.

`demo/mcp.tape` records this script. It starts `veridelta mcp` on the files in
`demo/` over stdio, with row values allowed, as someone who may read the data
would start it. It then calls three tools in the order an agent would: check
the configuration, run the comparison, and read the rows that changed. Each
answer prints the fields named in `_SHOWN`, as the server returned them. It
needs the `mcp` extra.
"""

import json
import os
import sys
from pathlib import Path
from typing import Any

import anyio
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client

_DEMO = Path(__file__).resolve().parent

_CALLS: list[tuple[str, dict[str, Any]]] = [
    ("validate_config", {"path": "veridelta.yaml"}),
    ("run_comparison", {"path": "veridelta.yaml"}),
    ("read_discrepancies", {"path": "veridelta.yaml", "kind": "changed"}),
]

_SHOWN = {
    "validate_config": ["valid", "errors", "warnings"],
    "run_comparison": [
        "verdict",
        "exit_code",
        "added_count",
        "removed_count",
        "changed_count",
        "column_mismatches",
    ],
    "read_discrepancies": ["total", "truncated", "rows"],
}
"""The fields each answer prints. The rest, such as the plain-text report, repeat these."""


async def _session() -> None:
    """Start the server, call each tool, and print the call and its answer."""
    server = StdioServerParameters(
        command="veridelta",
        args=["mcp", "--root", str(_DEMO), "--allow-row-values"],
        env=dict(os.environ),
    )
    with open(os.devnull, "w", encoding="utf-8") as quiet:
        async with Client(stdio_client(server, errlog=quiet)) as client:
            for name, arguments in _CALLS:
                print(f"-> {name} {json.dumps(arguments)}")
                answer = (await client.call_tool(name, arguments)).structured_content or {}
                shown = {field: answer[field] for field in _SHOWN[name]}
                print("<- " + json.dumps(shown, indent=2))


def main() -> int:
    """Run the session.

    Returns:
        int: 0 once every call is answered.
    """
    anyio.run(_session)
    return 0


if __name__ == "__main__":
    sys.exit(main())
