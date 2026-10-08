# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Call the tools of `veridelta mcp` as an agent's host does, and print each call and answer.

`demo/mcp.tape` records this script. It starts `veridelta mcp` on the files in
`demo/` over stdio, with row values allowed, as someone who may read the data
would start it. It then calls three tools in the order an agent would: check
the configuration, run the comparison, and read the rows that changed. Each
answer prints the fields named in `_SHOWN`, as the server returned them. It
needs the `mcp` extra.

`--brief` is for promotional video, in a large font: each call and answer
prints as YAML, one field to a line, and an answer prints the fields in
`_BRIEF`. `demo/promo/mcp.tape` records it.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import anyio
import yaml
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

_BRIEF = {
    "validate_config": ["valid"],
    "run_comparison": _SHOWN["run_comparison"][:5],
    "read_discrepancies": ["rows"],
}
"""The fields each answer prints with `--brief`: the verdict and counts, and every row whole."""


def _yaml(prefix: str, fields: dict[str, Any]) -> str:
    """Return fields as YAML, one to a line, the first after `prefix` and the rest under it."""
    lines = yaml.safe_dump(fields, sort_keys=False, default_flow_style=False).splitlines()
    return "\n".join([prefix + lines[0], *("   " + line for line in lines[1:])])


async def _session(brief: bool) -> None:
    """Start the server, call each tool, and print the call and its answer."""
    server = StdioServerParameters(
        command="veridelta",
        args=["mcp", "--root", str(_DEMO), "--allow-row-values"],
        env=dict(os.environ),
    )
    with open(os.devnull, "w", encoding="utf-8") as quiet:
        async with Client(stdio_client(server, errlog=quiet)) as client:
            for name, arguments in _CALLS:
                if brief:
                    print(f"-> {name}:")
                    print(_yaml("   ", arguments))
                else:
                    print(f"-> {name} {json.dumps(arguments)}")
                answer = (await client.call_tool(name, arguments)).structured_content or {}
                fields = (_BRIEF if brief else _SHOWN)[name]
                shown = {field: answer[field] for field in fields}
                print(_yaml("<- ", shown) if brief else "<- " + json.dumps(shown, indent=2))


def main() -> int:
    """Run the session.

    Returns:
        int: 0 once every call is answered.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--brief", action="store_true", help="Print YAML, one field to a line.")
    anyio.run(_session, parser.parse_args().brief)
    return 0


if __name__ == "__main__":
    sys.exit(main())
