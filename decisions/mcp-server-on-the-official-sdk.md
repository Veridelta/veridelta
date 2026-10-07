---
type: Decision
title: The MCP server runs on the official SDK, from an extra, over stdio
description: Why veridelta mcp uses the official Python SDK's 2.x line, imported only when the server starts, rather than a protocol loop written in this repository.
status: stable
decided: 2026-10-07
generated: { by: claude-code, at: 2026-10-07T03:14:00Z }
---

**Claim:** `veridelta mcp` serves its tools through the official MCP Python SDK, pinned to `mcp>=2.3.0,<3` in the `mcp` extra and imported by the first `build_server()` in `src/veridelta/mcp_server.py`, over stdio only, and the folders a tool may read are the `--root` folders the person who starts it names.

**Evidence:** The SDK's 2.3.0 release, read on PyPI and at py.sdk.modelcontextprotocol.io on 2026-10-07, builds a tool's input and output schemas from its type hints, and answers both the 2025 and the 2026-07-28 protocol revisions from one server. It depends on a web stack, so `uv lock` added thirteen packages, starlette, uvicorn, and httpx2 among them; with the import deferred, `import veridelta.cli` loads none of them. The protocol's own roots capability is deprecated on every revision by SEP-2577, so the folders are a server option. `tests/unit/test_mcp_server.py` drives the server through the SDK's in-memory client, and `tests/e2e/test_mcp_server.py` starts the console script as a host does, over stdio.

**Alternative considered:** A JSON-RPC loop over stdin and stdout written in this repository, which adds no dependency.

**Why rejected:** It would reimplement the handshake, both protocol revisions, the tool schemas, and the error shapes, and follow every revision after them, which is protocol work and not Veridelta's. The SDK's cost stays inside an extra that only `veridelta mcp` imports, and its one side effect, a root logger configured when the server is built, is undone in `build_server` and held by a test.

**How to reverse:** The tools call `check_configuration` and `resolve_path`, which take no SDK type, so only `build_server` and `serve` in `src/veridelta/mcp_server.py` change for another server layer. To drop the server, remove the `mcp` extra from `pyproject.toml`, the module, the `mcp` subcommand in `src/veridelta/cli.py`, and their tests.
