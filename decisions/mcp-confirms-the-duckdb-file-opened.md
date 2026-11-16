---
type: Decision
title: The MCP server confirms which DuckDB file DuckDB opened
description: Why the server looks a DuckDB file up through no link and then checks the descriptor DuckDB holds, rather than copying the file or handing DuckDB a descriptor's path.
status: stable
decided: 2026-11-16
generated: { by: claude-code, at: 2026-11-16T18:19:08+00:00 }
---

**Claim:** Inside `sandboxed`, `_open_checked` in `src/veridelta/connectors/duckdb.py` looks the database file and each write-ahead log up through no link, lets DuckDB open the file by its path, and then requires a descriptor that was not open before `duckdb.connect` to name the file it looked up, and each log to be as it was, before any statement runs. A lock holds these steps, so no other read's DuckDB open runs alongside them. A query spills into a folder the block creates, never beside the file.

**Evidence:** With DuckDB 1.5.6, a folder swapped for a link between the server's check and `duckdb.connect` made `describe_schema` return the columns of a file outside the roots, and a log linked beside the file after the check replayed another database's rows into it. `strace` shows DuckDB resolving the path with `readlink`, holding the file open with a read lock for the connection's life, and opening and closing the log inside `duckdb.connect`. `test_it_refuses_a_duckdb_folder_swapped_for_a_link_as_duckdb_opens_the_file` and `test_it_refuses_a_duckdb_log_put_beside_the_file_as_duckdb_opens_it` in `tests/unit/test_mcp_server.py` swap the link inside `duckdb.connect` and are refused. A first version accepted any descriptor of the process, and a second tool call that held the same file open through a hard link let a folder swapped in and back pass; `test_it_refuses_a_duckdb_folder_swapped_while_another_call_holds_the_file` holds the file that way and is refused. DuckDB's default spill folder was `duckdb:` and the path with `.tmp`, from the working directory, which `test_it_spills_into_a_folder_of_the_sandboxs_own` in `tests/integration/test_duckdb_sources.py` replaces.

**Alternative considered:** Copy the file and its logs, from descriptors opened through no link, into a private folder, and open the copy, which no link can reach.

**Why rejected:** A copy costs the file's whole size in time and disk on every tool call, and the system's temporary folder is often memory, so a large database could exhaust it. Handing DuckDB `/proc/self/fd/N` fails too: DuckDB resolves it back to the path's text, and names the log from that. What remains is a log, or a folder on its path, swapped in and out during `duckdb.connect`; a file swapped in and back while a query in another tool call opens the checked file, such as through a hard link or its renamed folder; and on Windows, which lists no descriptors, the file swapped in and back during that call. Requiring DuckDB's read lock on the new descriptor, as `/proc/self/fdinfo` shows it, would close the second on Linux, but a system that does not show locks there would refuse every file, and macOS shows none.

**How to reverse:** Open the file with `_connect` alone inside `sandboxed`, in place of `_open_checked`, and remove the sentences on it from `docs/agents.md` and `rules/security.md`.
