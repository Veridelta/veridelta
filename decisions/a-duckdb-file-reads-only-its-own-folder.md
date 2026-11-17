---
type: Decision
title: A DuckDB file reads only its own folder
description: Why every DuckDB file connection is held to the database file's own folder, with extensions and settings locked, rather than to no folder or behind a trusted switch.
status: stable
decided: 2026-11-14
generated: { by: claude-code, at: 2026-11-14T23:14:15+00:00 }
---

**Claim:** `_open` in `src/veridelta/connectors/duckdb.py` runs `compile_duckdb_sandbox` on every DuckDB file connection, not only inside `sandboxed`. Outside the MCP server the one allowed folder is the database file's own, after `~` expansion and resolution, so a view over a file beside it or under it still reads. On Windows, DuckDB's own check of the folder does not hold, as [On Windows, the MCP server's DuckDB connection opens no other file](mcp-duckdb-opens-no-other-file-on-windows.md) shows, so there `docs/sources.md` tells a user to treat a file from someone else as code.

**Evidence:** A view stored in a file runs whatever its author wrote, and read-only stops writes but not reads. Before this change, a view over `read_text` returned another folder's file as rows, and a view over a URL loaded `httpfs` by itself, through `veridelta run`, the GitHub Action, and the Python API. `test_it_runs_a_files_views_only_over_files_beside_it` in `tests/integration/test_duckdb_sources.py` now reads a view over a CSV under the file's folder, refuses one over a file beside that folder, and checks that external access, extension autoinstall and autoload are off and the settings are locked. `test_it_holds_a_file_to_its_own_folder_outside_the_sandbox` in `tests/unit/test_duckdb.py` checks the folder for a relative, an absolute, and a `~` path.

**Alternative considered:** Allow no other file at all, and add a `trusted: true` field to the DuckDB source that opens the connection with DuckDB's defaults.

**Why rejected:** Allowing no file breaks the common case, a view or a `query` over Parquet or CSV files kept beside the database. A `trusted` switch is a new configuration field whose one setting reopens every local file and the network, where moving the files beside the database already works.

**How to reverse:** Change the default `folders` in `_open`, and the sentences that describe the folder in `docs/sources.md`, `docs/upgrading.md`, and `rules/security.md`.
