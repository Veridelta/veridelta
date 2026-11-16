---
type: Decision
title: On Windows, the MCP server's DuckDB connection opens no other file
description: Why the server disables DuckDB's file system on Windows, where DuckDB's own check of the roots does not hold, rather than refusing --allow-queries there or trusting the check.
status: stable
decided: 2026-11-16
generated: { by: claude-code, at: 2026-11-16T21:34:36+00:00 }
---

**Claim:** Inside `sandboxed`, wherever `_FOLDERS_HOLD` in `src/veridelta/connectors/duckdb.py` is false, as on Windows, `_open` passes `other_files=False` to `compile_duckdb_sandbox`, which runs `SET disabled_filesystems = 'LocalFileSystem'` after `SET enable_external_access = false` and before the lock. A file's connection on the MCP server then reads its own tables and views, and opens no other file, for a `query` and for a view stored in the file alike.

**Evidence:** In DuckDB 1.5.0 and 1.5.6, `DBConfig::CanAccessFile` in `src/main/config.cpp` resolves a path with `LocalFileSystem::CanonicalizePath` and matches the result against `allowed_directories` as text. On Windows, `LocalFileSystem::PathStartsWithDrive` in `src/common/local_file_system.cpp` accepts only `A` to `Z`, so `MakePathAbsolute` joins `c:..\x.csv` to the working directory, the check passes, and `CreateFileW` then opens the path from the folder above. `TryCanonicalizeExistingPath` calls `CreateFileW` on a share path before the check refuses it, which reaches the machine it names. `VirtualFileSystem::FindFileSystem` in `src/common/virtual_file_system.cpp` refuses a disabled file system by its name, with no `#ifdef`, and `VirtualFileSystem::CanonicalizePath` calls it first, so the check throws before `LocalFileSystem` sees the path. Under strace on Linux, a refused query and an `ATTACH` outside the roots made 15 `readlink` calls on the path in both versions, and none with the file system disabled. `test_it_opens_no_other_file_when_sandboxed_where_duckdbs_folders_do_not_hold` in `tests/integration/test_duckdb_sources.py` reads a table, a view over it, and a query, and refuses `read_csv`, `glob`, `ATTACH`, a view over a CSV file, and a `SET` that would undo it. `TestDuckDBSides` in `tests/integration/test_mcp_windows_paths.py` runs each Windows form on Windows, `c:..` included, and expects DuckDB's message that the file system is disabled. DuckDB 1.5.0 to 1.5.2 read and write persistent secrets in the home folder whatever the file system setting says, so the claim holds only with persistent secrets off, as [A DuckDB file's connection keeps no persistent secret](duckdb-file-keeps-no-persistent-secret.md) records.

**Alternative considered:** Refuse `--allow-queries` on Windows, so no `query` reaches DuckDB, and keep DuckDB's check of the roots for everything else.

**Why rejected:** A view stored in a DuckDB file under a root runs as its author wrote it, without `--allow-queries`, and an agent can write files there. So refusing queries leaves the same escape open, and closing it would mean refusing every DuckDB file on Windows. Disabling the file system closes both, keeps a query over the database's own tables, and costs only reads of other files and a statement that would spill to disk.

**How to reverse:** Once a DuckDB release fixes `PathStartsWithDrive` and checks a share path before it resolves it, and `TestDuckDBSides` shows the check holding on Windows, set `_FOLDERS_HOLD` to true on every system and change the Windows sentences in `docs/agents.md`, `docs/cli.md`, `docs/sources.md`, `docs/upgrading.md`, `docs/pushdown.md`, and `rules/security.md`.
