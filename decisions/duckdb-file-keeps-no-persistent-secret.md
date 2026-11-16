---
type: Decision
title: A DuckDB file's connection keeps no persistent secret
description: Why every DuckDB file connection turns off persistent secrets in its sandbox statements, rather than raising the DuckDB floor to 1.5.3.
status: stable
decided: 2026-11-16
generated: { by: claude-code, at: 2026-11-16T22:20:31+00:00 }
---

**Claim:** `compile_duckdb_sandbox` in `src/veridelta/connectors/sql.py` runs `SET allow_persistent_secrets = false` before the lock, so no file connection, on the command line or on the MCP server, reads, stores, or drops a secret in the home folder.

**Evidence:** DuckDB 1.5.0 to 1.5.2 read and write `~/.duckdb/stored_secrets` without checking `allowed_directories` or `disabled_filesystems`. At 1.5.0, a sandboxed connection listed the user's stored bearer token through `duckdb_secrets()`, dropped it with `DROP PERSISTENT SECRET`, and stored a planted proxy with `CREATE PERSISTENT SECRET`, through a `query` that a `SELECT` followed. With the setting, under strace on Linux at 1.5.0 and 1.5.6, those statements made no file call in the home folder. `test_it_reads_and_writes_no_persistent_secret` in `tests/integration/test_duckdb_sources.py` checks the listing, the refusals, an in-memory `CREATE SECRET`, and an unchanged home folder, with the file's own folder, the roots, and the roots with the file system disabled. It fails without the setting at 1.5.0 and at 1.5.6.

**Alternative considered:** Raise the `duckdb` floor in `pyproject.toml` to 1.5.3, which checks the secret folder against the sandbox.

**Why rejected:** A newer floor moves every user's DuckDB to fix a setting that one statement turns off at every version. At 1.5.6 without the setting, the sandbox makes `duckdb_secrets()` fail outright and `CREATE PERSISTENT SECRET` fail with an unrelated message, while with it an in-memory secret still works.

**How to reverse:** Remove the statement from `compile_duckdb_sandbox`, with its line in `tests/unit/test_sql_compiler.py`, the test above, and the persistent secret sentences in `docs/sources.md`, `docs/agents.md`, `docs/upgrading.md`, and `rules/security.md`.
