---
type: Decision
title: The MySQL and SQL Server test drivers sit in their own dependency group
description: Why pymysql and pymssql install only for make databases, make sqlserver, and the Database Servers CI job, while psycopg is in the dev group.
status: stable
decided: 2026-10-06
generated: { by: claude-code, at: 2026-10-11T11:00:13+00:00 }
---

**Claim:** `pymysql` and `pymssql` sit in the `databases` dependency group. Only `make databases`, `make sqlserver`, and the Database Servers job install it.

**Evidence:** Only `tests/integration/test_database_servers.py` and `tests/integration/sqlserver_harness.py` import them, each inside the function that loads rows, so neither import runs unless a server is set. Every other job runs `uv sync --all-extras`, which leaves the group out. `psycopg` stays in the dev group because `tests/integration/duckdb_harness.py` imports it in every run.

**Alternative considered:** Both drivers in the dev group, beside `psycopg`.

**Why rejected:** Every contributor and every CI job would install two drivers for tests that skip without a server.

**How to reverse:** Move both lines into the `dev` group, drop `--group databases` from the Makefile and the CI job, and run `uv lock`.
