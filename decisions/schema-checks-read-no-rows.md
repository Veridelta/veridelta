---
type: Decision
title: Schema checks read no rows
description: Why validate --schemas and the MCP describe_schema tool read each database side with a probe that returns no rows, even though SQLite then reports a NUMERIC column as text.
status: stable
decided: 2026-10-11
generated: { by: claude-code, at: 2026-10-11T18:08:40+00:00 }
---

**Claim:** `veridelta validate --schemas` and the MCP `describe_schema` tool read a database table with `WHERE 1 = 0` added, so no row is fetched. A SQLite `NUMERIC` column then shows as `String` there, where a run reads it as `Float64`.

**Evidence:** `compile_database_probe` in `src/veridelta/connectors/sql.py` adds the condition, and `schema_frame` in `src/veridelta/_reading.py` opens each database side with `probe=True`. `docs/cli.md` promises that no row is fetched, and `docs/how-to/troubleshooting.md` describes the SQLite `NUMERIC` gap. The maintainer chose to keep the gap on 2026-10-11, in the checklist on issue 290.

**Alternative considered:** Read one row with `LIMIT 1`, so SQLite reports the type a run reads.

**Why rejected:** Every database source shares one probe, so the check would no longer read zero rows on any of them, which the docs promise and a warehouse bills for. The gap touches one type in one database, and the troubleshooting page names it.

**How to reverse:** Add `LIMIT 1` to the SQLite probe in `compile_database_probe`, and change the sentences in `docs/cli.md` and `docs/how-to/troubleshooting.md` that describe the probe.
