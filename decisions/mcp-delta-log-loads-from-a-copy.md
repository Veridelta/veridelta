---
type: Decision
title: The MCP server loads a Delta Lake table on this machine from a copy of its log
description: Why the server copies each checked file of a local Delta Lake log into a private folder and loads the table there, chosen over checking deltalake's own reads afterwards.
status: stable
decided: 2026-11-16
generated: { by: claude-code, at: 2026-11-16T16:23:07+00:00 }
---

**Claim:** For a Delta Lake table that deltalake reads from this machine, `_delta_files` in `src/veridelta/mcp_server.py` copies each file of the log, and each deletion vector at a relative path, just after its check, into a `DeltaLogCopy` in the system's temporary folder, and loads the `DeltaTable` from that copy. It refuses the table when that folder lies under a root.

**Evidence:** deltalake 1.6 reads the log again in Rust as it builds a `DeltaTable`, and each vector's file in `deletion_vectors`. On origin/main, a commit written after the server read the log, naming a vector outside the roots, chose which rows a scan returned. `test_it_loads_a_delta_table_from_the_log_it_checked` and `test_it_refuses_a_file_replaced_as_it_is_checked` in `tests/unit/test_mcp_server.py` hold the copy to that. `test_the_server_reads_the_rows_polars_reads` in `tests/integration/test_lakehouse_sources.py` holds its rows to `pl.scan_delta`'s, with checkpoints, deletion vectors, partitions, and a new column.

**Alternative considered:** Load the table from its own folder, then compare the version, the actions, and the device and number of each log and vector file deltalake read with those the server checked, and refuse a scan whose files differ.

**Why rejected:** deltalake reports neither the files it read nor a vector's descriptor, so the comparison would rest on stats taken around its read. A file swapped in for that read and swapped back would pass. The copy leaves deltalake no file in the table's folder to read, and costs one copy of the log for each listing. The copy itself compares two stats of each file, around its check and its open, so two swaps that bracket the check or the open still get through. Checking the path of the open file would close that, but Linux, macOS, and Windows each give it through a call of their own, so the stats stay.

**How to reverse:** In `_delta_files`, set `copy` to None for every table, which loads the table from its own folder as before, and delete `DeltaLogCopy`, `_copy_checked`, and the copy's paragraph in `rules/security.md` and `docs/agents.md`.
