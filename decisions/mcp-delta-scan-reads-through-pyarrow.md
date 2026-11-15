---
type: Decision
title: The MCP server reads a Delta Lake table's data files through PyArrow
description: Why a Delta Lake scan on the MCP server reads its data files through a checking PyArrow file system, chosen over Polars' own scan of the listed version.
status: stable
decided: 2026-11-15
generated: { by: claude-code, at: 2026-11-15T20:30:00+00:00 }
---

**Claim:** Inside `checking_table_files`, `DeltaLakeConnector` reads the `DeltaTable` that the server's listing checked, and `_checked_delta_scan` in `src/veridelta/connectors/lakehouse.py` reads its data files through deltalake's own fragments in a PyArrow `PyFileSystem` that checks each file just before it opens it, and drops the rows each deletion vector deletes itself. `veridelta run` keeps `pl.scan_delta`.

**Evidence:** Polars' `scan_delta` hands the data file paths to its Rust Parquet reader, and deltalake reads the log and the deletion vectors in Rust, so no Python check can run when either opens a file. On origin/main, a data file replaced by a link out of the roots after the listing was read by the MCP server. `TestCheckedDeltaScan` in `tests/integration/test_lakehouse_sources.py` holds the PyArrow read to the frames `pl.scan_delta` returns, for a time-travelled version, a partitioned table with a new column and a delete, a table with a deletion vector, a table partitioned by a text column with an empty value, and an empty table. `test_it_stops_a_delta_scan_at_a_file_that_changed_after_it_started` in `tests/unit/test_mcp_server.py` stops the swapped file.

**Alternative considered:** Hand the checked `DeltaTable` to `pl.scan_delta`, so the scan reads only the files the listing saw, and leave the window between the listing and each open.

**Why rejected:** The window would span the whole comparison, since Polars opens the data files when the rows are read, long after the check. deltalake's own `to_pyarrow_dataset` refuses a table with deletion vectors, so the fragments come from its private `dataset_partitions`, which a deltalake upgrade can change: the integration tests catch that.

**How to reverse:** In `DeltaLakeConnector.connect`, call `pl.scan_delta(listing(self._config))` in place of `_checked_delta_scan`, delete `_checked_delta_scan` and `_deleted_rows`, and state the window in `docs/agents.md` and `rules/security.md`.
