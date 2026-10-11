---
type: Decision
title: Pushdown to another database goes through the database source, by URI scheme
description: Why SQL Server and Redshift tables compare in place as a database source that sets pushdown, run through ConnectorX with one dialect per URI scheme, rather than as new warehouse types with their own drivers.
status: stable
decided: 2026-10-11
generated: { by: claude-code, at: 2026-10-11T10:09:01+00:00 }
---

**Claim:** A database other than Postgres compares in place as a `type: database` source that sets `pushdown`. `_DATABASE_WAREHOUSES` in `src/veridelta/_warehouses.py` picks its backend by URI scheme, its session runs every statement through ConnectorX, and each scheme has one SQL dialect.

**Evidence:** ConnectorX already reads `mssql://` and `redshift://` tables for a local run (`docs/sources.md`), so a pair that sets `pushdown: false` on the same configuration gives the local verdict through the same connection, and no runtime dependency is added. `veridelta validate` picks the dialect from the scheme without connecting (`_checks.pushdown_findings`). `tests/integration/test_engine_routing.py` holds the scheme table equal to `DATABASE_PUSHDOWN_SCHEMES`.

**Alternative considered:** A `type: redshift` source on `redshift-connector`, which signs in with IAM, and a `type: synapse` source on `mssql-python`, which signs in with Entra ID.

**Why rejected:** Each would add a runtime dependency: `redshift-connector` brings boto3, and `mssql-python` brings a package of ODBC binaries under a proprietary license. Local and pushdown reads of one table would also take two drivers, so a difference between them could come from the driver rather than the engine. Sign-in through ConnectorX is a password only, which Redshift's temporary IAM credentials and SQL Server logins both give.

**How to reverse:** Add the type and its connector beside `SnowflakeConfig` in `models.py` and `_WAREHOUSES`, and remove its scheme from `DATABASE_PUSHDOWN_SCHEMES` and `_DATABASE_WAREHOUSES`.
