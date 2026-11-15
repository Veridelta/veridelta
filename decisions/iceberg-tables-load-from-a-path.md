---
type: Decision
title: An Iceberg table loads from its path, never from a catalog
description: Why an Iceberg table_uri names a metadata file or folder, and a catalog identifier is refused rather than looked up in the default catalog.
status: stable
decided: 2026-11-14
generated: { by: claude-code, at: 2026-11-14T22:49:58+00:00 }
---

**Claim:** An Iceberg `table_uri` names the table's metadata file or folder, which `_static_table` in `src/veridelta/connectors/lakehouse.py` loads with a FileIO built from `storage_options` alone. `IcebergConfig` refuses a `table_uri` with no slash or backslash, which Polars would look up in the default catalog.

**Evidence:** PyIceberg 0.12 builds a loaded table's FileIO from the catalog's properties merged with the table's own metadata properties, in `Catalog._load_file_io` (`pyiceberg/catalog/__init__.py`), which the SQL, Glue, Hive, DynamoDB, and BigQuery catalogs call while they load a table. `load_file_io` then imports the class a `py-io-impl` property names. `TestIcebergMetadataProperties` in `tests/integration/test_lakehouse_sources.py` writes that property into a table's metadata, and fails on a loader that imports it.

**Alternative considered:** Keep reading a catalog identifier through the default catalog, as Polars does, and drop the `py-` properties from the table it returns.

**Why rejected:** The catalog imports the class inside `load_table`, before it returns the table, so nothing Veridelta does afterwards can stop it. Vetting the properties first would mean loading each catalog's table metadata by hand, once for every catalog PyIceberg supports.

**How to reverse:** Remove `_refuse_catalog_identifier` from `IcebergConfig` in `src/veridelta/models.py`, and in `IcebergConnector.connect` pass a name with no slash to `pl.scan_iceberg` as it was, once PyIceberg stops reading a class name from table properties.
