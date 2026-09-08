# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Lakehouse-native connectors for Delta Lake and Apache Iceberg.

Scanners return unevaluated Polars LazyFrames. Optional extras (`deltalake`,
`pyiceberg`) are imported only through Polars' native `scan_*` APIs.
"""

import polars as pl

from veridelta.connectors.base import VerideltaConnector
from veridelta.exceptions import ConnectorError
from veridelta.models import DeltaLakeConfig, IcebergConfig

_UNCONNECTED = "Lakehouse connector is not connected. Call connect() first."
_PUSHDOWN_UNSUPPORTED = "SQL pushdown is warehouse-only; lakehouse connectors use lazy scans."


class DeltaLakeConnector(VerideltaConnector):
    """Delta Lake scanner backed by `pl.scan_delta`."""

    def __init__(self, config: DeltaLakeConfig) -> None:
        """Initialize the connector with validated Delta Lake settings.

        Args:
            config (DeltaLakeConfig): Frozen table URI and optional version.
        """
        self._config = config
        self._frame: pl.LazyFrame | None = None

    def connect(self) -> None:
        """Open an unevaluated `pl.scan_delta` handle for the configured table.

        Raises:
            ConnectorError: If the `deltalake` extra is missing or the scan fails.
        """
        storage_options = self._config.storage_options or None
        try:
            self._frame = pl.scan_delta(
                self._config.table_uri,
                version=self._config.version,
                storage_options=storage_options,
            )
        except (ImportError, pl.exceptions.PolarsError) as exc:
            raise ConnectorError(
                "Delta Lake scan failed. Install the optional extra with: uv sync --extra delta"
            ) from exc

    def execute_pushdown(self, statement: str) -> pl.LazyFrame:
        """Reject SQL pushdown; lakehouse work stays on the lazy scan.

        Args:
            statement (str): Unused SQL payload reserved by the ABC.

        Returns:
            pl.LazyFrame: Never returned; lakehouse diffs use `connect()`.

        Raises:
            ConnectorError: Always; SQL pushdown is warehouse-only.
        """
        _ = statement
        raise ConnectorError(_PUSHDOWN_UNSUPPORTED)

    def fetch_schema(self) -> pl.Schema:
        """Return the Delta table schema without collecting the full dataset.

        Returns:
            pl.Schema: Column names and dtypes from the lazy scan.

        Raises:
            ConnectorError: If `connect()` has not been called.
        """
        if self._frame is None:
            raise ConnectorError(_UNCONNECTED)
        return self._frame.collect_schema()

    def lazyframe(self) -> pl.LazyFrame:
        """Return the unevaluated Delta scan established by `connect()`.

        Returns:
            pl.LazyFrame: Lazy table scan.

        Raises:
            ConnectorError: If `connect()` has not been called.
        """
        if self._frame is None:
            raise ConnectorError(_UNCONNECTED)
        return self._frame


class IcebergConnector(VerideltaConnector):
    """Apache Iceberg scanner backed by `pl.scan_iceberg`."""

    def __init__(self, config: IcebergConfig) -> None:
        """Initialize the connector with validated Iceberg settings.

        Args:
            config (IcebergConfig): Frozen table URI and storage options.
        """
        self._config = config
        self._frame: pl.LazyFrame | None = None

    def connect(self) -> None:
        """Open an unevaluated `pl.scan_iceberg` handle for the configured table.

        Raises:
            ConnectorError: If the `pyiceberg` extra is missing or the scan fails.
        """
        storage_options = self._config.storage_options or None
        try:
            self._frame = pl.scan_iceberg(
                self._config.table_uri,
                storage_options=storage_options,
            )
        except (ImportError, pl.exceptions.PolarsError) as exc:
            raise ConnectorError(
                "Iceberg scan failed. Install the optional extra with: uv sync --extra iceberg"
            ) from exc

    def execute_pushdown(self, statement: str) -> pl.LazyFrame:
        """Reject SQL pushdown; lakehouse work stays on the lazy scan.

        Args:
            statement (str): Unused SQL payload reserved by the ABC.

        Returns:
            pl.LazyFrame: Never returned; lakehouse diffs use `connect()`.

        Raises:
            ConnectorError: Always; SQL pushdown is warehouse-only.
        """
        _ = statement
        raise ConnectorError(_PUSHDOWN_UNSUPPORTED)

    def fetch_schema(self) -> pl.Schema:
        """Return the Iceberg table schema without collecting the full dataset.

        Returns:
            pl.Schema: Column names and dtypes from the lazy scan.

        Raises:
            ConnectorError: If `connect()` has not been called.
        """
        if self._frame is None:
            raise ConnectorError(_UNCONNECTED)
        return self._frame.collect_schema()

    def lazyframe(self) -> pl.LazyFrame:
        """Return the unevaluated Iceberg scan established by `connect()`.

        Returns:
            pl.LazyFrame: Lazy table scan.

        Raises:
            ConnectorError: If `connect()` has not been called.
        """
        if self._frame is None:
            raise ConnectorError(_UNCONNECTED)
        return self._frame
