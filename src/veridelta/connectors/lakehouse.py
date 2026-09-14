# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Lakehouse-native connectors for Delta Lake and Apache Iceberg.

Scanners return unevaluated Polars LazyFrames. Optional extras (`deltalake`,
`pyiceberg`) are imported only through Polars' native `scan_*` APIs.

A missing extra and a failed scan are reported separately: the former names
the extra to install, the latter names the table and carries the scanner's own
message, so a bad snapshot or an unreachable bucket never reads as an install
problem. Scan lifecycle is logged under the `veridelta.connectors.lakehouse`
logger; log lines carry the table URI and pin, never `storage_options`.
"""

import logging

import polars as pl

from veridelta.connectors.base import PushdownQueryType, VerideltaConnector
from veridelta.exceptions import ConnectorError
from veridelta.models import DeltaLakeConfig, IcebergConfig

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_UNCONNECTED = "Lakehouse connector is not connected. Call connect() first."
_PUSHDOWN_UNSUPPORTED = "SQL pushdown is warehouse-only; lakehouse connectors use lazy scans."
_DELTA_EXTRA = "Delta Lake extra is not installed. Install it with: uv sync --extra delta"
_ICEBERG_EXTRA = "Iceberg extra is not installed. Install it with: uv sync --extra iceberg"


class DeltaLakeConnector(VerideltaConnector):
    """Delta Lake scanner backed by `pl.scan_delta`.

    `connect()` opens a lazy scan of `DeltaLakeConfig.table_uri`, pinned to
    `version` when one is set, and `lazyframe()` hands that scan to the local
    engine. No SQL is involved: `execute_pushdown` always raises because a
    Delta table has no compute to push work into. Requires the `delta` extra
    (`uv add 'veridelta[delta]'`).
    """

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
        except ImportError as exc:
            raise ConnectorError(_DELTA_EXTRA) from exc
        except Exception as exc:
            logger.warning("Delta Lake scan of %s failed", self._config.table_uri)
            raise ConnectorError(
                f"Delta Lake scan of '{self._config.table_uri}' failed: {exc}"
            ) from exc
        logger.info(
            "Opened Delta Lake scan of %s (version=%s)",
            self._config.table_uri,
            "latest" if self._config.version is None else self._config.version,
        )

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Reject SQL pushdown; lakehouse work stays on the lazy scan.

        Args:
            statement (str): Unused SQL payload reserved by the ABC.
            query_type (PushdownQueryType): Unused warehouse round-trip tag.

        Returns:
            pl.LazyFrame: Never returned; lakehouse diffs use `connect()`.

        Raises:
            ConnectorError: Always; SQL pushdown is warehouse-only.
        """
        _ = statement
        _ = query_type
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

    def close(self) -> None:
        """Drop the scan handle. Idempotent; `connect()` reopens it."""
        self._frame = None


class IcebergConnector(VerideltaConnector):
    """Apache Iceberg scanner backed by `pl.scan_iceberg`.

    `connect()` opens a lazy scan of `IcebergConfig.table_uri`, pinned to
    `snapshot_id` when one is set, and `lazyframe()` hands that scan to the
    local engine. As with Delta Lake, `execute_pushdown` always raises; the
    comparison runs in Polars. Requires the `iceberg` extra
    (`uv add 'veridelta[iceberg]'`).
    """

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
            if self._config.snapshot_id is not None:
                self._frame = pl.scan_iceberg(
                    self._config.table_uri,
                    snapshot_id=self._config.snapshot_id,
                    storage_options=storage_options,
                )
            else:
                self._frame = pl.scan_iceberg(
                    self._config.table_uri,
                    storage_options=storage_options,
                )
        except ImportError as exc:
            raise ConnectorError(_ICEBERG_EXTRA) from exc
        except Exception as exc:
            logger.warning("Iceberg scan of %s failed", self._config.table_uri)
            raise ConnectorError(
                f"Iceberg scan of '{self._config.table_uri}' failed: {exc}"
            ) from exc
        logger.info(
            "Opened Iceberg scan of %s (snapshot_id=%s)",
            self._config.table_uri,
            "latest" if self._config.snapshot_id is None else self._config.snapshot_id,
        )

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Reject SQL pushdown; lakehouse work stays on the lazy scan.

        Args:
            statement (str): Unused SQL payload reserved by the ABC.
            query_type (PushdownQueryType): Unused warehouse round-trip tag.

        Returns:
            pl.LazyFrame: Never returned; lakehouse diffs use `connect()`.

        Raises:
            ConnectorError: Always; SQL pushdown is warehouse-only.
        """
        _ = statement
        _ = query_type
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

    def close(self) -> None:
        """Drop the scan handle. Idempotent; `connect()` reopens it."""
        self._frame = None
