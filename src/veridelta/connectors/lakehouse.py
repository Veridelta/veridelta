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

from veridelta.connectors.base import ReaderConnector, shown_location, without_location
from veridelta.exceptions import ConnectorError, missing_extra
from veridelta.models import DeltaLakeConfig, IcebergConfig

logger = logging.getLogger(__name__)

_UNCONNECTED = "Lakehouse connector is not connected. Call connect() first."
_DELTA_EXTRA = missing_extra("delta", "Reading a Delta Lake table")
_ICEBERG_EXTRA = missing_extra("iceberg", "Reading an Iceberg table")


class DeltaLakeConnector(ReaderConnector):
    """Delta Lake scanner backed by `pl.scan_delta`.

    `connect()` opens a lazy scan of `DeltaLakeConfig.table_uri`, pinned to
    `version` when one is set, and `lazyframe()` hands that scan to the local
    engine. No SQL is involved: the comparison runs in Polars. Requires the
    `delta` extra (`uv add 'veridelta[delta]'`).
    """

    def __init__(self, config: DeltaLakeConfig) -> None:
        """Initialize the connector with validated Delta Lake settings.

        Args:
            config (DeltaLakeConfig): Frozen table URI and optional version.
        """
        self._config = config
        self._frame: pl.LazyFrame | None = None

    def connect(self) -> None:
        """Open a lazy `pl.scan_delta` of the configured table and read its log.

        Raises:
            ConnectorError: If the `deltalake` extra is missing, or the table
                or version cannot be read.
        """
        storage_options = self._config.storage_options or None
        try:
            frame = pl.scan_delta(
                self._config.table_uri,
                version=self._config.version,
                storage_options=storage_options,
            )
            # Polars defers the scan, so read the log now: a missing table or
            # version then fails here, named, instead of partway through a run.
            frame.collect_schema()
        except ImportError as exc:
            raise ConnectorError(_DELTA_EXTRA) from exc
        except Exception as exc:
            where = shown_location(self._config.table_uri)
            logger.warning("Delta Lake scan of %s failed", where)
            raise ConnectorError(
                f"Delta Lake scan of '{where}' failed: "
                f"{without_location(str(exc), self._config.table_uri)}"
            ) from exc
        self._frame = frame
        logger.info(
            "Opened Delta Lake scan of %s (version=%s)",
            shown_location(self._config.table_uri),
            "latest" if self._config.version is None else self._config.version,
        )

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


class IcebergConnector(ReaderConnector):
    """Apache Iceberg scanner backed by `pl.scan_iceberg`.

    `connect()` opens a lazy scan of `IcebergConfig.table_uri`, pinned to
    `snapshot_id` when one is set, and `lazyframe()` hands that scan to the
    local engine. As with Delta Lake, the comparison runs in Polars. Requires
    the `iceberg` extra (`uv add 'veridelta[iceberg]'`).
    """

    def __init__(self, config: IcebergConfig) -> None:
        """Initialize the connector with validated Iceberg settings.

        Args:
            config (IcebergConfig): Frozen table URI and storage options.
        """
        self._config = config
        self._frame: pl.LazyFrame | None = None

    def connect(self) -> None:
        """Open a lazy `pl.scan_iceberg` of the configured table and read its metadata.

        Raises:
            ConnectorError: If the `pyiceberg` extra is missing, or the table
                or snapshot cannot be read.
        """
        try:
            frame = pl.scan_iceberg(
                self._config.table_uri,
                snapshot_id=self._config.snapshot_id,
                storage_options=self._config.storage_options or None,
            )
            # Polars defers the scan, so read the metadata now: a missing table
            # then fails here, named, instead of partway through a run. Polars
            # looks a snapshot up only to read rows, so time travel reads one.
            if self._config.snapshot_id is None:
                frame.collect_schema()
            else:
                frame.head(1).collect()
        except ImportError as exc:
            raise ConnectorError(_ICEBERG_EXTRA) from exc
        except Exception as exc:
            where = shown_location(self._config.table_uri)
            logger.warning("Iceberg scan of %s failed", where)
            raise ConnectorError(
                f"Iceberg scan of '{where}' failed: "
                f"{without_location(str(exc), self._config.table_uri)}"
            ) from exc
        self._frame = frame
        logger.info(
            "Opened Iceberg scan of %s (snapshot_id=%s)",
            shown_location(self._config.table_uri),
            "latest" if self._config.snapshot_id is None else self._config.snapshot_id,
        )

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
