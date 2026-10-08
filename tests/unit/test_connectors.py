# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for warehouse and lakehouse connector scaffolding."""

import logging
import traceback
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import DEFAULT, MagicMock, call
from urllib.parse import quote

import polars as pl
import pytest
from pydantic import BaseModel, ValidationError
from pytest_mock import MockerFixture

from veridelta.connectors import (
    BigQueryConnector,
    DatabaseConnector,
    DatabricksConnector,
    DeltaLakeConnector,
    DuckDBConnector,
    DuckDBPushdownSession,
    IcebergConnector,
    PostgresPushdownSession,
    PushdownSession,
    ReaderConnector,
    SnowflakeConnector,
    SQLDialect,
    VerideltaConnector,
)
from veridelta.connectors.base import mask_secrets, read_subject
from veridelta.connectors.sql import compile_postgres_columns_query
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import (
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    IcebergConfig,
    SnowflakeConfig,
)

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def _snowflake_config() -> SnowflakeConfig:
    """Build a minimal valid Snowflake configuration."""
    return SnowflakeConfig(
        account="xy12345",
        user="analyst",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
        table="ANALYTICS.PUBLIC.LEGACY_EVENTS",
    )


def _databricks_config() -> DatabricksConfig:
    """Build a minimal valid Databricks configuration."""
    return DatabricksConfig(
        server_hostname="adb.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc",
        table="main.default.legacy_events",
    )


def _delta_config() -> DeltaLakeConfig:
    """Build a minimal valid Delta Lake configuration."""
    return DeltaLakeConfig(table_uri="s3://lake/events")


def _iceberg_config() -> IcebergConfig:
    """Build a minimal valid Iceberg configuration."""
    return IcebergConfig(table_uri="s3://lake/iceberg/events")


def _sample_lazy_frame() -> pl.LazyFrame:
    """Return a small unevaluated frame for mocked lakehouse scans."""
    return pl.DataFrame({"id": [1, 2], "amount": [10.0, 20.0]}).lazy()


class TestConnectorConfigValidation:
    """Validate frozen, extra-forbid credential models."""

    @pytest.mark.parametrize(
        ("config", "unknown"),
        [
            pytest.param(_snowflake_config(), {"region": "us-east-1"}, id="snowflake"),
            pytest.param(_databricks_config(), {"cluster_id": "ignored"}, id="databricks"),
            pytest.param(_delta_config(), {"catalog": "main"}, id="delta"),
            pytest.param(_iceberg_config(), {"catalog": "main"}, id="iceberg"),
        ],
    )
    def test_it_forbids_unrecognized_fields(
        self, config: BaseModel, unknown: dict[str, str]
    ) -> None:
        """Ensure each connector config rejects a key it does not define."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            type(config).model_validate(config.model_dump() | unknown)

    def test_it_rejects_mutation_on_frozen_connector_configs(self) -> None:
        """Ensure credential models are immutable after construction."""
        snowflake = _snowflake_config()
        databricks = _databricks_config()
        delta = _delta_config()
        iceberg = _iceberg_config()

        with pytest.raises(ValidationError, match="frozen"):
            snowflake.account = "other"  # type: ignore[misc]
        with pytest.raises(ValidationError, match="frozen"):
            databricks.http_path = "/sql/other"  # type: ignore[misc]
        with pytest.raises(ValidationError, match="frozen"):
            delta.table_uri = "s3://other"  # type: ignore[misc]
        with pytest.raises(ValidationError, match="frozen"):
            iceberg.table_uri = "s3://other"  # type: ignore[misc]


class TestSharedHelpers:
    """Validate the helpers every connector shares for its messages."""

    def test_it_masks_each_set_secret_longest_first(self) -> None:
        """Ensure a secret inside another is masked whole, and an unset one changes nothing."""
        masked = mask_secrets("key=hunter22 pass=hunter2 token=", "hunter2", "hunter22", None, "")

        assert masked == "key=*** pass=*** token="
        assert mask_secrets("nothing set", None) == "nothing set"

    def test_it_names_a_table_or_the_query(self) -> None:
        """Ensure a message names the table it read, and never repeats a query."""
        assert read_subject("orders") == "table 'orders'"
        assert read_subject(None) == "the configured query"


class TestConnectorInterface:
    """Validate ABC instantiation and warehouse stubs."""

    def test_it_cannot_instantiate_the_abstract_connector(self) -> None:
        """Ensure VerideltaConnector remains an abstract interface."""
        with pytest.raises(TypeError, match="abstract"):
            VerideltaConnector()  # type: ignore[abstract]

    def test_a_reader_hands_out_what_connect_kept(self) -> None:
        """Ensure a reader needs only `connect()`, and refuses a read before it and after `close()`.

        The refusal names the kind of source, from the reader's own message.
        """

        class _Reader(ReaderConnector):
            _unconnected = "Test reader is not connected. Call connect() first."

            def connect(self) -> None:
                self._frame = _sample_lazy_frame()

        reader = _Reader()
        with pytest.raises(ConnectorError, match=r"^Test reader is not connected"):
            reader.lazyframe()
        reader.connect()
        assert reader.lazyframe().collect().height == 2
        reader.close()
        reader.close()
        with pytest.raises(ConnectorError, match=r"^Test reader is not connected"):
            reader.lazyframe()

    def test_a_session_must_run_sql(self) -> None:
        """Ensure a session that defines no `execute_pushdown` cannot be built."""

        class _Session(PushdownSession):
            def connect(self) -> None:
                return None

        with pytest.raises(TypeError, match="execute_pushdown"):
            _Session()  # type: ignore[abstract]

    @pytest.mark.parametrize(
        "reader", [DeltaLakeConnector, IcebergConnector, DatabaseConnector, DuckDBConnector]
    )
    def test_a_reader_scans_and_never_runs_sql(self, reader: type[VerideltaConnector]) -> None:
        """Ensure each reader is a `ReaderConnector` with no `execute_pushdown` to call."""
        assert issubclass(reader, ReaderConnector)
        assert not issubclass(reader, PushdownSession)
        assert not hasattr(reader, "execute_pushdown")

    @pytest.mark.parametrize(
        "session",
        [
            SnowflakeConnector,
            DatabricksConnector,
            BigQueryConnector,
            PostgresPushdownSession,
            DuckDBPushdownSession,
        ],
    )
    def test_a_session_runs_sql_and_never_scans(self, session: type[VerideltaConnector]) -> None:
        """Ensure each pushdown class is a `PushdownSession` with no `lazyframe` to call."""
        assert issubclass(session, PushdownSession)
        assert not issubclass(session, ReaderConnector)
        assert not hasattr(session, "lazyframe")

    def test_it_raises_connector_error_when_snowflake_extra_is_missing(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure Snowflake runtime methods fail without the optional extra."""
        mocker.patch("veridelta.connectors.warehouse.snowflake_connector", None)
        connector = SnowflakeConnector(_snowflake_config())

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[snowflake\]'"):
            connector.connect()
        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[snowflake\]'"):
            connector.execute_pushdown("SELECT 1")

    def test_it_raises_connector_error_when_databricks_extra_is_missing(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure Databricks runtime methods fail without the optional extra."""
        mocker.patch("veridelta.connectors.warehouse.databricks_sql", None)
        connector = DatabricksConnector(_databricks_config())

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[databricks\]'"):
            connector.connect()
        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[databricks\]'"):
            connector.execute_pushdown("SELECT 1")


class TestLakehouseConnectors:
    """Validate lazy Delta and Iceberg scan wiring without optional extras."""

    def test_it_raises_when_scanning_before_connect(self) -> None:
        """Ensure a frame is handed out only after `connect()` opened the scan."""
        delta = DeltaLakeConnector(_delta_config())
        iceberg = IcebergConnector(_iceberg_config())

        with pytest.raises(ConnectorError, match="not connected"):
            delta.lazyframe()
        with pytest.raises(ConnectorError, match="not connected"):
            iceberg.lazyframe()

    def test_it_connects_delta_via_scan_delta_and_returns_schema(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure connect() calls pl.scan_delta and the scan stays lazy."""
        lazy = _sample_lazy_frame()
        scan = mocker.patch("veridelta.connectors.lakehouse.pl.scan_delta", return_value=lazy)
        connector = DeltaLakeConnector(
            DeltaLakeConfig(
                table_uri="s3://lake/events",
                version=3,
                storage_options={"AWS_REGION": "us-east-1"},
            )
        )

        connector.connect()
        schema = connector.lazyframe().collect_schema()

        scan.assert_called_once_with(
            "s3://lake/events",
            storage_options={"AWS_REGION": "us-east-1"},
            version=3,
        )
        assert connector.lazyframe() is lazy
        assert schema.names() == ["id", "amount"]
        assert isinstance(lazy, pl.LazyFrame)

    def test_it_leaves_a_token_out_of_a_failed_scan(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure a table URI's query reaches neither the error nor the log, quoted or not."""
        uri = "s3://lake/events?token=tok-do-not-print"
        mocker.patch(
            "veridelta.connectors.lakehouse.pl.scan_delta",
            side_effect=RuntimeError(f"no table at {uri}"),
        )

        with (
            caplog.at_level(logging.WARNING, logger="veridelta.connectors.lakehouse"),
            pytest.raises(ConnectorError) as exc_info,
        ):
            DeltaLakeConnector(DeltaLakeConfig(table_uri=uri)).connect()

        assert str(exc_info.value) == (
            "Delta Lake scan of 's3://lake/events' failed: no table at s3://lake/events"
        )
        assert "tok-do-not-print" not in caplog.text
        assert "Delta Lake scan of s3://lake/events failed" in caplog.text

    def test_it_connects_iceberg_via_scan_iceberg_and_returns_schema(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure connect() calls pl.scan_iceberg and the scan stays lazy."""
        lazy = _sample_lazy_frame()
        scan = mocker.patch("veridelta.connectors.lakehouse.pl.scan_iceberg", return_value=lazy)
        connector = IcebergConnector(
            IcebergConfig(
                table_uri="s3://lake/iceberg/events",
                storage_options={"AWS_REGION": "us-east-1"},
            )
        )

        connector.connect()
        schema = connector.lazyframe().collect_schema()

        scan.assert_called_once_with(
            "s3://lake/iceberg/events",
            snapshot_id=None,
            storage_options={"AWS_REGION": "us-east-1"},
        )
        assert connector.lazyframe() is lazy
        assert schema.names() == ["id", "amount"]

    def test_it_passes_snapshot_id_into_scan_iceberg_when_configured(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure Iceberg time travel forwards snapshot_id to Polars."""
        lazy = _sample_lazy_frame()
        scan = mocker.patch("veridelta.connectors.lakehouse.pl.scan_iceberg", return_value=lazy)
        connector = IcebergConnector(
            IcebergConfig(table_uri="s3://lake/iceberg/events", snapshot_id=42)
        )

        connector.connect()

        scan.assert_called_once_with(
            "s3://lake/iceberg/events",
            snapshot_id=42,
            storage_options=None,
        )
        assert connector.lazyframe() is lazy

    def test_it_reports_an_invalid_iceberg_snapshot_as_a_scan_failure(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure a bad snapshot names the table and the cause, not a missing extra."""
        mocker.patch(
            "veridelta.connectors.lakehouse.pl.scan_iceberg",
            side_effect=pl.exceptions.ComputeError("snapshot_id 99 not found"),
        )
        connector = IcebergConnector(
            IcebergConfig(table_uri="s3://lake/iceberg/events", snapshot_id=99)
        )

        with (
            caplog.at_level(logging.WARNING, logger="veridelta.connectors.lakehouse"),
            pytest.raises(
                ConnectorError, match="Iceberg scan of 's3://lake/iceberg/events'"
            ) as info,
        ):
            connector.connect()

        assert "snapshot_id 99 not found" in str(info.value)
        assert "uv sync" not in str(info.value)
        assert isinstance(info.value.__cause__, pl.exceptions.ComputeError)
        assert "Iceberg scan of s3://lake/iceberg/events failed" in caplog.text

    def test_it_reports_a_delta_scan_failure_with_the_table_and_cause(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure any non-import scanner failure is wrapped with the table URI and message."""
        mocker.patch(
            "veridelta.connectors.lakehouse.pl.scan_delta",
            side_effect=OSError("bucket lake does not exist"),
        )
        connector = DeltaLakeConnector(_delta_config())

        with pytest.raises(ConnectorError, match="Delta Lake scan of 's3://lake/events'") as info:
            connector.connect()

        assert "bucket lake does not exist" in str(info.value)
        assert "uv sync" not in str(info.value)

    def test_it_wraps_missing_delta_extra_as_connector_error(self, mocker: MockerFixture) -> None:
        """Ensure ImportError from pl.scan_delta becomes an install hint."""
        mocker.patch(
            "veridelta.connectors.lakehouse.pl.scan_delta",
            side_effect=ImportError("deltalake is required"),
        )
        connector = DeltaLakeConnector(_delta_config())

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[delta\]'"):
            connector.connect()

    def test_it_wraps_missing_iceberg_extra_as_connector_error(self, mocker: MockerFixture) -> None:
        """Ensure ImportError from pl.scan_iceberg becomes an install hint."""
        mocker.patch(
            "veridelta.connectors.lakehouse.pl.scan_iceberg",
            side_effect=ImportError("pyiceberg is required"),
        )
        connector = IcebergConnector(_iceberg_config())

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[iceberg\]'"):
            connector.connect()

    def test_it_logs_the_scan_it_opened_without_storage_options(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure connect() logs the table and pin but never the credential map."""
        mocker.patch(
            "veridelta.connectors.lakehouse.pl.scan_delta", return_value=_sample_lazy_frame()
        )
        connector = DeltaLakeConnector(
            DeltaLakeConfig(
                table_uri="s3://lake/events",
                version=3,
                storage_options={"AWS_SECRET_ACCESS_KEY": "hunter2"},
            )
        )

        with caplog.at_level(logging.INFO, logger="veridelta.connectors.lakehouse"):
            connector.connect()

        assert "Opened Delta Lake scan of s3://lake/events (version=3)" in caplog.text
        assert "hunter2" not in caplog.text

    def test_it_drops_the_scan_handle_on_close_and_reopens_on_connect(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure close() returns a lakehouse connector to its unconnected state."""
        lazy = _sample_lazy_frame()
        scan = mocker.patch("veridelta.connectors.lakehouse.pl.scan_iceberg", return_value=lazy)
        connector = IcebergConnector(_iceberg_config())
        connector.close()  # before connect: a no-op

        connector.connect()
        assert connector.lazyframe() is lazy

        connector.close()
        connector.close()  # idempotent
        with pytest.raises(ConnectorError, match="not connected"):
            connector.lazyframe()

        connector.connect()
        assert connector.lazyframe() is lazy
        assert scan.call_count == 2

    def test_it_closes_on_context_exit(self, mocker: MockerFixture) -> None:
        """Ensure the context manager releases the scan even when the block raises."""
        mocker.patch(
            "veridelta.connectors.lakehouse.pl.scan_delta", return_value=_sample_lazy_frame()
        )
        connector = DeltaLakeConnector(_delta_config())

        def _scan_then_fail() -> None:
            with connector as managed:
                assert managed is connector
                managed.connect()
                raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            _scan_then_fail()

        with pytest.raises(ConnectorError, match="not connected"):
            connector.lazyframe()


class TestConnectorLifecycleDefaults:
    """Validate the lifecycle members every connector inherits from the ABC."""

    def test_it_provides_a_no_op_close_and_a_self_returning_context(self) -> None:
        """Ensure a reader that only connects and reads gets close() and the context protocol."""

        class _Minimal(ReaderConnector):
            def connect(self) -> None:
                return None

            def lazyframe(self) -> pl.LazyFrame:
                return _sample_lazy_frame()

        connector = _Minimal()
        connector.close()
        with connector as managed:
            assert managed is connector
        assert connector.lazyframe().collect().height == 2


_DATABASE_SECRET = "p@ss:w/rd %+&?#"
"""A password holding every character a URI treats specially."""

_CATALOG_SCHEMA = {"attname": pl.String, "atttypmod": pl.Int32, "is_numeric": pl.Boolean}
"""Columns of the Postgres catalog query a `table` read sends first."""


def _catalog(*columns: tuple[str, int, bool]) -> pl.DataFrame:
    """Build a Postgres catalog result: each column's name, typmod, and whether it is numeric."""
    return pl.DataFrame(list(columns), schema=_CATALOG_SCHEMA, orient="row")


def _read_database(
    mocker: MockerFixture,
    *,
    return_value: object = None,
    side_effect: object = None,
    catalog: pl.DataFrame | None = None,
    columns: pl.DataFrame | None = None,
    counts: tuple[int, int] = (0, 2),
    bounds: tuple[object, object] = (1, 2),
) -> MagicMock:
    """Patch the extra probe and Polars' reader, returning the reader mock.

    A Postgres `table` read asks the catalog for its `numeric` columns first.
    `catalog` answers that query, by default with no `numeric` columns. A SQL
    Server `table` read first reads the table's columns and no rows, which
    `columns` answers when it is given. A partitioned read first counts the
    column's NULL and other rows, which `counts` answers, then reads its range,
    which `bounds` answers. Every other statement returns the mock's
    `return_value`.
    """
    mocker.patch("veridelta.connectors.database.connectorx", object())
    answers = _catalog() if catalog is None else catalog

    def _answer(statement: str, uri: str, **partitions: object) -> object:
        if statement.startswith("SELECT attname"):
            return answers
        if columns is not None and statement.endswith(" WHERE 1 = 0"):
            return columns
        if statement.startswith("SELECT COUNT(*) - COUNT("):
            return pl.DataFrame({"null_rows": [counts[0]], "valued_rows": [counts[1]]})
        if statement.startswith("SELECT MIN("):
            return pl.DataFrame({"low": [bounds[0]], "high": [bounds[1]]})
        return DEFAULT

    read = MagicMock(
        return_value=return_value, side_effect=_answer if side_effect is None else side_effect
    )
    mocker.patch("veridelta.connectors.database.pl.read_database_uri", read)
    return read


class TestDatabaseConnector:
    """Validate the connector that reads a database table or query into Polars."""

    def test_it_reads_a_table_with_its_name_quoted(self, mocker: MockerFixture) -> None:
        """Ensure a table is read with each segment of its name quoted for the database."""
        frame = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        read = _read_database(mocker, return_value=frame)
        uri = "postgresql://analyst@db.internal:5432/sales"
        connector = DatabaseConnector(DatabaseConfig(uri=uri, table="public.orders"))

        connector.connect()

        read.assert_called_with('SELECT * FROM "public"."orders"', uri)
        assert connector.lazyframe().collect().equals(frame)
        assert connector.lazyframe().collect_schema() == frame.schema

    def test_it_sends_a_query_verbatim(self, mocker: MockerFixture) -> None:
        """Ensure a configured statement reaches the driver as written, with no catalog lookup."""
        read = _read_database(mocker, return_value=pl.DataFrame({"n": [1]}))
        query = "SELECT id, total FROM orders WHERE region = 'EU'"
        uri = "postgresql://analyst@db.internal/sales"

        DatabaseConnector(DatabaseConfig(uri=uri, query=query)).connect()

        read.assert_called_once_with(query, uri)

    def test_it_percent_encodes_the_password_into_the_uri(self, mocker: MockerFixture) -> None:
        """Ensure a password with URI syntax in it still reaches the driver intact.

        A `${VAR}` expanded inside the URI is not encoded, so a password holding
        `@` or `/` would change which host the URI names. The separate field is
        encoded, and the host, port, path, and parameters are left as written.
        """
        read = _read_database(mocker, return_value=pl.DataFrame({"n": [1]}))
        config = DatabaseConfig(
            uri="postgresql://analyst@db.internal:5432/sales?sslmode=require",
            password=_DATABASE_SECRET,
            table="orders",
        )

        DatabaseConnector(config).connect()

        read.assert_called_with(
            'SELECT * FROM "orders"',
            "postgresql://analyst:p%40ss%3Aw%2Frd%20%25%2B%26%3F%23@db.internal:5432/sales"
            "?sslmode=require",
        )

    def test_it_explains_a_missing_extra_before_reading(self, mocker: MockerFixture) -> None:
        """Ensure a missing ConnectorX names the extra and never reaches Polars."""
        mocker.patch("veridelta.connectors.database.connectorx", None)
        read = mocker.patch("veridelta.connectors.database.pl.read_database_uri")
        connector = DatabaseConnector(DatabaseConfig(uri="sqlite:///srv/x.db", table="t"))

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[database\]'"):
            connector.connect()

        read.assert_not_called()

    def test_it_explains_missing_pyarrow_as_the_extra(self, mocker: MockerFixture) -> None:
        """Ensure a partial install reads as the same install hint, not an import trace."""
        _read_database(mocker, side_effect=ModuleNotFoundError("No module named 'pyarrow'"))
        connector = DatabaseConnector(
            DatabaseConfig(uri="postgresql://analyst@db.internal/sales", table="t")
        )

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[database\]'") as info:
            connector.connect()

        assert info.value.__cause__ is None

    def test_it_refuses_a_table_for_a_scheme_it_cannot_quote(self, mocker: MockerFixture) -> None:
        """Ensure an unquotable table fails as a configuration problem, before any read."""
        read = _read_database(mocker)
        connector = DatabaseConnector(
            DatabaseConfig(uri="trino://analyst@db.internal/hive", table="orders")
        )

        with pytest.raises(ConfigError, match="Write the statement in 'query' instead"):
            connector.connect()

        read.assert_not_called()

    @pytest.mark.parametrize(
        ("config", "secrets"),
        [
            pytest.param(
                DatabaseConfig(
                    uri="postgresql://analyst@db.internal/sales",
                    password=_DATABASE_SECRET,
                    table="orders",
                ),
                (_DATABASE_SECRET, quote(_DATABASE_SECRET, safe="")),
                id="password-field",
            ),
            pytest.param(
                DatabaseConfig(
                    uri="postgresql://analyst:pa%3Ass@db.internal/sales", table="orders"
                ),
                ("pa:ss", "pa%3Ass"),
                id="password-in-uri",
            ),
        ],
    )
    def test_it_keeps_passwords_out_of_read_failures(
        self, mocker: MockerFixture, config: DatabaseConfig, secrets: tuple[str, ...]
    ) -> None:
        """Ensure a failed read names what failed without any form of the password.

        Drivers echo connection strings in their errors, and Polars re-raises
        with the unscrubbed error attached as the cause, so the connector
        rewrites the message and raises without the original attached.
        """
        leak = " | ".join(
            f"password={secret} uri=postgresql://analyst:{secret}@db" for secret in secrets
        )
        _read_database(mocker, side_effect=RuntimeError(f"connection refused: {leak}"))

        with pytest.raises(ConnectorError) as info:
            DatabaseConnector(config).connect()

        message = str(info.value)
        assert message.startswith("Database read of table 'orders' from 'postgresql://analyst"), (
            message
        )
        assert "connection refused" in message
        assert "***" in message
        assert info.value.__cause__ is None
        assert info.value.__suppress_context__ is True
        rendered = "".join(traceback.format_exception(info.value))
        for secret in secrets:
            assert secret not in rendered

    def test_it_masks_a_credential_parameter_the_driver_repeats(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure `?password=` in the URI is masked when a driver error quotes the URI.

        Other parameters, such as `sslmode`, stay readable in the driver's text.
        """
        uri = "postgresql://analyst@db.internal/sales?sslmode=require&password=pw-do-not-print"
        _read_database(mocker, side_effect=RuntimeError(f"could not connect to {uri}"))
        config = DatabaseConfig(uri=uri, table="orders")

        with pytest.raises(ConnectorError) as info:
            DatabaseConnector(config).connect()

        assert "pw-do-not-print" not in str(info.value)
        assert "sslmode=require&password=***" in str(info.value)
        assert "from 'postgresql://analyst@db.internal/sales' failed" in str(info.value)

    def test_it_names_a_query_source_without_its_sql(self, mocker: MockerFixture) -> None:
        """Ensure a failed query is identified without repeating the statement."""
        _read_database(mocker, side_effect=RuntimeError("syntax error"))
        config = DatabaseConfig(uri="sqlite:///srv/x.db", query="SELECT secret_column FROM t")
        mocker.patch("veridelta.connectors.database.Path.is_file", return_value=True)

        with pytest.raises(ConnectorError) as info:
            DatabaseConnector(config).connect()

        assert "Database read of the configured query from 'sqlite:///srv/x.db' failed" in str(
            info.value
        )
        assert "secret_column" not in str(info.value)

    def test_it_logs_reads_without_secrets_or_sql(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure log lines name the source and masked URI, never a password or statement."""
        config = DatabaseConfig(
            uri="postgresql://analyst:hunter2@db.internal/sales", query="SELECT hidden FROM t"
        )
        read = _read_database(mocker, return_value=pl.DataFrame({"n": [1, 2, 3]}))

        with caplog.at_level(logging.INFO, logger="veridelta.connectors.database"):
            DatabaseConnector(config).connect()
            read.side_effect = RuntimeError("down")
            with pytest.raises(ConnectorError):
                DatabaseConnector(config).connect()

        assert (
            "Read 3 rows of the configured query from postgresql://analyst:***@db.internal/sales"
            in caplog.text
        )
        assert "Database read of the configured query from" in caplog.text
        assert "hunter2" not in caplog.text
        assert "hidden" not in caplog.text

    def test_it_refuses_a_missing_sqlite_file(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Ensure a mistyped SQLite path fails instead of creating an empty database.

        ConnectorX opens SQLite files in create mode, so a missing path would
        otherwise leave an empty file behind and fail on the first table.
        """
        read = _read_database(mocker)
        missing = tmp_path / "legacy.db"
        connector = DatabaseConnector(
            DatabaseConfig(uri="sqlite://" + quote(str(missing)), table="orders")
        )

        with pytest.raises(ConnectorError, match="does not exist") as info:
            connector.connect()

        assert str(missing) in str(info.value)
        read.assert_not_called()
        assert not missing.exists()

    def test_it_encodes_a_sqlite_path_for_connectorx(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure a path written plainly, with spaces, still reaches ConnectorX encoded."""
        read = _read_database(mocker, return_value=pl.DataFrame({"n": [1]}))
        database = tmp_path / "legacy data.db"
        database.write_bytes(b"")

        DatabaseConnector(DatabaseConfig(uri=f"sqlite://{database}", table="orders")).connect()

        read.assert_called_once_with('SELECT * FROM "orders"', "sqlite://" + quote(str(database)))

    def test_it_drops_the_frame_on_close_and_rereads_on_connect(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure close() returns the connector to its unconnected state."""
        read = _read_database(mocker, return_value=pl.DataFrame({"n": [1]}))
        connector = DatabaseConnector(
            DatabaseConfig(uri="mysql://analyst@db.internal/sales", table="t")
        )
        connector.close()  # before connect: a no-op

        with pytest.raises(ConnectorError, match="not connected"):
            connector.lazyframe()

        connector.connect()
        connector.close()
        connector.close()  # idempotent
        with pytest.raises(ConnectorError, match="not connected"):
            connector.lazyframe()

        connector.connect()
        assert connector.lazyframe().collect().height == 1
        assert read.call_count == 2

    def test_it_closes_on_context_exit(self, mocker: MockerFixture) -> None:
        """Ensure the context manager releases the frame even when the block raises."""
        _read_database(mocker, return_value=pl.DataFrame({"n": [1]}))
        connector = DatabaseConnector(
            DatabaseConfig(uri="postgresql://analyst@db.internal/sales", table="t")
        )

        def _read_then_fail() -> None:
            with connector as managed:
                managed.connect()
                raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            _read_then_fail()

        with pytest.raises(ConnectorError, match="not connected"):
            connector.lazyframe()


class TestDatabaseSchemaProbe:
    """Validate `DatabaseConnector(..., probe=True)`, which reads a table's columns only."""

    def test_it_reads_no_rows(self, mocker: MockerFixture) -> None:
        """Ensure the probe sends the zero-row statement and keeps the schema."""
        frame = pl.DataFrame(schema={"id": pl.Int64, "name": pl.String})
        read = _read_database(mocker, return_value=frame)
        uri = "postgresql://analyst@db.internal/sales"

        with DatabaseConnector(DatabaseConfig(uri=uri, table="orders"), probe=True) as connector:
            connector.connect()
            schema = connector.lazyframe().collect_schema()

        read.assert_called_with('SELECT * FROM "orders" WHERE 1 = 0', uri)
        assert schema == frame.schema

    def test_it_refuses_to_probe_a_query(self, mocker: MockerFixture) -> None:
        """Ensure a query is never run whole when only its columns were asked for."""
        read = _read_database(mocker, return_value=pl.DataFrame())
        config = DatabaseConfig(uri="sqlite:///srv/x.db", query="SELECT * FROM t")

        with pytest.raises(ConfigError, match="probe reads a 'table'"):
            DatabaseConnector(config, probe=True).connect()

        read.assert_not_called()


_NUMERIC_10_2 = 655366
"""Postgres `atttypmod` of a `numeric(10, 2)` column: (10 << 16 | 2) + 4."""


class TestPostgresDeclaredScale:
    """Validate that a Postgres table's `numeric` columns keep their declared precision and scale.

    ConnectorX reads every `numeric` as Decimal(38, 10), which rounds past ten
    decimal places and fails on a value with 19 or more integer digits.
    """

    def test_it_reads_each_declared_numeric_as_text_and_casts_it_back(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a `numeric` arrives at its declared type, with every digit it stores."""
        catalog = _catalog(
            ("id", -1, False), ("amount", _NUMERIC_10_2, True), ("units", 1310724, True)
        )
        as_text = pl.DataFrame(
            {"id": [1, 2], "amount": ["1.50", "-7.25"], "units": ["12345678901234567890", "0"]}
        )
        read = _read_database(mocker, return_value=as_text, catalog=catalog)
        uri = "postgresql://analyst@db.internal/sales"

        with DatabaseConnector(DatabaseConfig(uri=uri, table="public.orders")) as connector:
            connector.connect()
            frame = connector.lazyframe().collect()

        assert read.call_args_list == [
            call(compile_postgres_columns_query("public.orders"), uri),
            call(
                'SELECT "id", CAST("amount" AS TEXT) AS "amount", '
                'CAST("units" AS TEXT) AS "units" FROM "public"."orders"',
                uri,
            ),
        ]
        assert frame.schema == pl.Schema(
            {"id": pl.Int64(), "amount": pl.Decimal(10, 2), "units": pl.Decimal(20, 0)}
        )
        assert frame.get_column("amount").to_list() == [Decimal("1.50"), Decimal("-7.25")]
        assert frame.get_column("units").to_list() == [
            Decimal("12345678901234567890"),
            Decimal("0"),
        ]

    @pytest.mark.parametrize(
        ("typmod", "is_numeric"),
        [
            pytest.param(-1, True, id="unconstrained"),
            pytest.param(2621446, True, id="precision-40"),
            pytest.param(329730, True, id="negative-scale"),
            pytest.param(_NUMERIC_10_2, False, id="not-numeric"),
        ],
    )
    def test_it_keeps_the_default_read_for_a_column_without_an_exact_decimal(
        self, mocker: MockerFixture, typmod: int, is_numeric: bool
    ) -> None:
        """Ensure only a `numeric` Polars holds at its declared precision and scale is cast."""
        catalog = _catalog(("amount", typmod, is_numeric))
        read = _read_database(mocker, return_value=pl.DataFrame({"amount": [1]}), catalog=catalog)
        uri = "postgresql://analyst@db.internal/sales"

        DatabaseConnector(DatabaseConfig(uri=uri, table="orders")).connect()

        read.assert_called_with('SELECT * FROM "orders"', uri)

    def test_it_probes_a_declared_numeric_at_its_declared_type(self, mocker: MockerFixture) -> None:
        """Ensure `validate --schemas` sees the type a run reads."""
        catalog = _catalog(("amount", _NUMERIC_10_2, True))
        empty = pl.DataFrame(schema={"amount": pl.String})
        read = _read_database(mocker, return_value=empty, catalog=catalog)
        uri = "postgres://analyst@db.internal/sales"

        with DatabaseConnector(DatabaseConfig(uri=uri, table="orders"), probe=True) as connector:
            connector.connect()
            schema = connector.lazyframe().collect_schema()

        read.assert_called_with(
            'SELECT CAST("amount" AS TEXT) AS "amount" FROM "orders" WHERE 1 = 0', uri
        )
        assert schema == pl.Schema({"amount": pl.Decimal(10, 2)})

    def test_it_names_the_column_holding_a_value_no_decimal_holds(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a stored NaN fails the read with its column named, and nothing is kept."""
        catalog = _catalog(("amount", _NUMERIC_10_2, True))
        _read_database(
            mocker, return_value=pl.DataFrame({"amount": ["1.50", "NaN"]}), catalog=catalog
        )
        connector = DatabaseConnector(
            DatabaseConfig(uri="postgresql://analyst@db.internal/sales", table="orders")
        )

        with pytest.raises(
            ConnectorError,
            match="Column 'amount' of table 'orders' holds a value with no decimal form",
        ) as info:
            connector.connect()

        assert info.value.__cause__ is None
        with pytest.raises(ConnectorError, match="not connected"):
            connector.lazyframe()


_MSSQL_URI = "mssql://analyst@db.internal:1433/sales"

_MSSQL_COLUMNS = pl.DataFrame(
    schema={"id": pl.Int64, "placed": pl.Datetime("us", "UTC"), "seen": pl.Datetime("us")}
)
"""A SQL Server table's columns and no rows: a `DATETIMEOFFSET`, then a `DATETIME2`."""


class TestSqlServerDatetimeOffset:
    """Validate that a SQL Server table's `DATETIMEOFFSET` columns keep their instants.

    ConnectorX shifts a `DATETIMEOFFSET` value by its offset a second time, so
    `12:00 +02:00` would arrive as 08:00 in UTC. A value at offset zero is not
    shifted, so the read moves each such column there first.
    """

    def test_it_reads_each_datetimeoffset_at_offset_zero(self, mocker: MockerFixture) -> None:
        """Ensure only the columns read in UTC are switched, and every column keeps its place."""
        rows = pl.DataFrame(
            {
                "id": [1],
                "placed": [datetime(2026, 1, 1, 10, tzinfo=UTC)],
                "seen": [datetime(2026, 1, 1, 12)],
            }
        )
        read = _read_database(mocker, return_value=rows, columns=_MSSQL_COLUMNS)

        with DatabaseConnector(DatabaseConfig(uri=_MSSQL_URI, table="dbo.orders")) as connector:
            connector.connect()
            frame = connector.lazyframe().collect()

        assert read.call_args_list == [
            call("SELECT * FROM [dbo].[orders] WHERE 1 = 0", _MSSQL_URI),
            call(
                "SELECT [id], SWITCHOFFSET([placed], '+00:00') AS [placed], [seen] "
                "FROM [dbo].[orders]",
                _MSSQL_URI,
            ),
        ]
        assert frame.equals(rows)

    def test_it_reads_a_table_without_one_as_written(self, mocker: MockerFixture) -> None:
        """Ensure a table with no `DATETIMEOFFSET` column is read whole, as on any server."""
        columns = pl.DataFrame(schema={"id": pl.Int64, "seen": pl.Datetime("us")})
        read = _read_database(mocker, return_value=pl.DataFrame({"id": [1]}), columns=columns)

        DatabaseConnector(DatabaseConfig(uri=_MSSQL_URI, table="orders")).connect()

        assert read.call_args_list == [
            call("SELECT * FROM [orders] WHERE 1 = 0", _MSSQL_URI),
            call("SELECT * FROM [orders]", _MSSQL_URI),
        ]

    def test_it_probes_in_one_read(self, mocker: MockerFixture) -> None:
        """Ensure `validate --schemas` reads once, since a switched column keeps its type."""
        read = _read_database(mocker, return_value=_MSSQL_COLUMNS)
        config = DatabaseConfig(uri=_MSSQL_URI, table="orders")

        with DatabaseConnector(config, probe=True) as connector:
            connector.connect()
            schema = connector.lazyframe().collect_schema()

        read.assert_called_once_with("SELECT * FROM [orders] WHERE 1 = 0", _MSSQL_URI)
        assert schema == _MSSQL_COLUMNS.schema

    def test_it_sends_a_query_verbatim(self, mocker: MockerFixture) -> None:
        """Ensure a query is never probed or rewritten, so it needs its own `SWITCHOFFSET`."""
        read = _read_database(mocker, return_value=pl.DataFrame({"n": [1]}), columns=_MSSQL_COLUMNS)
        query = "SELECT id, placed FROM dbo.orders"

        DatabaseConnector(DatabaseConfig(uri=_MSSQL_URI, query=query)).connect()

        read.assert_called_once_with(query, _MSSQL_URI)

    def test_it_keeps_the_password_out_of_a_failed_column_read(self, mocker: MockerFixture) -> None:
        """Ensure a failure before the rows fails as the read does, with the password masked."""
        encoded = quote(_DATABASE_SECRET, safe="")
        read = _read_database(mocker, side_effect=RuntimeError(f"login failed: {encoded}"))
        config = DatabaseConfig(uri=_MSSQL_URI, password=_DATABASE_SECRET, table="orders")

        with pytest.raises(
            ConnectorError, match=r"^Database read of table 'orders' from 'mssql://analyst"
        ) as info:
            DatabaseConnector(config).connect()

        read.assert_called_once_with(
            "SELECT * FROM [orders] WHERE 1 = 0",
            f"mssql://analyst:{encoded}@db.internal:1433/sales",
        )
        assert encoded not in str(info.value)
        assert info.value.__cause__ is None


_POSTGRES_URI = "postgresql://analyst@db.internal:5432/sales"


def _postgres_session(password: str | None = None) -> PostgresPushdownSession:
    """Build an unconnected session for a pushed-down Postgres table."""
    return PostgresPushdownSession(
        DatabaseConfig(uri=_POSTGRES_URI, password=password, table="orders", pushdown=True)
    )


def _setting(value: str) -> pl.DataFrame:
    """Return the one-row result of the literal-rules check."""
    return pl.DataFrame({"value": [value]})


class TestPartitionedDatabaseRead:
    """Validate a `table` read that ConnectorX splits across parallel connections."""

    _URI = "mysql://analyst@db.internal/sales"

    def _config(self, **fields: object) -> DatabaseConfig:
        return DatabaseConfig.model_validate(
            {"uri": self._URI, "table": "orders", "partition_on": "order_id", "partitions": 4}
            | fields
        )

    def test_it_measures_the_column_then_splits_the_read(self, mocker: MockerFixture) -> None:
        """Ensure the read checks for NULLs and finds the range, then hands both to ConnectorX."""
        frame = pl.DataFrame({"order_id": [1, 7]})
        read = _read_database(mocker, return_value=frame, bounds=(1, 7))

        with DatabaseConnector(self._config()) as connector:
            connector.connect()
            assert connector.lazyframe().collect().equals(frame)

        assert read.call_args_list == [
            call(
                "SELECT COUNT(*) - COUNT(order_id) AS null_rows, "
                "COUNT(order_id) AS valued_rows FROM `orders`",
                self._URI,
            ),
            call("SELECT MIN(order_id) AS low, MAX(order_id) AS high FROM `orders`", self._URI),
            call(
                "SELECT * FROM `orders`",
                self._URI,
                partition_on="order_id",
                partition_num=4,
                partition_range=(1, 7),
            ),
        ]

    def test_it_reads_an_empty_table_in_one_piece(self, mocker: MockerFixture) -> None:
        """Ensure a table with no values to range over is read whole, keeping its columns."""
        read = _read_database(mocker, return_value=pl.DataFrame(), counts=(0, 0))

        DatabaseConnector(self._config()).connect()

        assert read.call_args_list[1:] == [call("SELECT * FROM `orders`", self._URI)]

    @pytest.mark.parametrize(
        "bounds",
        [
            pytest.param(("a", "z"), id="text"),
            pytest.param((Decimal("1"), Decimal("9")), id="decimal"),
            pytest.param((1, 9.5), id="float"),
        ],
    )
    def test_it_refuses_a_column_that_does_not_hold_integers(
        self, mocker: MockerFixture, bounds: tuple[object, object]
    ) -> None:
        """Ensure ranges are only drawn over integers, which ConnectorX's ranges compare."""
        read = _read_database(mocker, return_value=pl.DataFrame(), bounds=bounds)

        with pytest.raises(ConnectorError, match="'order_id' of table 'orders' does not hold"):
            DatabaseConnector(self._config()).connect()

        assert read.call_count == 2

    def test_it_refuses_a_partition_column_holding_nulls(self, mocker: MockerFixture) -> None:
        """Ensure rows ConnectorX would leave out fail the read instead of vanishing."""
        read = _read_database(mocker, return_value=pl.DataFrame(), counts=(3, 5))

        with pytest.raises(
            ConnectorError, match="Column 'order_id' of table 'orders' is NULL in 3 of its rows"
        ):
            DatabaseConnector(self._config()).connect()

        assert read.call_count == 1

    def test_it_splits_the_postgres_read_that_keeps_declared_scale(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure the text select that keeps each `numeric` column's scale is what is split."""
        catalog = _catalog(("order_id", -1, False), ("amount", _NUMERIC_10_2, True))
        as_text = pl.DataFrame({"order_id": [1], "amount": ["1.50"]})
        read = _read_database(mocker, return_value=as_text, catalog=catalog)
        uri = "postgresql://analyst@db.internal/sales"

        with DatabaseConnector(self._config(uri=uri, partitions=2)) as connector:
            connector.connect()
            frame = connector.lazyframe().collect()

        assert read.call_args_list[3:] == [
            call(
                'SELECT "order_id", CAST("amount" AS TEXT) AS "amount" FROM "orders"',
                uri,
                partition_on="order_id",
                partition_num=2,
                partition_range=(1, 2),
            ),
        ]
        assert frame.schema == pl.Schema({"order_id": pl.Int64(), "amount": pl.Decimal(10, 2)})

    def test_it_splits_the_sql_server_read_that_keeps_each_instant(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure the select that moves each `DATETIMEOFFSET` to offset zero is what is split."""
        columns = pl.DataFrame(schema={"order_id": pl.Int64, "placed": pl.Datetime("us", "UTC")})
        read = _read_database(mocker, return_value=pl.DataFrame(), columns=columns)
        uri = "mssql://analyst@db.internal/sales"

        DatabaseConnector(self._config(uri=uri, partitions=2)).connect()

        assert read.call_args_list[0] == call("SELECT * FROM [orders] WHERE 1 = 0", uri)
        assert read.call_args_list[3:] == [
            call(
                "SELECT [order_id], SWITCHOFFSET([placed], '+00:00') AS [placed] FROM [orders]",
                uri,
                partition_on="order_id",
                partition_num=2,
                partition_range=(1, 2),
            ),
        ]

    def test_it_refuses_to_split_a_sql_server_read_naming_a_bracket(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a name ConnectorX would mangle while splitting the read fails before any row."""
        columns = pl.DataFrame(schema={"order_id": pl.Int64, "at] UTC": pl.Datetime("us", "UTC")})
        read = _read_database(mocker, return_value=pl.DataFrame(), columns=columns)

        with pytest.raises(
            ConnectorError, match=r"Column 'at\] UTC' of table 'orders' has '\]' in its name"
        ):
            DatabaseConnector(self._config(uri="mssql://analyst@db.internal/sales")).connect()

        assert read.call_count == 1

    def test_it_probes_a_partitioned_table_in_one_read(self, mocker: MockerFixture) -> None:
        """Ensure `validate --schemas`, which reads no rows, neither counts nor splits."""
        read = _read_database(mocker, return_value=pl.DataFrame(schema={"order_id": pl.Int64}))

        DatabaseConnector(self._config(), probe=True).connect()

        read.assert_called_once_with("SELECT * FROM `orders` WHERE 1 = 0", self._URI)


class TestPostgresPushdownSession:
    """Validate the session that runs compiled comparison SQL inside Postgres."""

    def test_it_compiles_for_postgres(self) -> None:
        """Ensure the engine compiles this session's statements in the Postgres dialect."""
        assert _postgres_session().compiler.dialect is SQLDialect.POSTGRES

    def test_it_checks_how_the_server_reads_string_literals(self, mocker: MockerFixture) -> None:
        """Ensure connecting asks whether backslashes in literals are plain text."""
        read = _read_database(mocker, return_value=_setting("on"))

        _postgres_session().connect()

        read.assert_called_once_with(
            "SELECT current_setting('standard_conforming_strings') AS value", _POSTGRES_URI
        )

    def test_it_refuses_a_server_that_reads_backslashes_as_escapes(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a literal can never be cut short by a backslash the compiler left alone.

        The Postgres dialect writes literals by the SQL standard, where a
        backslash is plain text. With `standard_conforming_strings` off, a value
        ending in a backslash would escape its own closing quote.
        """
        _read_database(mocker, return_value=_setting("off"))

        with pytest.raises(ConnectorError, match="standard_conforming_strings is off"):
            _postgres_session().connect()

    def test_it_runs_each_statement_as_one_read(self, mocker: MockerFixture) -> None:
        """Ensure a statement reaches ConnectorX as written and returns its rows lazily."""
        result = pl.DataFrame({"id": [3]})
        read = _read_database(mocker, return_value=_setting("on"))
        session = _postgres_session()
        session.connect()
        read.return_value = result

        frame = session.execute_pushdown('SELECT "id" FROM "orders"', query_type="added")

        read.assert_called_with('SELECT "id" FROM "orders"', _POSTGRES_URI)
        assert frame.collect().equals(result)

    def test_it_sends_the_password_field_encoded(self, mocker: MockerFixture) -> None:
        """Ensure a password with URI syntax reaches the driver as one intact field."""
        read = _read_database(mocker, return_value=_setting("on"))

        _postgres_session(_DATABASE_SECRET).connect()

        assert read.call_args.args[1] == (
            "postgresql://analyst:p%40ss%3Aw%2Frd%20%25%2B%26%3F%23@db.internal:5432/sales"
        )

    def test_it_reports_a_failed_statement_without_the_password(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a driver error names the statement kind but carries no secret."""
        read = _read_database(mocker, return_value=_setting("on"))
        session = _postgres_session(_DATABASE_SECRET)
        session.connect()
        read.side_effect = RuntimeError(f"auth failed for {quote(_DATABASE_SECRET, safe='')}")

        with pytest.raises(ConnectorError, match="Postgres mismatch statement") as info:
            session.execute_pushdown("SELECT 1")

        assert info.value.__cause__ is None
        assert _DATABASE_SECRET not in "".join(traceback.format_exception(info.value))
        assert quote(_DATABASE_SECRET, safe="") not in str(info.value)

    def test_it_explains_a_missing_extra_before_connecting(self, mocker: MockerFixture) -> None:
        """Ensure a missing ConnectorX names the extra and never reaches Polars."""
        mocker.patch("veridelta.connectors.database.connectorx", None)
        read = mocker.patch("veridelta.connectors.database.pl.read_database_uri")

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[database\]'"):
            _postgres_session().connect()

        read.assert_not_called()

    def test_it_explains_missing_pyarrow_as_the_extra(self, mocker: MockerFixture) -> None:
        """Ensure a partial install reads as the same install hint, not an import trace."""
        _read_database(mocker, side_effect=ModuleNotFoundError("No module named 'pyarrow'"))

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[database\]'"):
            _postgres_session().connect()

    def test_it_reads_declared_numeric_types_from_the_catalog(self, mocker: MockerFixture) -> None:
        """Ensure the session reports the declared type of each `numeric` Polars holds exactly."""
        catalog = _catalog(("id", -1, False), ("amount", _NUMERIC_10_2, True), ("free", -1, True))
        read = _read_database(mocker, return_value=_setting("on"), catalog=catalog)
        session = _postgres_session()
        session.connect()

        declared = session.declared_types("public.orders")

        read.assert_called_with(compile_postgres_columns_query("public.orders"), _POSTGRES_URI)
        assert declared == {"amount": pl.Decimal(10, 2)}

    def test_it_refuses_work_until_connected_and_after_closing(self, mocker: MockerFixture) -> None:
        """Ensure the lifecycle matches the other connectors, and closing twice is safe."""
        _read_database(mocker, return_value=_setting("on"))
        session = _postgres_session()

        with pytest.raises(ConnectorError, match="not connected"):
            session.execute_pushdown("SELECT 1")

        session.connect()
        session.close()
        session.close()

        with pytest.raises(ConnectorError, match="not connected"):
            session.execute_pushdown("SELECT 1")

    def test_it_logs_the_statement_kind_and_never_the_sql(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure a log line can be shared without leaking SQL or credentials."""
        _read_database(mocker, return_value=_setting("on"))
        session = _postgres_session(_DATABASE_SECRET)

        with caplog.at_level(logging.INFO, logger="veridelta.connectors.database"):
            session.connect()

        assert "settings" in caplog.text
        assert "current_setting" not in caplog.text
        assert _DATABASE_SECRET not in caplog.text
