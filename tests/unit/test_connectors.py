# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for warehouse and lakehouse connector scaffolding."""

import logging
import traceback
from pathlib import Path
from unittest.mock import MagicMock
from urllib.parse import quote

import polars as pl
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from veridelta.config import (
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    IcebergConfig,
    SnowflakeConfig,
)
from veridelta.connectors import (
    DatabaseConnector,
    DatabricksConnector,
    DeltaLakeConnector,
    IcebergConnector,
    PushdownQueryType,
    SnowflakeConnector,
    VerideltaConnector,
)
from veridelta.exceptions import ConfigError, ConnectorError


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


@pytest.mark.unit
@pytest.mark.fast
class TestConnectorConfigValidation:
    """Validate frozen, extra-forbid credential models."""

    def test_it_forbids_unrecognized_fields_on_snowflake_config(self) -> None:
        """Ensure typos in Snowflake settings raise ValidationError."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            SnowflakeConfig(
                account="xy12345",
                user="analyst",
                warehouse="COMPUTE_WH",
                database="ANALYTICS",
                schema_name="PUBLIC",
                table="ANALYTICS.PUBLIC.LEGACY_EVENTS",
                region="us-east-1",  # type: ignore[call-arg]
            )

    def test_it_forbids_unrecognized_fields_on_databricks_config(self) -> None:
        """Ensure typos in Databricks settings raise ValidationError."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            DatabricksConfig(
                server_hostname="adb.azuredatabricks.net",
                http_path="/sql/1.0/warehouses/abc",
                table="main.default.legacy_events",
                cluster_id="ignored",  # type: ignore[call-arg]
            )

    def test_it_forbids_unrecognized_fields_on_delta_and_iceberg_configs(self) -> None:
        """Ensure lakehouse configs reject unknown keys."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            DeltaLakeConfig(table_uri="s3://lake/events", catalog="main")  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            IcebergConfig(table_uri="s3://lake/iceberg/events", catalog="main")  # type: ignore[call-arg]

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


@pytest.mark.unit
@pytest.mark.fast
class TestConnectorInterface:
    """Validate ABC instantiation and warehouse stubs."""

    def test_it_cannot_instantiate_the_abstract_connector(self) -> None:
        """Ensure VerideltaConnector remains an abstract interface."""
        with pytest.raises(TypeError, match="abstract"):
            VerideltaConnector()  # type: ignore[abstract]

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
        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[snowflake\]'"):
            connector.fetch_schema()

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
        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[databricks\]'"):
            connector.fetch_schema()


@pytest.mark.unit
@pytest.mark.fast
class TestLakehouseConnectors:
    """Validate lazy Delta and Iceberg scan wiring without optional extras."""

    def test_it_raises_when_fetching_schema_before_connect(self) -> None:
        """Ensure schema reads require an established lazy-scan handle."""
        delta = DeltaLakeConnector(_delta_config())
        iceberg = IcebergConnector(_iceberg_config())

        with pytest.raises(ConnectorError, match="not connected"):
            delta.fetch_schema()
        with pytest.raises(ConnectorError, match="not connected"):
            delta.lazyframe()
        with pytest.raises(ConnectorError, match="not connected"):
            iceberg.fetch_schema()
        with pytest.raises(ConnectorError, match="not connected"):
            iceberg.lazyframe()

    def test_it_rejects_sql_pushdown_on_lakehouse_connectors(self) -> None:
        """Ensure lakehouse backends do not accept warehouse SQL."""
        delta = DeltaLakeConnector(_delta_config())
        iceberg = IcebergConnector(_iceberg_config())

        with pytest.raises(ConnectorError, match="warehouse-only"):
            delta.execute_pushdown("SELECT * FROM events")
        with pytest.raises(ConnectorError, match="warehouse-only"):
            iceberg.execute_pushdown("SELECT * FROM events")

    def test_it_connects_delta_via_scan_delta_and_returns_schema(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure connect() calls pl.scan_delta and fetch_schema is lazy."""
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
        schema = connector.fetch_schema()

        scan.assert_called_once_with(
            "s3://lake/events",
            storage_options={"AWS_REGION": "us-east-1"},
            version=3,
        )
        assert connector.lazyframe() is lazy
        assert schema.names() == ["id", "amount"]
        assert isinstance(lazy, pl.LazyFrame)

    def test_it_connects_iceberg_via_scan_iceberg_and_returns_schema(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure connect() calls pl.scan_iceberg and fetch_schema is lazy."""
        lazy = _sample_lazy_frame()
        scan = mocker.patch("veridelta.connectors.lakehouse.pl.scan_iceberg", return_value=lazy)
        connector = IcebergConnector(
            IcebergConfig(
                table_uri="s3://lake/iceberg/events",
                storage_options={"AWS_REGION": "us-east-1"},
            )
        )

        connector.connect()
        schema = connector.fetch_schema()

        scan.assert_called_once_with(
            "s3://lake/iceberg/events",
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
        with pytest.raises(ConnectorError, match="not connected"):
            connector.fetch_schema()

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


@pytest.mark.unit
@pytest.mark.fast
class TestConnectorLifecycleDefaults:
    """Validate the lifecycle members every connector inherits from the ABC."""

    def test_it_provides_a_no_op_close_and_a_self_returning_context(self) -> None:
        """Ensure a three-method subclass still gets close() and the context protocol."""

        class _Minimal(VerideltaConnector):
            def connect(self) -> None:
                return None

            def execute_pushdown(
                self, statement: str, query_type: PushdownQueryType = "mismatch"
            ) -> pl.LazyFrame:
                return _sample_lazy_frame()

            def fetch_schema(self) -> pl.Schema:
                return _sample_lazy_frame().collect_schema()

        connector = _Minimal()
        connector.close()
        with connector as managed:
            assert managed is connector
        assert connector.execute_pushdown("SELECT 1").collect().height == 2


_DATABASE_SECRET = "p@ss:w/rd %+&?#"
"""A password holding every character a URI treats specially."""


def _read_database(
    mocker: MockerFixture, *, return_value: object = None, side_effect: object = None
) -> MagicMock:
    """Patch the extra probe and Polars' reader, returning the reader mock."""
    mocker.patch("veridelta.connectors.database.connectorx", object())
    read = MagicMock(return_value=return_value, side_effect=side_effect)
    mocker.patch("veridelta.connectors.database.pl.read_database_uri", read)
    return read


@pytest.mark.unit
@pytest.mark.fast
class TestDatabaseConnector:
    """Validate the connector that reads a database table or query into Polars."""

    def test_it_reads_a_table_through_one_select(self, mocker: MockerFixture) -> None:
        """Ensure a table is read once, with its name quoted for the database."""
        frame = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        read = _read_database(mocker, return_value=frame)
        uri = "postgresql://analyst@db.internal:5432/sales"
        connector = DatabaseConnector(DatabaseConfig(uri=uri, table="public.orders"))

        connector.connect()

        read.assert_called_once_with('SELECT * FROM "public"."orders"', uri)
        assert connector.lazyframe().collect().equals(frame)
        assert connector.fetch_schema() == frame.schema

    def test_it_sends_a_query_verbatim(self, mocker: MockerFixture) -> None:
        """Ensure a configured statement reaches the driver exactly as written."""
        read = _read_database(mocker, return_value=pl.DataFrame({"n": [1]}))
        query = "SELECT id, total FROM orders WHERE region = 'EU'"
        uri = "mysql://analyst@db.internal/sales"

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

        read.assert_called_once_with(
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

    def test_it_refuses_pushdown(self) -> None:
        """Ensure a database source can never be driven as a warehouse."""
        connector = DatabaseConnector(DatabaseConfig(uri="sqlite:///srv/x.db", table="t"))

        with pytest.raises(ConnectorError, match="compared locally"):
            connector.execute_pushdown("SELECT 1")

    def test_it_drops_the_frame_on_close_and_rereads_on_connect(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure close() returns the connector to its unconnected state."""
        read = _read_database(mocker, return_value=pl.DataFrame({"n": [1]}))
        connector = DatabaseConnector(
            DatabaseConfig(uri="postgresql://analyst@db.internal/sales", table="t")
        )
        connector.close()  # before connect: a no-op

        with pytest.raises(ConnectorError, match="not connected"):
            connector.lazyframe()
        with pytest.raises(ConnectorError, match="not connected"):
            connector.fetch_schema()

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
