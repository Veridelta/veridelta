# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the DuckDB and MotherDuck connector, with the driver replaced."""

import logging
import os
import traceback
from pathlib import Path
from unittest.mock import MagicMock, call

import polars as pl
import pytest
from pytest_mock import MockerFixture

from veridelta.connectors.duckdb import DuckDBConnector, DuckDBPushdownSession, sandboxed
from veridelta.connectors.sql import SQLDialect, compile_duckdb_sandbox
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import DuckDBConfig

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]

_SECRET = "md-token-do-not-print"

_FILE = DuckDBConfig(database="warehouse.duckdb", table="main.orders")
_MOTHERDUCK = DuckDBConfig(database="md:sales", table="orders", motherduck_token=_SECRET)


class _Type:
    """Stand in for a DuckDB column type: its id, and the name DuckDB prints."""

    def __init__(self, name: str) -> None:
        self.id = name.lower()
        self._name = name

    def __str__(self) -> str:
        return self._name


@pytest.fixture(autouse=True)
def _clear_token_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset both token variables, so the shell running the tests cannot supply one."""
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    monkeypatch.delenv("motherduck_token", raising=False)


def _driver(
    mocker: MockerFixture, frame: pl.DataFrame | None = None, *, types: list[str] | None = None
) -> MagicMock:
    """Replace the DuckDB module; its connection's statements return `frame`.

    Returns:
        MagicMock: The module mock. `connect.return_value` is the connection.
    """
    result = pl.DataFrame({"id": [1, 2]}) if frame is None else frame
    relation = MagicMock(columns=result.columns, types=[_Type(t) for t in types or []])
    if types is None:
        relation.types = [_Type("INTEGER") for _ in result.columns]
    relation.pl.return_value = result
    module = MagicMock()
    module.connect.return_value.sql.return_value = relation
    mocker.patch("veridelta.connectors.duckdb.duckdb", module)
    return module


class TestDuckDBConnector:
    """Validate the connector that reads a DuckDB file or MotherDuck database into Polars."""

    def test_it_opens_a_file_read_only_and_reads_its_table(self, mocker: MockerFixture) -> None:
        """Ensure a file can never be changed by the read, and the table name is quoted."""
        frame = pl.DataFrame({"id": [1, 2], "status": ["open", "closed"]})
        driver = _driver(mocker, frame)

        with DuckDBConnector(_FILE) as connector:
            connector.connect()
            assert connector.lazyframe().collect().equals(frame)
            assert connector.lazyframe().collect_schema() == frame.schema

        driver.connect.assert_called_once_with("warehouse.duckdb", read_only=True)
        driver.connect.return_value.sql.assert_called_once_with('SELECT * FROM "main"."orders"')

    def test_it_sends_a_query_verbatim(self, mocker: MockerFixture) -> None:
        """Ensure a configured statement reaches DuckDB as written."""
        driver = _driver(mocker)
        query = "SELECT id FROM read_parquet('orders/*.parquet') WHERE region = 'EU'"

        DuckDBConnector(DuckDBConfig(database="warehouse.duckdb", query=query)).connect()

        driver.connect.return_value.sql.assert_called_once_with(query)

    def test_it_sets_the_time_zone_before_reading(self, mocker: MockerFixture) -> None:
        """Ensure timestamps and date casts read in UTC, whatever the machine's zone."""
        connection = _driver(mocker).connect.return_value

        DuckDBConnector(_FILE).connect()

        assert connection.mock_calls[:2] == [
            call.execute("SET TimeZone = 'UTC'"),
            call.sql('SELECT * FROM "main"."orders"'),
        ]

    def test_it_holds_a_file_to_its_folders_before_reading(self, mocker: MockerFixture) -> None:
        """Ensure a read inside `sandboxed` locks the connection to its folders first."""
        connection = _driver(mocker).connect.return_value

        with sandboxed([Path("/data")]):
            DuckDBConnector(_FILE).connect()
        DuckDBConnector(_FILE).connect()

        held = [call.execute("SET TimeZone = 'UTC'")]
        # Each folder ends in the platform's separator, so `/data` admits no `/data-old`.
        folder = f"{Path('/data')}{os.sep}"
        held += [call.execute(statement) for statement in compile_duckdb_sandbox([folder])]
        assert connection.mock_calls[: len(held) + 1] == [
            *held,
            call.sql('SELECT * FROM "main"."orders"'),
        ]
        # The read outside the block sets the time zone alone.
        assert connection.execute.call_args_list[len(held) :] == [call("SET TimeZone = 'UTC'")]

    def test_it_closes_a_connection_whose_setup_fails(self, mocker: MockerFixture) -> None:
        """Ensure a connection is released when a setting fails, before any read."""
        connection = _driver(mocker).connect.return_value
        connection.execute.side_effect = RuntimeError("Invalid Input Error")

        with pytest.raises(ConnectorError, match="Invalid Input Error"):
            DuckDBConnector(_FILE).connect()

        connection.close.assert_called_once_with()
        connection.sql.assert_not_called()

    def test_it_closes_the_connection_after_reading(self, mocker: MockerFixture) -> None:
        """Ensure the file is released once the rows are in memory, read or not."""
        connection = _driver(mocker).connect.return_value

        DuckDBConnector(_FILE).connect()
        connection.sql.side_effect = RuntimeError("Catalog Error")
        with pytest.raises(ConnectorError):
            DuckDBConnector(_FILE).connect()

        assert connection.close.call_count == 2


class TestMotherDuckToken:
    """Validate how a MotherDuck connection gets its token, and keeps it out of output."""

    def test_it_passes_the_token_in_the_connection_config(self, mocker: MockerFixture) -> None:
        """Ensure the token reaches DuckDB as a setting, never inside a connection string."""
        driver = _driver(mocker)

        DuckDBConnector(_MOTHERDUCK).connect()

        driver.connect.assert_called_once_with("md:sales", config={"motherduck_token": _SECRET})

    def test_it_leaves_motherduck_out_of_the_folders(self, mocker: MockerFixture) -> None:
        """Ensure `sandboxed` sets nothing on MotherDuck, whose extension needs the network."""
        connection = _driver(mocker).connect.return_value

        with sandboxed([Path("/data")]):
            DuckDBConnector(_MOTHERDUCK).connect()

        assert connection.execute.call_args_list == [call("SET TimeZone = 'UTC'")]

    @pytest.mark.parametrize("variable", ["MOTHERDUCK_TOKEN", "motherduck_token"])
    def test_it_reads_the_token_from_the_environment(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, variable: str
    ) -> None:
        """Ensure either spelling MotherDuck documents supplies the token.

        Windows reads environment names without regard to case, so this checks
        that the token is found, not which name found it.
        """
        driver = _driver(mocker)
        monkeypatch.setenv(variable, "from-the-environment")

        DuckDBConnector(DuckDBConfig(database="md:sales", table="orders")).connect()

        driver.connect.assert_called_once_with(
            "md:sales", config={"motherduck_token": "from-the-environment"}
        )

    def test_it_prefers_the_configured_token(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure the field wins over the environment."""
        driver = _driver(mocker)
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "from-the-environment")

        DuckDBConnector(_MOTHERDUCK).connect()

        driver.connect.assert_called_once_with("md:sales", config={"motherduck_token": _SECRET})

    def test_it_refuses_motherduck_without_a_token(self, mocker: MockerFixture) -> None:
        """Ensure no connection starts the browser sign-in, which a CI job cannot finish."""
        driver = _driver(mocker)

        with pytest.raises(ConnectorError, match="needs a token"):
            DuckDBConnector(DuckDBConfig(database="md:sales", table="orders")).connect()

        driver.connect.assert_not_called()

    @pytest.mark.parametrize("from_environment", [False, True])
    def test_it_keeps_the_token_out_of_failures(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, from_environment: bool
    ) -> None:
        """Ensure a driver error that echoes the token is reported without it, or its cause."""
        driver = _driver(mocker)
        driver.connect.side_effect = RuntimeError(f"authentication failed for token {_SECRET}")
        config = _MOTHERDUCK
        if from_environment:
            monkeypatch.setenv("MOTHERDUCK_TOKEN", _SECRET)
            config = DuckDBConfig(database="md:sales", table="orders")

        with pytest.raises(ConnectorError) as info:
            DuckDBConnector(config).connect()

        message = str(info.value)
        assert message.startswith("DuckDB read of table 'orders' from 'md:sales' failed:")
        assert "authentication failed for token ***" in message
        assert info.value.__cause__ is None
        assert info.value.__suppress_context__ is True
        assert _SECRET not in "".join(traceback.format_exception(info.value))

    def test_it_logs_reads_without_the_token_or_the_query(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure log lines name the database and the source, never the token or a statement."""
        driver = _driver(mocker, pl.DataFrame({"n": [1, 2, 3]}))
        config = DuckDBConfig(
            database="md:sales", query="SELECT hidden FROM t", motherduck_token=_SECRET
        )

        with caplog.at_level(logging.INFO, logger="veridelta.connectors.duckdb"):
            DuckDBConnector(config).connect()
            driver.connect.side_effect = RuntimeError(f"down {_SECRET}")
            with pytest.raises(ConnectorError):
                DuckDBConnector(config).connect()

        assert "Read 3 rows of the configured query from md:sales" in caplog.text
        assert "DuckDB read of the configured query from md:sales failed" in caplog.text
        assert _SECRET not in caplog.text
        assert "hidden" not in caplog.text


class TestDuckDBReadErrors:
    """Validate the errors a DuckDB read raises before or instead of returning rows."""

    def test_it_explains_a_missing_extra_before_connecting(self, mocker: MockerFixture) -> None:
        """Ensure a missing `duckdb` package names the extra to install."""
        mocker.patch("veridelta.connectors.duckdb.duckdb", None)

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[duckdb\]'"):
            DuckDBConnector(_FILE).connect()

    def test_it_explains_missing_pyarrow_as_the_extra(self, mocker: MockerFixture) -> None:
        """Ensure the conversion's missing dependency names the same extra."""
        driver = _driver(mocker)
        driver.connect.return_value.sql.return_value.pl.side_effect = ModuleNotFoundError(
            "No module named 'pyarrow'"
        )

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[duckdb\]'"):
            DuckDBConnector(_FILE).connect()

    def test_it_refuses_a_query_that_returns_no_result(self, mocker: MockerFixture) -> None:
        """Ensure a statement such as SET, which DuckDB runs and answers with nothing, fails."""
        driver = _driver(mocker)
        driver.connect.return_value.sql.return_value = None

        with pytest.raises(ConnectorError, match="returned no rows to compare"):
            DuckDBConnector(DuckDBConfig(database="warehouse.duckdb", query="SET x = 1")).connect()

    def test_it_names_a_column_polars_cannot_read(self, mocker: MockerFixture) -> None:
        """Ensure an INTERVAL column fails with its name before the conversion can panic."""
        driver = _driver(
            mocker, pl.DataFrame({"id": [1], "span": [None]}), types=["INTEGER", "INTERVAL"]
        )

        with pytest.raises(
            ConnectorError, match=r"Column 'span' of table 'main\.orders' holds INTERVAL"
        ):
            DuckDBConnector(_FILE).connect()

        driver.connect.return_value.sql.return_value.pl.assert_not_called()


class TestDuckDBSchemaProbe:
    """Validate `DuckDBConnector(..., probe=True)`, which reads a table's columns only."""

    def test_it_reads_no_rows(self, mocker: MockerFixture) -> None:
        """Ensure the probe sends the zero-row statement and keeps the schema."""
        frame = pl.DataFrame(schema={"id": pl.Int64, "status": pl.String})
        driver = _driver(mocker, frame)

        with DuckDBConnector(_FILE, probe=True) as connector:
            connector.connect()
            schema = connector.lazyframe().collect_schema()

        driver.connect.return_value.sql.assert_called_once_with(
            'SELECT * FROM "main"."orders" WHERE 1 = 0'
        )
        assert schema == frame.schema

    def test_it_refuses_to_probe_a_query(self, mocker: MockerFixture) -> None:
        """Ensure a query is never run whole when only its columns were asked for."""
        driver = _driver(mocker)
        config = DuckDBConfig(database="warehouse.duckdb", query="SELECT * FROM t")

        with pytest.raises(ConfigError, match="probe reads a 'table'"):
            DuckDBConnector(config, probe=True).connect()

        driver.connect.assert_not_called()


class TestDuckDBConnectorLifecycle:
    """Validate the connector's contract before, after, and instead of a read."""

    def test_it_refuses_rows_until_connected_and_after_closing(self, mocker: MockerFixture) -> None:
        """Ensure a frame is only handed out between `connect()` and `close()`."""
        _driver(mocker)
        connector = DuckDBConnector(_FILE)

        with pytest.raises(ConnectorError, match="not connected"):
            connector.lazyframe()
        connector.connect()
        connector.close()
        connector.close()
        with pytest.raises(ConnectorError, match="not connected"):
            connector.lazyframe()


_PUSHDOWN = DuckDBConfig(database="warehouse.duckdb", table="src", pushdown=True)


class TestDuckDBPushdownSession:
    """Validate the session that runs compiled comparison SQL inside DuckDB."""

    def test_it_runs_every_statement_on_one_connection(self, mocker: MockerFixture) -> None:
        """Ensure the database opens once, read-only and in UTC, and closes once."""
        frame = pl.DataFrame({"id": [1, 2]})
        driver = _driver(mocker, frame)
        connection = driver.connect.return_value
        session = DuckDBPushdownSession(_PUSHDOWN)

        session.connect()
        first = session.execute_pushdown("SELECT 1", query_type="count")
        session.execute_pushdown("SELECT 2")
        session.close()
        session.close()

        driver.connect.assert_called_once_with("warehouse.duckdb", read_only=True)
        assert connection.mock_calls[0] == call.execute("SET TimeZone = 'UTC'")
        assert connection.sql.call_args_list == [call("SELECT 1"), call("SELECT 2")]
        assert first.collect().equals(frame)
        connection.close.assert_called_once_with()

    def test_it_closes_a_connection_whose_setup_fails(self, mocker: MockerFixture) -> None:
        """Ensure a session whose settings fail holds no connection open."""
        connection = _driver(mocker).connect.return_value
        connection.execute.side_effect = RuntimeError("Invalid Input Error")
        session = DuckDBPushdownSession(_PUSHDOWN)

        with pytest.raises(ConnectorError, match="Invalid Input Error"):
            session.connect()

        connection.close.assert_called_once_with()
        with pytest.raises(ConnectorError, match="not connected"):
            session.execute_pushdown("SELECT 1")

    def test_it_compiles_for_the_duckdb_dialect(self) -> None:
        """Ensure the engine asks this session for DuckDB SQL."""
        assert DuckDBPushdownSession(_PUSHDOWN).compiler.dialect is SQLDialect.DUCKDB

    def test_it_refuses_work_until_connected_and_after_closing(self, mocker: MockerFixture) -> None:
        """Ensure a statement never runs without an open connection."""
        _driver(mocker)
        session = DuckDBPushdownSession(_PUSHDOWN)

        with pytest.raises(ConnectorError, match="not connected"):
            session.execute_pushdown("SELECT 1")
        session.connect()
        session.close()
        with pytest.raises(ConnectorError, match="not connected"):
            session.execute_pushdown("SELECT 1")

    def test_it_opens_motherduck_with_its_token(self, mocker: MockerFixture) -> None:
        """Ensure a MotherDuck pair connects as a MotherDuck read does."""
        driver = _driver(mocker)
        config = DuckDBConfig(
            database="md:sales", table="src", motherduck_token=_SECRET, pushdown=True
        )

        DuckDBPushdownSession(config).connect()

        driver.connect.assert_called_once_with("md:sales", config={"motherduck_token": _SECRET})

    def test_it_refuses_motherduck_without_a_token(self, mocker: MockerFixture) -> None:
        """Ensure no session starts the browser sign-in a CI job cannot finish."""
        driver = _driver(mocker)
        config = DuckDBConfig(database="md:sales", table="src", pushdown=True)

        with pytest.raises(ConnectorError, match="needs a token"):
            DuckDBPushdownSession(config).connect()

        driver.connect.assert_not_called()

    def test_it_keeps_the_token_out_of_failures(self, mocker: MockerFixture) -> None:
        """Ensure a failed connection or statement is reported without the token or its cause."""
        driver = _driver(mocker)
        config = DuckDBConfig(
            database="md:sales", table="src", motherduck_token=_SECRET, pushdown=True
        )
        driver.connect.side_effect = RuntimeError(f"refused token {_SECRET}")

        with pytest.raises(ConnectorError) as connecting:
            DuckDBPushdownSession(config).connect()
        driver.connect.side_effect = None
        driver.connect.return_value.sql.side_effect = RuntimeError(f"expired token {_SECRET}")
        session = DuckDBPushdownSession(config)
        session.connect()
        with pytest.raises(ConnectorError) as running:
            session.execute_pushdown("SELECT 1", query_type="mismatch")

        assert str(connecting.value) == (
            "DuckDB connection to 'md:sales' failed: refused token ***"
        )
        assert str(running.value) == (
            "DuckDB mismatch statement on 'md:sales' failed: expired token ***"
        )
        for error in (connecting.value, running.value):
            assert error.__cause__ is None
            assert _SECRET not in "".join(traceback.format_exception(error))

    def test_it_logs_statements_by_kind_without_their_sql(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure each statement is logged by its kind, never by its text."""
        driver = _driver(mocker, pl.DataFrame({"n": [1, 2, 3]}))
        session = DuckDBPushdownSession(_PUSHDOWN)

        with caplog.at_level(logging.INFO, logger="veridelta.connectors.duckdb"):
            session.connect()
            session.execute_pushdown("SELECT secret_column FROM src", query_type="count")
            driver.connect.return_value.sql.side_effect = RuntimeError("down")
            with pytest.raises(ConnectorError):
                session.execute_pushdown("SELECT secret_column FROM src", query_type="added")
            session.close()

        assert "Connected to DuckDB database warehouse.duckdb" in caplog.text
        assert "Ran DuckDB count statement on warehouse.duckdb" in caplog.text
        assert "DuckDB added statement on warehouse.duckdb failed" in caplog.text
        assert "Closed DuckDB database warehouse.duckdb" in caplog.text
        assert "secret_column" not in caplog.text

    def test_it_explains_missing_extras(self, mocker: MockerFixture) -> None:
        """Ensure a missing `duckdb` or `pyarrow` names the extra to install."""
        mocker.patch("veridelta.connectors.duckdb.duckdb", None)
        with pytest.raises(ConnectorError, match=r"veridelta\[duckdb\]"):
            DuckDBPushdownSession(_PUSHDOWN).connect()

        driver = _driver(mocker)
        driver.connect.return_value.sql.return_value.pl.side_effect = ModuleNotFoundError("pyarrow")
        session = DuckDBPushdownSession(_PUSHDOWN)
        session.connect()
        with pytest.raises(ConnectorError, match=r"veridelta\[duckdb\]"):
            session.execute_pushdown("SELECT 1")

    def test_it_names_a_result_column_polars_cannot_read(self, mocker: MockerFixture) -> None:
        """Ensure a sample holding an INTERVAL fails by name instead of panicking."""
        _driver(mocker, pl.DataFrame({"span_source": [None]}), types=["INTERVAL"])
        session = DuckDBPushdownSession(_PUSHDOWN)
        session.connect()

        with pytest.raises(
            ConnectorError, match="Column 'span_source' of the samples statement holds INTERVAL"
        ):
            session.execute_pushdown("SELECT sample", query_type="samples")
