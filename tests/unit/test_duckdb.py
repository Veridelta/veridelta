# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the DuckDB and MotherDuck connector, with the driver replaced."""

import errno
import logging
import os
import sys
import tempfile
import traceback
from collections.abc import Callable, Generator
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, call

import polars as pl
import pytest
from pytest_mock import MockerFixture

from veridelta.connectors import duckdb as duckdb_connector
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

    Its parser reads every statement as a SELECT, unless a test says otherwise.

    Returns:
        MagicMock: The module mock. `connect.return_value` is the connection.
    """
    result = pl.DataFrame({"id": [1, 2]}) if frame is None else frame
    relation = MagicMock(columns=result.columns, types=[_Type(t) for t in types or []])
    if types is None:
        relation.types = [_Type("INTEGER") for _ in result.columns]
    relation.pl.return_value = result
    module = MagicMock()
    module.extract_statements.return_value = [
        MagicMock(
            type=module.StatementType.SELECT,
            expected_result_type=[module.ExpectedResultType.QUERY_RESULT],
        )
    ]
    module.connect.return_value.sql.return_value = relation
    mocker.patch("veridelta.connectors.duckdb.duckdb", module)
    return module


def _settings(spill: str) -> dict[str, object]:
    """Return the settings a file opens with: no extension, and the folder it spills into."""
    return {
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
        "temp_directory": spill,
    }


def _spills(mocker: MockerFixture) -> MagicMock:
    """Watch the folders a connection creates to spill into.

    Returns:
        MagicMock: The spy on `tempfile.mkdtemp`, whose `spy_return` is the
            last folder created.
    """
    return mocker.spy(tempfile, "mkdtemp")


_Opening = Callable[[Path], MagicMock]
"""Replace the DuckDB module with one whose `connect` holds a new, empty file open."""


@pytest.fixture
def opening(mocker: MockerFixture) -> Generator[_Opening]:
    """Return a function that replaces the DuckDB module, and close what its `connect` opened.

    DuckDB holds the database file open while its connection is, and inside
    the sandbox the connector reads that from the process's descriptors.
    """
    descriptors: list[int] = []

    def replace(file: Path) -> MagicMock:
        file.write_bytes(b"")
        module = _driver(mocker)

        def connect(*_: object, **__: object) -> MagicMock:
            descriptors.append(os.open(file, os.O_RDONLY))
            return module.connect.return_value

        module.connect.side_effect = connect
        return module

    yield replace
    for descriptor in descriptors:
        os.close(descriptor)


class TestDuckDBConnector:
    """Validate the connector that reads a DuckDB file or MotherDuck database into Polars."""

    def test_it_opens_a_file_read_only_and_reads_its_table(self, mocker: MockerFixture) -> None:
        """Ensure a file can never be changed by the read, and the table name is quoted."""
        frame = pl.DataFrame({"id": [1, 2], "status": ["open", "closed"]})
        driver = _driver(mocker, frame)
        created = _spills(mocker)

        with DuckDBConnector(_FILE) as connector:
            connector.connect()
            assert connector.lazyframe().collect().equals(frame)
            assert connector.lazyframe().collect_schema() == frame.schema

        # As a DuckDB database, so neither the text nor the file's header picks an extension.
        driver.connect.assert_called_once_with(
            "duckdb:warehouse.duckdb", read_only=True, config=_settings(created.spy_return)
        )
        driver.connect.return_value.sql.assert_called_once_with('SELECT * FROM "main"."orders"')

    def test_it_spills_into_a_folder_of_its_own_and_removes_it(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure a large query spills into a folder this read creates, never beside the file.

        A file from someone else can arrive with a folder beside it that holds
        a link under the name of a spill file, which DuckDB would write through.
        """
        driver = _driver(mocker)
        created = _spills(mocker)
        config = _file(tmp_path)
        Path(f"{config.database}.tmp").mkdir()
        spilled: list[bool] = []

        def connect(*_: object, **kwargs: object) -> MagicMock:
            spilled.append(
                Path(cast("dict[str, str]", kwargs["config"])["temp_directory"]).is_dir()
            )
            return driver.connect.return_value

        driver.connect.side_effect = connect

        DuckDBConnector(config).connect()

        folder = Path(created.spy_return)
        assert spilled == [True]
        assert not folder.is_relative_to(tmp_path)
        assert not folder.exists()

    def test_it_removes_its_spill_folder_when_duckdb_cannot_open_the_file(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a read that fails as DuckDB opens the file leaves no folder behind."""
        driver = _driver(mocker)
        driver.connect.side_effect = RuntimeError("IO Error")
        created = _spills(mocker)

        with pytest.raises(ConnectorError, match="IO Error"):
            DuckDBConnector(_FILE).connect()

        assert not Path(created.spy_return).exists()

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

        assert connection.mock_calls[0] == call.execute("SET TimeZone = 'UTC'")
        assert call.sql('SELECT * FROM "main"."orders"') in connection.mock_calls

    def test_it_holds_a_file_to_its_folders_before_reading(
        self, opening: _Opening, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Ensure a read inside `sandboxed` locks the connection to its folders first."""
        connection = opening(tmp_path / "warehouse.duckdb").connect.return_value
        monkeypatch.chdir(tmp_path)

        with sandboxed([Path("/data")]):
            DuckDBConnector(_FILE).connect()

        held = [call.execute("SET TimeZone = 'UTC'")]
        # Each folder ends in the platform's separator, so `/data` admits no `/data-old`.
        folder = f"{Path('/data')}{os.sep}"
        # On Windows the sandbox also turns off DuckDB's local file system.
        other_files = duckdb_connector._FOLDERS_HOLD
        sandbox = compile_duckdb_sandbox([folder], other_files=other_files)
        held += [call.execute(statement) for statement in sandbox]
        assert connection.mock_calls[: len(held) + 1] == [
            *held,
            call.sql('SELECT * FROM "main"."orders"'),
        ]

    @pytest.mark.parametrize("folders_hold", [True, False])
    def test_it_opens_no_other_file_when_sandboxed_where_duckdbs_folders_do_not_hold(
        self,
        opening: _Opening,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        folders_hold: bool,
    ) -> None:
        """Ensure a sandboxed file opens no other file on a system such as Windows.

        There DuckDB's folder check lets some paths out, and the server cannot
        see the paths a query or a view names.
        """
        monkeypatch.setattr(duckdb_connector, "_FOLDERS_HOLD", folders_hold)
        connection = opening(tmp_path / "warehouse.duckdb").connect.return_value
        monkeypatch.chdir(tmp_path)

        with sandboxed([Path("/data")]):
            DuckDBConnector(_FILE).connect()

        sandbox = compile_duckdb_sandbox([f"{Path('/data')}{os.sep}"], other_files=folders_hold)
        held = [call.execute("SET TimeZone = 'UTC'"), *map(call.execute, sandbox)]
        assert connection.mock_calls[: len(held)] == held

    def test_it_holds_a_file_outside_the_sandbox_to_its_folder_wherever_it_runs(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Ensure a file outside the MCP server still reads the files beside it, on Windows too."""
        monkeypatch.setattr(duckdb_connector, "_FOLDERS_HOLD", False)
        monkeypatch.chdir(tmp_path)
        connection = _driver(mocker).connect.return_value

        DuckDBConnector(_FILE).connect()

        sandbox = compile_duckdb_sandbox([f"{tmp_path.resolve()}{os.sep}"])
        held = [call.execute("SET TimeZone = 'UTC'"), *map(call.execute, sandbox)]
        assert connection.mock_calls[: len(held)] == held

    @pytest.mark.parametrize(
        ("database", "folder"),
        [
            pytest.param("warehouse.duckdb", ".", id="relative"),
            pytest.param("{root}/data/warehouse.duckdb", "data", id="absolute"),
            pytest.param("~/warehouse.duckdb", "home", id="home"),
        ],
    )
    def test_it_holds_a_file_to_its_own_folder_outside_the_sandbox(
        self,
        mocker: MockerFixture,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        database: str,
        folder: str,
    ) -> None:
        """Ensure a view in a file someone else wrote reads nothing outside the file's folder."""
        connection = _driver(mocker).connect.return_value
        monkeypatch.chdir(tmp_path)
        # DuckDB expands `~`, so the folder must too: POSIX reads HOME, Windows USERPROFILE.
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setenv("USERPROFILE", str(tmp_path / "home"))
        config = DuckDBConfig(database=database.format(root=tmp_path), table="orders")

        DuckDBConnector(config).connect()

        sandbox = compile_duckdb_sandbox([f"{(tmp_path / folder).resolve()}{os.sep}"])
        held = [
            call.execute("SET TimeZone = 'UTC'"),
            *[call.execute(statement) for statement in sandbox],
            call.sql('SELECT * FROM "orders"'),
        ]
        assert connection.mock_calls[: len(held)] == held

    def test_it_expands_the_home_folder_before_duckdb_does(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Ensure DuckDB gets `~` expanded as the MCP server's check expands it.

        DuckDB reads `~` as HOME alone, and as the root folder when HOME is
        unset, where Python asks the system for the home folder.
        """
        driver = _driver(mocker)
        home = tmp_path / "home"
        # POSIX reads HOME, Windows USERPROFILE.
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("USERPROFILE", str(home))

        DuckDBConnector(DuckDBConfig(database="~/warehouse.duckdb", table="t")).connect()

        assert driver.connect.call_args.args == (f"duckdb:{home}/warehouse.duckdb",)

    @pytest.mark.parametrize(
        ("database", "home"),
        [
            pytest.param("//srv/warehouse.duckdb", None, id="two-slashes"),
            pytest.param("~//srv/warehouse.duckdb", "/", id="home-of-the-root-folder"),
        ],
    )
    def test_it_refuses_a_path_duckdb_reads_as_no_type(
        self,
        mocker: MockerFixture,
        monkeypatch: pytest.MonkeyPatch,
        database: str,
        home: str | None,
    ) -> None:
        """Ensure no path reaches DuckDB as `duckdb://`, from which it strips no type.

        DuckDB would open the relative file `duckdb:/srv/warehouse.duckdb`
        instead, and by its first bytes, through the SQLite extension if
        that is what the file holds.
        """
        driver = _driver(mocker)
        if home is not None:
            # POSIX reads HOME, Windows USERPROFILE.
            monkeypatch.setenv("HOME", home)
            monkeypatch.setenv("USERPROFILE", home)
        connector = DuckDBConnector(DuckDBConfig(database=database, table="t"))
        session = DuckDBPushdownSession(DuckDBConfig(database=database, table="t", pushdown=True))

        with pytest.raises(ConnectorError, match="starts with //, once ~ is expanded") as refused:
            connector.connect()
        with pytest.raises(ConnectorError, match="starts with //, once ~ is expanded"):
            session.connect()

        assert str(refused.value).startswith("The DuckDB 'database' starts with //")
        driver.connect.assert_not_called()

    def test_it_refuses_a_home_path_with_no_home_folder(self, mocker: MockerFixture) -> None:
        """Ensure DuckDB is never handed `~`, which it expands where Python found no home.

        Python leaves the text as written when the system names no home
        folder, so the MCP server checks a folder `~` in the working
        directory, while DuckDB would read `~` as HOME alone.
        """
        driver = _driver(mocker)
        mocker.patch.object(os.path, "expanduser", side_effect=lambda path: path)

        with pytest.raises(ConnectorError, match="names no home folder"):
            DuckDBConnector(DuckDBConfig(database="~/warehouse.duckdb", table="t")).connect()

        driver.connect.assert_not_called()

    def test_it_closes_a_connection_whose_setup_fails(self, mocker: MockerFixture) -> None:
        """Ensure a connection, and its spill folder, are released when a setting fails."""
        connection = _driver(mocker).connect.return_value
        connection.execute.side_effect = RuntimeError("Invalid Input Error")
        created = _spills(mocker)

        with pytest.raises(ConnectorError, match="Invalid Input Error"):
            DuckDBConnector(_FILE).connect()

        connection.close.assert_called_once_with()
        connection.sql.assert_not_called()
        assert not Path(created.spy_return).exists()

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

    def test_it_refuses_a_statement_that_parses_as_returning_nothing(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a SET fails before it runs, where DuckDB 2.0 would answer it with a placeholder.

        DuckDB 2.0 answers SET with a one-column `Success` relation rather than
        none, so the read asks the parser what the last statement returns.
        """
        driver = _driver(mocker)
        driver.extract_statements.return_value = [
            MagicMock(expected_result_type=[driver.ExpectedResultType.NOTHING])
        ]

        with pytest.raises(ConnectorError, match="returned no rows to compare"):
            DuckDBConnector(DuckDBConfig(database="warehouse.duckdb", query="SET x = 1")).connect()

        driver.connect.return_value.sql.assert_not_called()

    def test_it_refuses_a_write_before_it_runs(self, mocker: MockerFixture) -> None:
        """Ensure an INSERT is refused by its kind and never reaches a read-write connection.

        The parser expects rows from an INSERT, since it may carry RETURNING, so
        only the statement's kind tells a write from a read.
        """
        driver = _driver(mocker)
        insert_kind = MagicMock()
        insert_kind.name = "INSERT"
        driver.extract_statements.return_value = [
            MagicMock(
                type=insert_kind,
                expected_result_type=[
                    driver.ExpectedResultType.CHANGED_ROWS,
                    driver.ExpectedResultType.QUERY_RESULT,
                ],
            )
        ]

        with pytest.raises(ConnectorError, match="ends with an INSERT statement"):
            DuckDBConnector(
                DuckDBConfig(
                    database="md:warehouse",
                    query="INSERT INTO t VALUES (1)",
                    motherduck_token=_SECRET,
                ),
            ).connect()

        driver.connect.return_value.sql.assert_not_called()

    def test_it_refuses_a_query_that_parses_as_no_statement(self, mocker: MockerFixture) -> None:
        """Ensure a query of only a comment, which DuckDB answers with no relation, fails."""
        driver = _driver(mocker)
        driver.extract_statements.return_value = []
        driver.connect.return_value.sql.return_value = None

        with pytest.raises(ConnectorError, match="returned no rows to compare"):
            DuckDBConnector(
                DuckDBConfig(database="warehouse.duckdb", query="-- no statement")
            ).connect()

    def test_it_reads_a_statement_that_parses_as_returning_rows(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a SELECT, whose last statement returns a query result, is read."""
        frame = pl.DataFrame({"id": [1, 2]})
        driver = _driver(mocker, frame)
        driver.extract_statements.return_value = [
            MagicMock(expected_result_type=[driver.ExpectedResultType.NOTHING]),
            MagicMock(
                type=driver.StatementType.SELECT,
                expected_result_type=[driver.ExpectedResultType.QUERY_RESULT],
            ),
        ]

        with DuckDBConnector(
            DuckDBConfig(database="warehouse.duckdb", query="SET x = 1; SELECT 1 AS id")
        ) as connector:
            connector.connect()
            assert connector.lazyframe().collect().equals(frame)

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


_LINKS = pytest.mark.skipif(
    sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
)

_BESIDE = [
    pytest.param(".wal", "WAL", id="wal"),
    pytest.param(".wal.checkpoint", "checkpoint WAL", id="checkpoint"),
    pytest.param(".wal.recovery", "recovery WAL", id="recovery"),
]
"""Each log DuckDB names beside a file, with the name a refusal gives it."""

_NO_QUESTION_MARK = pytest.mark.skipif(
    sys.platform == "win32", reason="Windows allows no `?` in a file or folder name."
)

_LISTED = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows lists no open descriptors, so only the path is looked up again.",
)

_LINKED = "is a link, lies in a folder reached through one, or is not a file"
_CHANGED = "or the file or a log beside it changed while DuckDB opened it"


def _file(tmp_path: Path) -> DuckDBConfig:
    """Return a side that reads an empty file at `data/warehouse.duckdb`, by its resolved path."""
    (tmp_path / "data").mkdir(parents=True)
    file = tmp_path / "data" / "warehouse.duckdb"
    file.write_bytes(b"")
    return DuckDBConfig(database=os.path.realpath(file), table="orders")


class TestDuckDBFilesBeside:
    """Validate the logs DuckDB names beside a file, outside the sandbox."""

    @_LINKS
    @pytest.mark.parametrize(("suffix", "name"), _BESIDE)
    def test_it_refuses_a_link_out_of_the_files_folder(
        self,
        mocker: MockerFixture,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        suffix: str,
        name: str,
    ) -> None:
        """Ensure DuckDB replays no log from outside the file's folder.

        A log replays before any setting applies.
        """
        driver = _driver(mocker)
        config = _file(tmp_path)
        elsewhere = tmp_path_factory.mktemp("elsewhere")
        Path(f"{config.database}{suffix}").symlink_to(elsewhere, target_is_directory=True)

        with pytest.raises(ConnectorError, match=f"file's {name} '.+' lies outside the folder"):
            DuckDBConnector(config).connect()

        driver.connect.assert_not_called()

    @_LINKS
    def test_it_follows_a_link_that_stays_in_the_files_folder(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure a log that is a link to a file beside the database still replays."""
        driver = _driver(mocker)
        config = _file(tmp_path)
        (tmp_path / "data" / "old.wal").write_bytes(b"")
        Path(f"{config.database}.wal").symlink_to(tmp_path / "data" / "old.wal")

        DuckDBConnector(config).connect()

        driver.connect.assert_called_once()

    @_NO_QUESTION_MARK
    def test_it_reads_a_file_in_a_folder_whose_name_holds_a_question_mark(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure a log path that names nothing is no refusal, though `?` puts it in another folder.

        DuckDB adds `.wal` before the first `?`, so the log of `q?a/x` is
        `q.wal?a/x`.
        """
        driver = _driver(mocker)
        config = _file(tmp_path / "q?a")

        DuckDBConnector(config).connect()

        driver.connect.assert_called_once()

    @_NO_QUESTION_MARK
    def test_it_refuses_a_log_that_a_question_mark_puts_in_another_folder(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure a log DuckDB would replay from a folder beside the file's is refused, link or not."""
        driver = _driver(mocker)
        config = _file(tmp_path / "q?a")
        log = tmp_path / "q.wal?a" / "data" / "warehouse.duckdb"
        log.parent.mkdir(parents=True)
        log.write_bytes(b"")

        with pytest.raises(ConnectorError, match=r"file's WAL '.+' lies outside the folder"):
            DuckDBConnector(config).connect()

        driver.connect.assert_not_called()


class TestDuckDBSandboxedOpen:
    """Validate that inside the sandbox, DuckDB opens the file its path named, through no link."""

    def test_it_spills_into_a_folder_the_block_creates(
        self, opening: _Opening, tmp_path: Path
    ) -> None:
        """Ensure a large query spills into a folder of the block's, which it removes at its end.

        DuckDB would otherwise spill beside the database, where an agent can
        put a link that leads the writes and deletes elsewhere.
        """
        config = _file(tmp_path)
        driver = opening(Path(config.database))

        with sandboxed([tmp_path]):
            DuckDBConnector(config).connect()
            temporary = Path(driver.connect.call_args.kwargs["config"]["temp_directory"])
            assert temporary.is_dir()

        assert not temporary.is_relative_to(tmp_path)
        assert not temporary.exists()

    def test_it_refuses_a_missing_file(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Ensure a file that is gone is refused before DuckDB could create it elsewhere."""
        driver = _driver(mocker)

        with sandboxed([tmp_path]), pytest.raises(ConnectorError, match="does not exist"):
            DuckDBConnector(DuckDBConfig(database=str(tmp_path / "x.duckdb"), table="t")).connect()

        driver.connect.assert_not_called()

    @pytest.mark.parametrize(
        "link",
        [
            pytest.param("folder", id="folder", marks=_LINKS),
            pytest.param("file", id="file", marks=_LINKS),
            pytest.param("log", id="log", marks=_LINKS),
            pytest.param("log folder", id="log-not-a-file"),
        ],
    )
    @pytest.mark.parametrize("windows", [False, True], ids=["by-folder", "by-resolving"])
    def test_it_refuses_a_path_through_a_link(
        self,
        mocker: MockerFixture,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        link: str,
        windows: bool,
    ) -> None:
        """Ensure the file and each log open only by a path with no link, and only as a file.

        The server resolves the path before it checks it, so a link on it now
        was put there since. Where the system looks no name up in an open
        folder, as on Windows, the path must resolve to itself instead.
        """
        if windows:
            monkeypatch.setattr(os, "supports_dir_fd", set())
        driver = _driver(mocker)
        real = _file(tmp_path / "real")
        database = tmp_path / "data" / "warehouse.duckdb"
        if link == "folder":
            (tmp_path / "data").symlink_to(tmp_path / "real" / "data", target_is_directory=True)
        else:
            (tmp_path / "data").mkdir()
        if link == "file":
            database.symlink_to(real.database)
        elif link != "folder":
            database.write_bytes(b"")
        if link == "log":
            Path(f"{database}.wal").symlink_to(real.database)
        if link == "log folder":
            Path(f"{database}.wal").mkdir()

        with sandboxed([tmp_path]), pytest.raises(ConnectorError, match=_LINKED):
            DuckDBConnector(DuckDBConfig(database=str(database), table="t")).connect()

        driver.connect.assert_not_called()

    def test_it_creates_a_spill_folder_once_and_only_for_a_file(
        self, opening: _Opening, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure a block that opens no file creates no folder, and one that opens two shares one."""
        created = mocker.spy(tempfile, "mkdtemp")
        with sandboxed([tmp_path]):
            pass
        created.assert_not_called()
        config = _file(tmp_path)
        driver = opening(Path(config.database))

        with sandboxed([tmp_path]):
            DuckDBConnector(config).connect()
            DuckDBConnector(config).connect()

        created.assert_called_once()
        folders = {
            each.kwargs["config"]["temp_directory"] for each in driver.connect.call_args_list
        }
        assert folders == {created.spy_return}

    def test_it_refuses_an_open_that_waits_too_long_for_another(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Ensure a file that never finishes opening, such as a pipe, blocks no other read for good."""
        monkeypatch.setattr(duckdb_connector, "_OPEN_WAIT", 0.01)
        driver = _driver(mocker)
        opening = duckdb_connector._OPENING  # pyright: ignore[reportPrivateUsage] - the lock another open holds

        with (
            opening,
            sandboxed([tmp_path]),
            pytest.raises(ConnectorError, match="another DuckDB file was still opening"),
        ):
            DuckDBConnector(_file(tmp_path)).connect()

        driver.connect.assert_not_called()

    def test_it_opens_one_file_at_a_time(self, opening: _Opening, tmp_path: Path) -> None:
        """Ensure no other read's DuckDB connection opens a descriptor while one is confirmed."""
        config = _file(tmp_path)
        driver = opening(Path(config.database))
        open_the_file = driver.connect.side_effect
        held: list[bool] = []

        def connect(*args: object, **kwargs: object) -> MagicMock:
            held.append(duckdb_connector._OPENING.locked())  # pyright: ignore[reportPrivateUsage] - the lock is what keeps reads apart
            return open_the_file(*args, **kwargs)

        driver.connect.side_effect = connect

        with sandboxed([tmp_path]):
            DuckDBConnector(config).connect()

        assert held == [True]
        assert not duckdb_connector._OPENING.locked()  # pyright: ignore[reportPrivateUsage] - the lock is what keeps reads apart

    @_LISTED
    def test_it_refuses_a_file_duckdb_does_not_hold_open(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure a file DuckDB opened elsewhere, through a link swapped in and out, is refused.

        DuckDB opens the path by its text, after the connector looked it up,
        so only the descriptor DuckDB holds tells which file it opened.
        """
        connection = _driver(mocker).connect.return_value

        with sandboxed([tmp_path]), pytest.raises(ConnectorError, match=_CHANGED):
            DuckDBConnector(_file(tmp_path)).connect()

        connection.close.assert_called_once_with()
        connection.sql.assert_not_called()

    @_LISTED
    def test_it_refuses_a_file_another_read_held_open_before(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure a descriptor of the file that another read already held does not count.

        A tool call that holds the file open, through a hard link, would
        otherwise let another call's DuckDB open a file elsewhere unseen.
        """
        config = _file(tmp_path)
        connection = _driver(mocker).connect.return_value
        descriptor = os.open(config.database, os.O_RDONLY)
        try:
            with sandboxed([tmp_path]), pytest.raises(ConnectorError, match=_CHANGED):
                DuckDBConnector(config).connect()
        finally:
            os.close(descriptor)

        connection.close.assert_called_once_with()

    def test_it_refuses_where_the_descriptors_cannot_be_listed(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Ensure a system that should list its descriptors, and cannot, opens no file."""
        monkeypatch.setattr("veridelta.connectors.duckdb._DESCRIPTORS", str(tmp_path / "none"))
        driver = _driver(mocker)

        with sandboxed([tmp_path]), pytest.raises(ConnectorError, match="could not list the files"):
            DuckDBConnector(_file(tmp_path)).connect()

        driver.connect.assert_not_called()

    def test_it_names_why_a_path_could_not_be_looked_up(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Ensure a folder the server may not open is reported as such, not as a link."""
        denied = PermissionError(errno.EACCES, "Permission denied")
        monkeypatch.setattr(duckdb_connector, "_looked_up", MagicMock(side_effect=denied))
        driver = _driver(mocker)

        with (
            sandboxed([tmp_path]),
            pytest.raises(ConnectorError, match=r"could not be looked up.+: Permission denied"),
        ):
            DuckDBConnector(_file(tmp_path)).connect()

        driver.connect.assert_not_called()

    def test_it_refuses_a_log_that_appears_while_duckdb_opens_the_file(
        self, opening: _Opening, tmp_path: Path
    ) -> None:
        """Ensure a log that was not there when the file was looked up is refused after it."""
        config = _file(tmp_path)
        driver = opening(Path(config.database))
        open_the_file = driver.connect.side_effect

        def connect(*args: object, **kwargs: object) -> MagicMock:
            Path(f"{config.database}.wal").write_bytes(b"")
            return open_the_file(*args, **kwargs)

        driver.connect.side_effect = connect

        with sandboxed([tmp_path]), pytest.raises(ConnectorError, match=_CHANGED):
            DuckDBConnector(config).connect()

        driver.connect.return_value.close.assert_called_once_with()

    def test_it_looks_the_paths_up_again_where_no_descriptor_is_listed(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Ensure a system that lists no descriptors, as Windows, still reads an unchanged file."""
        monkeypatch.setattr("veridelta.connectors.duckdb._DESCRIPTORS", None)
        connection = _driver(mocker).connect.return_value

        with sandboxed([tmp_path]):
            DuckDBConnector(_file(tmp_path)).connect()

        connection.sql.assert_called_once_with('SELECT * FROM "orders"')

    def test_it_skips_a_descriptor_closed_since_it_was_listed(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Ensure a descriptor that closed between the listing and its check counts as none.

        The listing here holds a closed descriptor throughout, and the one
        `connect` opens once it runs.
        """
        config = _file(tmp_path)
        listed = tmp_path / "descriptors"
        listed.mkdir()
        (listed / "1048575").write_bytes(b"")
        monkeypatch.setattr("veridelta.connectors.duckdb._DESCRIPTORS", str(listed))
        driver = _driver(mocker)
        descriptors: list[int] = []

        def connect(*_: object, **__: object) -> MagicMock:
            descriptors.append(os.open(config.database, os.O_RDONLY))
            (listed / str(descriptors[-1])).write_bytes(b"")
            return driver.connect.return_value

        driver.connect.side_effect = connect
        try:
            with sandboxed([tmp_path]):
                DuckDBConnector(config).connect()
        finally:
            for descriptor in descriptors:
                os.close(descriptor)

        driver.connect.return_value.sql.assert_called_once_with('SELECT * FROM "orders"')


_PUSHDOWN = DuckDBConfig(database="warehouse.duckdb", table="src", pushdown=True)


class TestDuckDBPushdownSession:
    """Validate the session that runs compiled comparison SQL inside DuckDB."""

    def test_it_runs_every_statement_on_one_connection(self, mocker: MockerFixture) -> None:
        """Ensure the database opens once, read-only and in UTC, and closes once with its folder."""
        frame = pl.DataFrame({"id": [1, 2]})
        driver = _driver(mocker, frame)
        connection = driver.connect.return_value
        created = _spills(mocker)
        session = DuckDBPushdownSession(_PUSHDOWN)

        session.connect()
        first = session.execute_pushdown("SELECT 1", query_type="count")
        session.execute_pushdown("SELECT 2")
        assert Path(created.spy_return).is_dir()
        session.close()
        session.close()

        assert not Path(created.spy_return).exists()
        driver.connect.assert_called_once_with(
            "duckdb:warehouse.duckdb", read_only=True, config=_settings(created.spy_return)
        )
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
