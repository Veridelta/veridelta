# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

r"""Integration tests of the MCP server's folder rule on Windows, against real files.

The unit tests read each Windows path form on any system, through `ntpath` and
a stand-in disk. These run on Windows alone, where the system itself resolves
each path: a drive-relative path such as `C:x.csv`, a path from the drive's
root such as `\data\x.csv`, mixed separators, a name with a trailing dot or
space, an 8.3 short name, a directory junction, a drive that `subst` gives a
folder, a `file:` URI with a drive letter, and a lowercase drive letter. A path
the system reads as on another machine or as a device, such as `\\?\C:\x.csv`,
is refused before anything resolves it, even when it leads under a root.

Each path is built from `tmp_path` with the system's own separators, never
from POSIX text.
"""

import ctypes
import glob
import json
import os
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path, PureWindowsPath
from typing import Any, cast

import duckdb
import polars as pl
import pytest
from pytest_mock import MockerFixture

from veridelta import mcp_server
from veridelta.connectors.base import names_another_machine
from veridelta.connectors.database import sqlite_path
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.mcp_server import (
    Settings,
    check_data_paths,
    describe_side,
    resolve_path,
    run_configuration,
)
from veridelta.models import (
    DatabaseConfig,
    DeltaLakeConfig,
    DuckDBConfig,
    SourceConfig,
    SourceRef,
)

pytestmark = [pytest.mark.integration]

_ON_WINDOWS = pytest.mark.skipif(
    sys.platform != "win32",
    reason="Only Windows reads these path forms, and resolves them against its own disk.",
)

_OUTSIDE = r"The source [\w ]+'.*' is outside the folders this server reads from"
"""The refusal of a source path that resolves outside every root."""

_Form = Callable[[Path, str], str]
"""A way to write the path of a named file in a folder."""


def _lowercase_drive(text: str) -> str:
    r"""Write a path's drive letter in lowercase, as in `c:\data\x.csv`."""
    assert text[1] == ":", text
    return text[0].lower() + text[1:]


def _from_the_drives_root(folder: Path) -> str:
    r"""Write a folder's path from its drive's root, with no drive, as in `\data`."""
    return str(folder)[len(folder.drive) :]


def _admin_share(path: Path) -> str:
    r"""Write a path from its drive's administrative share, as in `C$\data\x.csv`."""
    return f"{path.drive[0]}$\\{path.relative_to(path.anchor)}"


_HERE: list[Any] = [
    pytest.param(lambda folder, name: f"{folder.drive}{name}", id="drive-relative"),
    pytest.param(
        lambda folder, name: f"{_from_the_drives_root(folder)}\\{name}", id="from-the-drives-root"
    ),
    pytest.param(lambda folder, name: f"{folder.as_posix()}\\{name}", id="mixed-separators"),
    pytest.param(lambda folder, name: f"{folder}\\{name}.", id="trailing-dot"),
    pytest.param(lambda folder, name: f"{folder}\\{name} ", id="trailing-space"),
    pytest.param(lambda folder, name: f"{folder}.\\{name}", id="folder-with-a-trailing-dot"),
    pytest.param(
        lambda folder, name: _lowercase_drive(f"{folder}\\{name}"), id="lowercase-drive-letter"
    ),
]
"""Forms of a path the system reads as the named file in the folder, the working directory.

Windows strips a trailing dot or space from the last name, and a trailing dot
from any folder's name, as it resolves a path."""

_THERE: list[Any] = [
    pytest.param(
        lambda root, folder: f"{_from_the_drives_root(folder)}\\x.csv", id="from-the-drives-root"
    ),
    pytest.param(lambda root, folder: f"{folder.as_posix()}\\x.csv", id="mixed-separators"),
    pytest.param(
        lambda root, folder: f"{root.as_posix()}\\..//{folder.name}\\x.csv",
        id="mixed-separators-and-dot-dot",
    ),
    pytest.param(lambda root, folder: f"{folder}\\x.csv.", id="trailing-dot"),
    pytest.param(lambda root, folder: f"{folder}\\x.csv ", id="trailing-space"),
    pytest.param(
        lambda root, folder: f"{root}\\..\\{folder.name}.\\x.csv", id="folder-with-a-trailing-dot"
    ),
    pytest.param(
        lambda root, folder: _lowercase_drive(f"{folder}\\x.csv"), id="lowercase-drive-letter"
    ),
]
"""Forms of a path the system reads as `x.csv` in a folder outside the root."""

_ELSEWHERE: list[Any] = [
    pytest.param(lambda path: f"\\\\?\\{path}", id="extended-length"),
    pytest.param(lambda path: f"//?/{path.as_posix()}", id="extended-length-with-slashes"),
    pytest.param(lambda path: f"\\\\.\\{path}", id="device"),
    pytest.param(lambda path: f"\\??\\{path}", id="kernel"),
    pytest.param(lambda path: f"\\\\?\\UNC\\localhost\\{_admin_share(path)}", id="unc-device"),
    pytest.param(lambda path: f"\\\\.\\UNC\\localhost\\{_admin_share(path)}", id="unc-dot-device"),
    pytest.param(lambda path: f"\\\\localhost\\{_admin_share(path)}", id="localhost"),
    pytest.param(lambda path: f"\\\\127.0.0.1\\{_admin_share(path)}", id="loopback"),
    pytest.param(
        lambda path: "\\/localhost/" + _admin_share(path).replace("\\", "/"),
        id="localhost-with-mixed-separators",
    ),
]
"""Forms of a path to a file on this machine that Windows reads as on another
machine or as a device: an extended-length path, a device path, or a share."""


_FILE_URIS: list[Any] = [
    pytest.param(lambda path: path.as_uri(), id="empty-host"),
    pytest.param(lambda path: f"file:///{_lowercase_drive(path.as_posix())}", id="lowercase-drive"),
]
"""Forms of a `file:` URI with a drive letter, which every reader opens as a path on that drive."""

_LOCALHOST_URI: Any = pytest.param(
    lambda path: f"file://localhost/{path.as_posix()}", id="localhost"
)
"""A `file:` URI that names `localhost`, which PyIceberg reads with the host in front of the path."""


@pytest.fixture(autouse=True)
def _in_the_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run each test in `tmp_path`, the server's one root, as `veridelta mcp` runs."""
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """A server whose one root is `tmp_path`."""
    return Settings((tmp_path,))


@pytest.fixture
def inside(tmp_path: Path) -> Path:
    """A CSV file under the root."""
    return _csv(tmp_path / "x.csv", "inside")


@pytest.fixture
def outside(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A CSV file in a folder beside the root, outside it."""
    return _csv(tmp_path_factory.mktemp("outside") / "x.csv", "outside")


@pytest.fixture
def resolved(mocker: MockerFixture) -> Callable[[], list[str]]:
    """Spy on each call that resolves, matches, or opens a path.

    Returns:
        Callable[[], list[str]]: Lists each path the system read as on another
            machine or a device that a spied call was given so far.
    """
    spies = [
        mocker.spy(os.path, "realpath"),
        mocker.spy(os, "stat"),
        mocker.spy(glob, "glob"),
        mocker.spy(Path, "resolve"),
    ]
    return lambda: [
        text
        for spy in spies
        for call in spy.call_args_list
        if (text := str(call.args[0])).replace("/", "\\").startswith(("\\\\", "\\??\\"))
    ]


def _csv(path: Path, value: str) -> Path:
    """Write a CSV file with one row, whose `v` says which file it is."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"id,v\n1,{value}\n", encoding="utf-8", newline="")
    return path


def _duckdb(path: Path) -> Path:
    """Write a DuckDB file with a table `t`, and close it."""
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE t AS SELECT 1 AS id")
    return path


def _sqlite(path: Path) -> Path:
    """Write a SQLite file with a table `t`, and close it."""
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE t (id INTEGER)")
        connection.commit()
    return path


def _sqlite_config(path: str) -> DatabaseConfig:
    """Name a SQLite file by a `sqlite://` URI, its path written with forward slashes."""
    return DatabaseConfig(type="database", uri=f"sqlite://{path.replace(chr(92), '/')}", table="t")


def _check(settings: Settings, source: SourceRef) -> SourceRef:
    """Check a source as every tool does, beside a CSV target under the root, and return it."""
    return check_data_paths(settings, source, SourceConfig(path="x.csv"))[0]


def _write(root: Path, source: str) -> None:
    """Write a configuration that compares a source with `x.csv` under the root."""
    (root / "veridelta.yaml").write_text(
        f"source:\n{source}target:\n  path: x.csv\nprimary_keys: [id]\n", encoding="utf-8"
    )


def _short(path: Path) -> str:
    r"""Return the 8.3 form Windows gives a path, such as `C:\PROGRA~1`, or skip.

    A volume makes 8.3 names only while its setting allows them, so a test
    that needs one skips where the folder has none.
    """
    if sys.platform != "win32":
        raise AssertionError("Only Windows gives a path an 8.3 form.")
    else:  # The branch keeps the Windows-only names out of a type check on any other system.
        buffer = ctypes.create_unicode_buffer(32768)
        if not ctypes.windll.kernel32.GetShortPathNameW(str(path), buffer, len(buffer)):
            raise ctypes.WinError()
        if PureWindowsPath(buffer.value).name.lower() == path.name.lower():
            pytest.skip("The volume makes no 8.3 names, so the folder has no short form.")
        return buffer.value


@contextmanager
def _junction(link: Path, target: Path) -> Iterator[Path]:
    """Link a folder to another with a directory junction, which needs no privilege, for the block."""
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(target)], check=True, capture_output=True
    )
    try:
        yield link
    finally:
        os.rmdir(link)  # Removes the junction alone, never the folder it leads to.


@contextmanager
def _substituted(folder: Path) -> Iterator[str]:
    """Give a folder a drive of its own with `subst`, which needs no privilege, for the block."""
    free = [letter for letter in "ZYXWVUTSRQPONMLKJIHG" if not os.path.exists(f"{letter}:\\")]
    if not free:
        pytest.skip("Every drive letter is taken, so subst has none to give.")
    drive = f"{free[0]}:"
    subprocess.run(["subst", drive, str(folder)], check=True, capture_output=True)
    try:
        yield drive
    finally:
        subprocess.run(["subst", drive, "/D"], check=True, capture_output=True)


@_ON_WINDOWS
class TestAnotherMachine:
    """Validate that a path Windows reads as on another machine or a device is never resolved."""

    @pytest.mark.parametrize("form", _ELSEWHERE)
    def test_it_reads_the_path_as_one_that_can_reach_another_machine(
        self, inside: Path, form: Callable[[Path], str]
    ) -> None:
        """Ensure each form names another machine or a device, as Windows reads it."""
        assert names_another_machine(form(inside))

    @pytest.mark.parametrize("form", _ELSEWHERE)
    def test_it_refuses_a_tool_calls_path_before_resolving_it(
        self,
        settings: Settings,
        tmp_path: Path,
        resolved: Callable[[], list[str]],
        form: Callable[[Path], str],
    ) -> None:
        """Ensure a configuration file named this way is refused, even one under the root."""
        path = form(tmp_path / "veridelta.yaml")
        _write(tmp_path, "  path: x.csv\n")

        with pytest.raises(ConfigError, match="is outside the folders this server reads from"):
            resolve_path(settings, path)

        assert resolved() == []

    @pytest.mark.parametrize("form", _ELSEWHERE)
    def test_it_refuses_a_data_path_before_resolving_it(
        self,
        settings: Settings,
        inside: Path,
        resolved: Callable[[], list[str]],
        form: Callable[[Path], str],
    ) -> None:
        r"""Ensure a data path written this way is refused, though it leads to a file under the root.

        A long path such as `\\?\C:\data\x.csv` turns off the system's own
        reading of the path, and a share can name another machine, so the
        server reads neither.
        """
        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=form(inside)))

        assert resolved() == []

    @pytest.mark.parametrize("form", _ELSEWHERE)
    def test_it_refuses_a_duckdb_file_before_resolving_it(
        self,
        settings: Settings,
        tmp_path: Path,
        resolved: Callable[[], list[str]],
        form: Callable[[Path], str],
    ) -> None:
        """Ensure a DuckDB file named this way is refused before the server opens it to resolve it."""
        database = form(_duckdb(tmp_path / "w.duckdb"))

        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, DuckDBConfig(type="duckdb", database=database, table="t"))

        assert resolved() == []

    @pytest.mark.parametrize("form", _ELSEWHERE)
    def test_it_refuses_a_sqlite_file_before_resolving_it(
        self,
        settings: Settings,
        tmp_path: Path,
        resolved: Callable[[], list[str]],
        form: Callable[[Path], str],
    ) -> None:
        """Ensure a SQLite path written this way comes back unresolved, and is refused."""
        path = form(_sqlite(tmp_path / "x.db")).replace("\\", "/")
        config = _sqlite_config(path)

        assert sqlite_path(config) == path
        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, config)
        assert resolved() == []

    @pytest.mark.parametrize("form", _ELSEWHERE)
    def test_it_refuses_a_delta_table_before_listing_it(
        self,
        settings: Settings,
        tmp_path: Path,
        resolved: Callable[[], list[str]],
        mocker: MockerFixture,
        form: Callable[[Path], str],
    ) -> None:
        """Ensure a Delta Lake table named this way is refused before deltalake reads its log."""
        pl.DataFrame({"id": [1]}).write_delta(tmp_path / "t")
        loaded = mocker.patch("deltalake.DeltaTable")

        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, DeltaLakeConfig(table_uri=form(tmp_path / "t")))

        loaded.assert_not_called()
        assert resolved() == []


@_ON_WINDOWS
class TestThisMachine:
    """Validate each form of a path on this machine as Windows resolves it, inside or outside the root."""

    @pytest.mark.parametrize("form", _HERE)
    def test_it_reads_a_path_windows_resolves_under_the_root(
        self, settings: Settings, tmp_path: Path, inside: Path, form: _Form
    ) -> None:
        """Ensure the check passes a path to a file under the root, and Polars reads that file."""
        path = form(tmp_path, "x.csv")

        assert not names_another_machine(path)
        _check(settings, SourceConfig(path=path))
        assert pl.read_csv(path)["v"].to_list() == ["inside"]

    @pytest.mark.parametrize("form", _HERE)
    def test_it_resolves_a_tool_calls_path_as_windows_does(
        self, settings: Settings, tmp_path: Path, form: _Form
    ) -> None:
        """Ensure a configuration file named any of these ways resolves to the file under the root."""
        _write(tmp_path, "  path: x.csv\n")

        assert (
            resolve_path(settings, form(tmp_path, "veridelta.yaml")) == tmp_path / "veridelta.yaml"
        )

    @pytest.mark.parametrize("form", _HERE)
    def test_it_hands_duckdb_the_file_windows_resolves(
        self, settings: Settings, tmp_path: Path, form: _Form
    ) -> None:
        """Ensure the server checks a DuckDB file by the path Windows resolves, and hands DuckDB that path."""
        database = _duckdb(tmp_path / "w.duckdb")

        checked = _check(
            settings, DuckDBConfig(type="duckdb", database=form(tmp_path, "w.duckdb"), table="t")
        )

        assert Path(cast("DuckDBConfig", checked).database) == database

    @pytest.mark.parametrize(
        "form",
        [
            pytest.param(lambda folder: f"{folder.drive}x.db", id="drive-relative"),
            pytest.param(
                lambda folder: f"{_from_the_drives_root(folder)}\\x.db", id="from-the-drives-root"
            ),
            pytest.param(lambda folder: f"{folder}.\\x.db", id="folder-with-a-trailing-dot"),
            pytest.param(
                lambda folder: _lowercase_drive(f"{folder}\\x.db"), id="lowercase-drive-letter"
            ),
        ],
    )
    def test_it_resolves_a_sqlite_path_as_windows_does(
        self, settings: Settings, tmp_path: Path, form: Callable[[Path], str]
    ) -> None:
        """Ensure a SQLite path resolves to the file Windows opens, which the check passes."""
        database = _sqlite(tmp_path / "x.db")
        config = _sqlite_config(form(tmp_path))

        assert Path(cast("str", sqlite_path(config))) == database
        _check(settings, config)

    @pytest.mark.parametrize("form", _THERE)
    def test_it_refuses_a_path_windows_resolves_outside_the_root(
        self,
        settings: Settings,
        tmp_path: Path,
        outside: Path,
        form: Callable[[Path, Path], str],
    ) -> None:
        """Ensure a path that leads outside the root is refused, however it is written."""
        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=form(tmp_path, outside.parent)))

    def test_it_refuses_a_drive_relative_path_from_a_working_directory_outside(
        self, settings: Settings, outside: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure `C:x.csv` counts from the working directory on its drive, which can lie outside."""
        monkeypatch.chdir(outside.parent)

        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=f"{outside.drive}x.csv"))


@_ON_WINDOWS
class TestShortNames:
    """Validate 8.3 short names, which Windows resolves to the long name of the same folder."""

    def test_it_reads_a_short_name_under_the_root(self, settings: Settings, tmp_path: Path) -> None:
        """Ensure a path through a folder's short name counts as the folder under the root."""
        data = _csv(tmp_path / "a folder with a long name" / "x.csv", "inside")
        path = f"{_short(data.parent)}\\x.csv"

        _check(settings, SourceConfig(path=path))
        assert resolve_path(settings, path) == data
        assert pl.read_csv(path)["v"].to_list() == ["inside"]

    def test_it_refuses_a_short_name_outside_the_root(
        self, settings: Settings, outside: Path
    ) -> None:
        """Ensure a path through a short name outside the root is refused."""
        data = _csv(outside.parent / "a folder with a long name" / "x.csv", "outside")
        path = f"{_short(data.parent)}\\x.csv"

        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=path))
        with pytest.raises(ConfigError, match="is outside the folders this server reads from"):
            resolve_path(settings, path)

    def test_it_resolves_a_root_given_by_its_short_name(self, tmp_path: Path, inside: Path) -> None:
        """Ensure a root given by its short name holds the files under its long name."""
        settings = Settings((Path(_short(tmp_path)),))

        assert settings.roots == (tmp_path,)
        _check(settings, SourceConfig(path=str(inside)))

    def test_it_hands_duckdb_and_sqlite_the_long_name(
        self, settings: Settings, tmp_path: Path
    ) -> None:
        """Ensure a DuckDB or SQLite file named through a short name is checked and opened by its long name."""
        folder = tmp_path / "a folder with a long name"
        folder.mkdir()
        database = _duckdb(folder / "w.duckdb")
        lite = _sqlite(folder / "x.db")
        short = _short(folder)

        checked = _check(
            settings, DuckDBConfig(type="duckdb", database=f"{short}\\w.duckdb", table="t")
        )
        config = _sqlite_config(f"{short}\\x.db")

        assert Path(cast("DuckDBConfig", checked).database) == database
        assert Path(cast("str", sqlite_path(config))) == lite
        _check(settings, config)


@_ON_WINDOWS
class TestJunctions:
    """Validate directory junctions, which Windows follows as it resolves a path."""

    @pytest.fixture
    def junction(self, tmp_path: Path, outside: Path) -> Iterator[Path]:
        """A junction `j` under the root that leads to the folder outside it."""
        with _junction(tmp_path / "j", outside.parent) as link:
            yield link

    @pytest.mark.parametrize("name", ["x.csv", "*.csv"])
    def test_it_refuses_a_path_through_a_junction_that_leads_outside(
        self, settings: Settings, junction: Path, name: str
    ) -> None:
        """Ensure a path, or a pattern, through a junction to a folder outside the root is refused."""
        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=f"{junction}\\{name}"))

    def test_it_refuses_a_tool_calls_path_through_a_junction(
        self, settings: Settings, junction: Path, outside: Path
    ) -> None:
        """Ensure a configuration file reached through a junction to outside the root is refused."""
        _write(outside.parent, "  path: x.csv\n")

        with pytest.raises(ConfigError, match="is outside the folders this server reads from"):
            resolve_path(settings, "j\\veridelta.yaml")

    def test_it_folds_dot_dot_after_a_junction_before_following_it(
        self, settings: Settings, junction: Path, inside: Path
    ) -> None:
        r"""Ensure `j\..` counts as the root, since Windows folds `..` by its text before it follows `j`."""
        path = f"{junction}\\..\\x.csv"

        _check(settings, SourceConfig(path=path))
        assert pl.read_csv(path)["v"].to_list() == ["inside"]

    def test_it_refuses_a_duckdb_file_through_a_junction(
        self, settings: Settings, junction: Path, outside: Path
    ) -> None:
        """Ensure a DuckDB file reached through a junction to outside the root is refused."""
        _duckdb(outside.parent / "w.duckdb")

        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(
                settings, DuckDBConfig(type="duckdb", database=f"{junction}\\w.duckdb", table="t")
            )

    def test_it_refuses_a_sqlite_file_through_a_junction(
        self, settings: Settings, junction: Path, outside: Path
    ) -> None:
        """Ensure a SQLite path resolves through a junction to the file outside, and is refused."""
        lite = _sqlite(outside.parent / "x.db")
        config = _sqlite_config(f"{junction}\\x.db")

        assert Path(cast("str", sqlite_path(config))) == lite
        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, config)

    def test_it_refuses_a_delta_table_through_a_junction(
        self, settings: Settings, junction: Path, outside: Path
    ) -> None:
        """Ensure a Delta Lake table reached through a junction to outside the root is refused."""
        pl.DataFrame({"id": [1]}).write_delta(outside.parent / "t")

        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, DeltaLakeConfig(table_uri=f"{junction}\\t"))

    def test_it_refuses_a_delta_log_that_is_a_junction(
        self, settings: Settings, tmp_path: Path, outside: Path
    ) -> None:
        """Ensure a table under the root whose `_delta_log` is a junction to a log outside is refused."""
        pl.DataFrame({"id": [1]}).write_delta(outside.parent / "t")
        (tmp_path / "t").mkdir()

        with (
            _junction(tmp_path / "t" / "_delta_log", outside.parent / "t" / "_delta_log"),
            pytest.raises(ConfigError, match=r"The source log '.*' is outside the folders"),
        ):
            _check(settings, DeltaLakeConfig(table_uri=str(tmp_path / "t")))


@_ON_WINDOWS
class TestSubstitutedDrives:
    """Validate a drive that `subst` gives a folder, which Windows resolves to the folder."""

    def test_it_reads_a_drive_given_to_a_folder_under_the_root(
        self, settings: Settings, tmp_path: Path, inside: Path
    ) -> None:
        """Ensure a path on a drive that `subst` gives the root counts as under the root."""
        with _substituted(tmp_path) as drive:
            _check(settings, SourceConfig(path=f"{drive}\\x.csv"))
            assert resolve_path(settings, f"{drive}\\x.csv") == inside

    def test_it_refuses_a_drive_given_to_a_folder_outside(
        self, settings: Settings, outside: Path
    ) -> None:
        """Ensure a path on a drive that `subst` gives a folder outside the root is refused."""
        with _substituted(outside.parent) as drive, pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=f"{drive}\\x.csv"))


@_ON_WINDOWS
class TestFileUris:
    """Validate `file:` URIs with a drive letter, which Windows converts to a path on that drive."""

    @pytest.mark.parametrize("form", [*_FILE_URIS, _LOCALHOST_URI])
    def test_it_reads_a_file_uri_under_the_root(
        self, settings: Settings, inside: Path, form: Callable[[Path], str]
    ) -> None:
        """Ensure a `file:` URI of a file under the root converts to that file, and passes."""
        uri = form(inside)

        readings = mcp_server._local_paths(uri, "source path")  # pyright: ignore[reportPrivateUsage] - the first reading is the path a reader opens

        assert Path(readings[0]) == inside
        _check(settings, SourceConfig(path=uri))

    @pytest.mark.parametrize("form", [*_FILE_URIS, _LOCALHOST_URI])
    def test_it_refuses_a_file_uri_outside_the_root(
        self, settings: Settings, outside: Path, form: Callable[[Path], str]
    ) -> None:
        """Ensure a `file:` URI of a file outside the root is refused."""
        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=form(outside)))

    @pytest.mark.parametrize("form", _FILE_URIS)
    def test_it_lists_a_delta_table_at_a_file_uri_under_the_root(
        self, settings: Settings, tmp_path: Path, form: Callable[[Path], str]
    ) -> None:
        """Ensure a Delta Lake table at a `file:` URI under the root is listed, and passes."""
        pl.DataFrame({"id": [1]}).write_delta(tmp_path / "t")

        _check(settings, DeltaLakeConfig(table_uri=form(tmp_path / "t")))


@_ON_WINDOWS
class TestLowercaseDriveLetters:
    """Validate a DuckDB side named with a lowercase drive letter, from the configuration to the read."""

    def test_it_reads_a_duckdb_file_named_with_a_lowercase_drive_letter(
        self, settings: Settings, tmp_path: Path, inside: Path
    ) -> None:
        """Ensure DuckDB opens the file the server checked, inside the server's sandbox."""
        database = _lowercase_drive(str(_duckdb(tmp_path / "w.duckdb")))
        _write(tmp_path, f"  type: duckdb\n  database: {json.dumps(database)}\n  table: t\n")

        report = describe_side(settings, "veridelta.yaml", "source")

        assert list(report["columns"]) == ["id"]

    def test_it_holds_a_query_to_the_roots_through_a_lowercase_drive_letter(
        self, tmp_path: Path, inside: Path, outside: Path
    ) -> None:
        """Ensure DuckDB's sandbox refuses a file outside the roots named with a lowercase drive letter."""
        _duckdb(tmp_path / "w.duckdb")
        query = f"SELECT id, v FROM read_csv('{_lowercase_drive(str(outside))}')"
        _write(tmp_path, f"  type: duckdb\n  database: w.duckdb\n  query: {json.dumps(query)}\n")

        with pytest.raises(ConnectorError, match="Permission Error"):
            run_configuration(Settings((tmp_path,), allow_queries=True), "veridelta.yaml")
