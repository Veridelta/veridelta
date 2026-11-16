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
is refused before anything resolves it, even when it leads under a root, and
so is one that Python 3.11's `Path` would fold to a path on this machine, such
as `\\\localhost\C$\x.csv`. So is a `file:` URI that names a host, one whose
path starts with `//`, and one whose path Python cannot convert, as Windows
before Python 3.14 cannot one with a second `:`.

Each path is built from `tmp_path` with the system's own separators, never
from POSIX text.
"""

import ctypes
import glob
import json
import os
import socket
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path, PureWindowsPath
from typing import Any, cast
from urllib.parse import quote

import duckdb
import polars as pl
import pytest
from pytest_mock import MockerFixture

from veridelta import mcp_server
from veridelta.connectors.base import names_another_machine
from veridelta.connectors.database import sqlite_path
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.mcp_server import (
    RunReport,
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
    SnowflakeConfig,
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
    pytest.param(
        lambda path: "//?/UNC/localhost/" + _admin_share(path).replace("\\", "/"),
        id="unc-device-with-slashes",
    ),
    pytest.param(lambda path: f"\\\\localhost\\{_admin_share(path)}", id="localhost"),
    pytest.param(lambda path: f"\\\\127.0.0.1\\{_admin_share(path)}", id="loopback"),
    pytest.param(
        lambda path: f"\\\\\\localhost\\{_admin_share(path)}",
        id="localhost-after-three-backslashes",
    ),
    pytest.param(
        lambda path: "///localhost/" + _admin_share(path).replace("\\", "/"),
        id="localhost-after-three-slashes",
    ),
    pytest.param(
        lambda path: "\\/localhost/" + _admin_share(path).replace("\\", "/"),
        id="localhost-with-mixed-separators",
    ),
]
r"""Forms of a path to a file on this machine that Windows reads as on another
machine or as a device: an extended-length path, a device path, or a share.

On Python 3.11, Windows' `Path` drops the separators from the front of
`\\\localhost\C$` and `///localhost/C$` but one, and so reads each as a folder
`localhost` on this machine's drive. The server asks about the text before
`Path` reads it."""

_HOST_ALONE: list[Any] = [
    pytest.param("//localhost", id="slashes"),
    pytest.param("\\\\localhost", id="backslashes"),
    pytest.param("\\\\\\localhost", id="three-backslashes"),
]
r"""A machine's name alone after two separators or more, which Python 3.11's `Path` reads as `\localhost`."""

_HOSTS = ("localhost", "127.0.0.1")
"""The names `_ELSEWHERE` and `_HOST_ALONE` give this machine."""


_FILE_URIS: list[Any] = [
    pytest.param(lambda path: path.as_uri(), id="empty-host"),
    pytest.param(lambda path: f"file:///{_lowercase_drive(path.as_posix())}", id="lowercase-drive"),
]
"""Forms of a `file:` URI with a drive letter, which every reader opens as a path on that drive."""

_LOCAL_URIS: list[Any] = [
    pytest.param(
        lambda path: f"file:///{path.as_posix().replace('.', '%2E')}", id="percent-escapes"
    ),
    pytest.param(lambda path: f"fs:///{path.as_posix()}", id="delta-lake-scheme"),
    pytest.param(lambda path: f"opendal+fs:///{path.as_posix()}", id="opendal-scheme"),
]
"""Other forms of a URI with a drive letter whose path a reader opens on this machine."""

_LOCALHOST_URI: Any = pytest.param(
    lambda path: f"file://localhost/{path.as_posix()}", id="localhost"
)
"""A `file:` URI that names `localhost`, which PyIceberg reads with the host in front of the path."""

_TWO_SLASH_URIS: list[Any] = [
    pytest.param(
        lambda path: "file:////localhost/" + _admin_share(path).replace("\\", "/"),
        id="localhost-after-four-slashes",
    ),
    pytest.param(
        lambda path: "file://localhost//localhost/" + _admin_share(path).replace("\\", "/"),
        id="localhost-after-localhost",
    ),
    pytest.param(
        lambda path: "file://///localhost/" + _admin_share(path).replace("\\", "/"),
        id="localhost-after-five-slashes",
    ),
    pytest.param(
        lambda path: "file:" + "\\" * 4 + "localhost\\" + _admin_share(path),
        id="localhost-after-four-backslashes",
    ),
]
"""Forms of a `file:` URI whose path starts with `//`, which Windows reads as a share.

The URL standard reads a backslash as `/`, so four after `file:` count too."""


def _second_colon(folder: Path) -> str:
    """Write a `file:` URI of `x.csv` in a folder `a:b` in the folder, as in `file:///C:/data/a:b/x.csv`."""
    return f"file:///{folder.as_posix()}/a:b/x.csv"


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
        mocker.spy(os.path, "exists"),
        mocker.spy(os.path, "isfile"),
        mocker.spy(os.path, "isdir"),
        mocker.spy(os, "stat"),
        mocker.spy(os, "lstat"),
        mocker.spy(os, "scandir"),
        mocker.spy(glob, "glob"),
        mocker.spy(Path, "resolve"),
    ]
    return lambda: [
        text
        for spy in spies
        for call in spy.call_args_list
        for text in map(str, call.args[:1])
        if _names_a_machine_or_device(text)
    ]


def _names_a_machine_or_device(text: str) -> bool:
    r"""Say whether a spied call's path names another machine or a device, or this one by a name.

    On Python 3.11, Windows' `Path` reads `\\\localhost\C$\x.csv` as
    `\localhost\C$\x.csv`, so a path whose first name is one of `_HOSTS`, or
    this machine's name, counts too.
    """
    path = text.replace("/", "\\")
    first = path.lstrip("\\").split("\\")[0].lower()
    return path.startswith(("\\\\", "\\??\\")) or first in {*_HOSTS, socket.gethostname().lower()}


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


def _key_pair(path: str) -> SnowflakeConfig:
    """Build a Snowflake side that signs in with the key file at `path`, which the driver opens here."""
    return SnowflakeConfig(
        account="xy12345",
        user="analyst",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
        table="ORDERS",
        private_key_path=path,
    )


def _check(settings: Settings, source: SourceRef) -> SourceRef:
    """Check a source as every tool does, beside a CSV target under the root, and return it."""
    return check_data_paths(settings, source, SourceConfig(path="x.csv"))[0]


def _write(root: Path, source: str) -> None:
    """Write a configuration that compares a source with `x.csv` under the root."""
    (root / "veridelta.yaml").write_text(
        f"source:\n{source}target:\n  path: x.csv\nprimary_keys: [id]\n", encoding="utf-8"
    )


def _query(root: Path, path: str) -> RunReport:
    """Run a DuckDB query of the CSV file at `path` through a server with queries allowed, and return its report.

    The configuration compares the query with `x.csv` under the root.
    """
    if not (root / "w.duckdb").exists():
        _duckdb(root / "w.duckdb")
    query = f"SELECT id, v FROM read_csv('{path}')"
    _write(root, f"  type: duckdb\n  database: w.duckdb\n  query: {json.dumps(query)}\n")
    return run_configuration(Settings((root,), allow_queries=True), "veridelta.yaml")


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
    for letter in "ZYXWVUTSRQPONMLKJIHG":
        drive = f"{letter}:"
        if os.path.exists(f"{drive}\\"):
            continue
        # A letter a disconnected network drive holds reads as free, and subst refuses it.
        given = subprocess.run(["subst", drive, str(folder)], check=False, capture_output=True)
        if given.returncode == 0:
            break
    else:
        pytest.skip("Every drive letter is taken, so subst has none to give.")
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

    @pytest.mark.parametrize("name", ["x.csv", "*.csv"])
    @pytest.mark.parametrize("form", _ELSEWHERE)
    def test_it_refuses_a_data_path_before_resolving_it(
        self,
        settings: Settings,
        inside: Path,
        resolved: Callable[[], list[str]],
        form: Callable[[Path], str],
        name: str,
    ) -> None:
        r"""Ensure a data path or pattern written this way is refused, though it leads under the root.

        A long path such as `\\?\C:\data\x.csv` turns off the system's own
        reading of the path, and a share can name another machine, so the
        server reads neither, and matches no pattern there.
        """
        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=form(inside.parent / name)))

        assert resolved() == []

    @pytest.mark.parametrize("form", _ELSEWHERE)
    def test_it_refuses_a_file_a_scan_opens_before_resolving_it(
        self,
        settings: Settings,
        inside: Path,
        resolved: Callable[[], list[str]],
        form: Callable[[Path], str],
    ) -> None:
        """Ensure a file a lakehouse scan is about to open, named this way, is refused unresolved."""
        with pytest.raises(ConfigError, match="was about to open a file outside"):
            mcp_server._refuse_opened(settings, form(inside), remote=False)  # pyright: ignore[reportPrivateUsage] - a lakehouse scan calls it with each file it opens

        assert resolved() == []

    @pytest.mark.parametrize("form", _ELSEWHERE)
    def test_it_refuses_a_snowflake_key_file_before_resolving_it(
        self,
        settings: Settings,
        tmp_path: Path,
        resolved: Callable[[], list[str]],
        form: Callable[[Path], str],
    ) -> None:
        """Ensure a Snowflake key file named this way is refused before the driver could open it."""
        key = tmp_path / "k.pem"
        key.write_text("not a key\n", encoding="utf-8")

        with pytest.raises(ConfigError, match="private_key_path is outside the folders"):
            _check(settings, _key_pair(form(key)))

        assert resolved() == []

    @pytest.mark.parametrize("path", _HOST_ALONE)
    def test_it_refuses_a_machines_name_alone_before_resolving_it(
        self, settings: Settings, resolved: Callable[[], list[str]], path: str
    ) -> None:
        r"""Ensure a machine's name alone is refused in every setting, before anything resolves it.

        Python 3.11's `Path` reads each of these as `\localhost`, a folder on
        this machine's drive, so the server asks about the text first.
        """
        assert names_another_machine(path)
        with pytest.raises(ConfigError, match="is outside the folders this server reads from"):
            resolve_path(settings, path)
        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=path))
        with pytest.raises(ConfigError, match="was about to open a file outside"):
            mcp_server._refuse_opened(settings, path, remote=False)  # pyright: ignore[reportPrivateUsage] - a lakehouse scan calls it with each file it opens
        with pytest.raises(ConfigError, match="private_key_path is outside the folders"):
            _check(settings, _key_pair(path))

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
    def test_it_refuses_a_percent_encoded_sqlite_file_before_resolving_it(
        self,
        settings: Settings,
        tmp_path: Path,
        resolved: Callable[[], list[str]],
        form: Callable[[Path], str],
    ) -> None:
        r"""Ensure a SQLite path written this way with every character encoded, as in `%5C%5C`, is refused.

        ConnectorX decodes the whole path, so `sqlite_path` decodes it before
        it asks whether the path names another machine.
        """
        path = form(_sqlite(tmp_path / "x.db"))
        config = DatabaseConfig(type="database", uri=f"sqlite://{quote(path, safe='')}", table="t")

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

    @pytest.mark.parametrize(
        "name",
        [
            pytest.param("\\x.csv", id="from-the-drives-root"),
            pytest.param("x.csv", id="drive-relative"),
        ],
    )
    def test_it_reads_a_drive_given_to_a_folder_under_the_root(
        self, settings: Settings, tmp_path: Path, inside: Path, name: str
    ) -> None:
        """Ensure a path on a drive that `subst` gives the root counts as under the root.

        A drive-relative path such as `Z:x.csv`, on a drive the process has
        not used, counts from the drive's root.
        """
        with _substituted(tmp_path) as drive:
            _check(settings, SourceConfig(path=f"{drive}{name}"))
            assert resolve_path(settings, f"{drive}{name}") == inside

    @pytest.mark.parametrize(
        "name",
        [
            pytest.param("\\x.csv", id="from-the-drives-root"),
            pytest.param("x.csv", id="drive-relative"),
        ],
    )
    def test_it_refuses_a_drive_given_to_a_folder_outside(
        self, settings: Settings, outside: Path, name: str
    ) -> None:
        """Ensure a path on a drive that `subst` gives a folder outside the root is refused."""
        with _substituted(outside.parent) as drive, pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=f"{drive}{name}"))


@_ON_WINDOWS
class TestFileUris:
    """Validate `file:` URIs with a drive letter, which Windows converts to a path on that drive."""

    @pytest.mark.parametrize("form", [*_FILE_URIS, _LOCALHOST_URI, *_LOCAL_URIS])
    def test_it_reads_a_file_uri_under_the_root(
        self, settings: Settings, inside: Path, form: Callable[[Path], str]
    ) -> None:
        """Ensure a `file:` URI of a file under the root converts to that file, and passes."""
        uri = form(inside)

        readings = mcp_server._local_paths(uri, "source path")  # pyright: ignore[reportPrivateUsage] - the first reading is the path a reader opens

        assert Path(readings[0]) == inside
        _check(settings, SourceConfig(path=uri))

    @pytest.mark.parametrize("form", [*_FILE_URIS, _LOCALHOST_URI, *_LOCAL_URIS])
    def test_it_refuses_a_file_uri_outside_the_root(
        self, settings: Settings, outside: Path, form: Callable[[Path], str]
    ) -> None:
        """Ensure a `file:` URI of a file outside the root is refused."""
        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=form(outside)))

    @pytest.mark.parametrize("form", [*_FILE_URIS, _LOCALHOST_URI])
    def test_it_lists_a_delta_table_at_a_file_uri_under_the_root(
        self, settings: Settings, tmp_path: Path, form: Callable[[Path], str]
    ) -> None:
        """Ensure a Delta Lake table at a `file:` URI under the root is listed, and passes."""
        pl.DataFrame({"id": [1]}).write_delta(tmp_path / "t")

        _check(settings, DeltaLakeConfig(table_uri=form(tmp_path / "t")))

    @pytest.mark.parametrize(
        "host",
        [
            pytest.param("127.0.0.1", id="loopback"),
            pytest.param(socket.gethostname(), id="this-machines-name"),
        ],
    )
    def test_it_refuses_a_file_uri_that_names_a_host(
        self,
        settings: Settings,
        tmp_path: Path,
        resolved: Callable[[], list[str]],
        mocker: MockerFixture,
        host: str,
    ) -> None:
        """Ensure a `file:` URI that names a host other than `localhost` is refused before it is opened.

        On Windows, deltalake opens such a URI on that host, even when the host
        is this machine.
        """
        pl.DataFrame({"id": [1]}).write_delta(tmp_path / "t")
        loaded = mocker.patch("deltalake.DeltaTable")
        uri = f"file://{host}/{(tmp_path / 't').as_posix()}"

        for side in (SourceConfig(path=f"{uri}/x.csv"), DeltaLakeConfig(table_uri=uri)):
            with pytest.raises(ConfigError, match="is a file URI that names a host") as refused:
                _check(settings, side)

            # The URI's own `//host`, since the fixed text could hold a short machine name.
            assert f"//{host}" not in str(refused.value)
        loaded.assert_not_called()
        assert resolved() == []

    @pytest.mark.parametrize("form", _TWO_SLASH_URIS)
    def test_it_refuses_a_file_uri_whose_path_starts_with_two_slashes(
        self,
        settings: Settings,
        inside: Path,
        resolved: Callable[[], list[str]],
        mocker: MockerFixture,
        form: Callable[[Path], str],
    ) -> None:
        """Ensure a `file:` URI whose path Windows reads as a share is refused before it is converted."""
        converted = mocker.spy(mcp_server, "url2pathname")

        with pytest.raises(ConfigError, match="is a URI whose path starts with '//'"):
            _check(settings, SourceConfig(path=form(inside)))

        converted.assert_not_called()
        assert resolved() == []

    @pytest.mark.skipif(
        sys.version_info >= (3, 14),
        reason="Python 3.14 converts a file URI's path with a second ':', or a ':' alone, on Windows.",
    )
    @pytest.mark.parametrize(
        "form",
        [
            pytest.param(_second_colon, id="second-colon"),
            pytest.param(lambda folder: "file::/x.csv", id="colon-with-no-drive"),
        ],
    )
    def test_it_refuses_a_file_uri_python_cannot_convert(
        self, settings: Settings, tmp_path: Path, form: Callable[[Path], str]
    ) -> None:
        """Ensure a `file:` URI whose path has a `:` other than a drive letter's is refused, never quoted.

        Before Python 3.14, Windows' `url2pathname` raises an `OSError` on a
        path with a second `:`, even one under the root, and an `IndexError`
        on one with nothing before its `:`.
        """
        uri = form(tmp_path)

        with pytest.raises(ConfigError, match="whose path Python cannot read") as converted:
            mcp_server._local_paths(uri, "source path")  # pyright: ignore[reportPrivateUsage] - it converts each path a reader opens
        with pytest.raises(
            ConfigError, match="The source path is a file URI whose path Python cannot read"
        ) as checked:
            _check(settings, SourceConfig(path=uri))

        assert "x.csv" not in f"{converted.value} {checked.value}"

    @pytest.mark.skipif(
        sys.version_info < (3, 14),
        reason="Windows before Python 3.14 cannot convert a file URI's path with a second ':'.",
    )
    def test_it_holds_a_file_uri_with_a_second_colon_to_the_roots(
        self, settings: Settings, tmp_path: Path, outside: Path
    ) -> None:
        """Ensure a `file:` URI with a drive letter and a second `:` converts, and is held to the roots.

        From Python 3.14, Windows' `url2pathname` converts such a path, with
        `a:b` as a folder's name, which `realpath` would read as a drive.
        """
        uri = _second_colon(outside.parent)

        readings = mcp_server._local_paths(uri, "source path")  # pyright: ignore[reportPrivateUsage] - the first reading is the path a reader opens

        assert readings[0] == f"{outside.parent}\\a:b\\x.csv"
        _check(settings, SourceConfig(path=_second_colon(tmp_path)))
        with pytest.raises(ConfigError, match=_OUTSIDE):
            _check(settings, SourceConfig(path=uri))


@_ON_WINDOWS
class TestDuckDBSides:
    """Validate a DuckDB side on Windows, from the configuration to the read, in the server's sandbox."""

    def test_it_reads_a_duckdb_file_named_with_a_lowercase_drive_letter(
        self, settings: Settings, tmp_path: Path, inside: Path
    ) -> None:
        """Ensure DuckDB opens the file the server checked, inside the server's sandbox."""
        database = _lowercase_drive(str(_duckdb(tmp_path / "w.duckdb")))
        _write(tmp_path, f"  type: duckdb\n  database: {json.dumps(database)}\n  table: t\n")

        report = describe_side(settings, "veridelta.yaml", "source")

        assert list(report["columns"]) == ["id"]

    @pytest.mark.parametrize(
        "form",
        [
            pytest.param(
                lambda root, path: _lowercase_drive(str(path)), id="lowercase-drive-letter"
            ),
            pytest.param(
                lambda root, path: f"{_from_the_drives_root(path.parent)}\\{path.name}",
                id="from-the-drives-root",
            ),
            pytest.param(
                lambda root, path: f"{path.drive}..\\{path.parent.name}\\{path.name}",
                id="drive-relative-and-dot-dot",
            ),
            pytest.param(
                lambda root, path: f"{root.as_posix()}/../{path.parent.name}/{path.name}",
                id="root-and-dot-dot",
            ),
        ],
    )
    def test_it_holds_a_query_to_the_roots_however_windows_writes_the_path(
        self, tmp_path: Path, inside: Path, outside: Path, form: Callable[[Path, Path], str]
    ) -> None:
        """Ensure DuckDB's sandbox refuses a file outside the roots, however a query writes its path.

        The working directory is the root, so `C:..` is the folder above it,
        which holds the folder outside.
        """
        with pytest.raises(ConnectorError, match="Permission Error"):
            _query(tmp_path, form(tmp_path, outside))

    @pytest.mark.parametrize(
        "form",
        [
            pytest.param(lambda root, path: str(path), id="as-written"),
            pytest.param(
                lambda root, path: f"{root.as_posix()}/../{path.parent.name}/{path.name}",
                id="root-and-dot-dot",
            ),
        ],
    )
    def test_it_reads_a_query_under_the_roots(
        self, tmp_path: Path, inside: Path, form: Callable[[Path, Path], str]
    ) -> None:
        """Ensure DuckDB's sandbox reads a file under the root, so each refusal here is the sandbox's own."""
        assert _query(tmp_path, form(tmp_path, inside))["verdict"] == "match"

    @pytest.mark.parametrize("name", ["x.csv", "*.csv"])
    def test_it_holds_a_query_through_a_junction_to_the_folder_it_leads_to(
        self, tmp_path: Path, inside: Path, outside: Path, name: str
    ) -> None:
        """Ensure DuckDB's sandbox follows a junction before it matches the roots.

        It reads a file, or a pattern, through a junction to a folder under
        the root, and refuses one through a junction to a folder outside.
        """
        _csv(tmp_path / "sub" / "x.csv", "inside")

        with (
            _junction(tmp_path / "k", tmp_path / "sub") as here,
            _junction(tmp_path / "j", outside.parent) as there,
        ):
            assert _query(tmp_path, f"{here}\\{name}")["verdict"] == "match"
            with pytest.raises(ConnectorError, match="Permission Error"):
                _query(tmp_path, f"{there}\\{name}")

    @pytest.mark.parametrize("form", _ELSEWHERE)
    def test_it_reads_no_query_on_another_machine_or_a_device(
        self, tmp_path: Path, inside: Path, outside: Path, form: Callable[[Path], str]
    ) -> None:
        r"""Ensure a query fails on a file outside the roots written as a share or a device path.

        The server never sees a query's paths, so this holds DuckDB to it. The
        test cannot tell whether DuckDB reached the machine before it failed.
        """
        with pytest.raises(ConnectorError):
            _query(tmp_path, form(outside))
