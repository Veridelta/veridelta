# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""DuckDB and MotherDuck sources read into Polars for a local comparison.

A DuckDB source is compared by the local engine, so it pairs with files,
lakehouse tables, databases, or another DuckDB source. `connect()` opens the
database, reads the configured `table` or `query` once through DuckDB's own
Polars export, and closes the connection. Requires the `duckdb` extra
(`uv add 'veridelta[duckdb]'`).

Two tables in one database that both opt into `pushdown` are compared inside
DuckDB instead: `DuckDBPushdownSession` runs each compiled statement on one
connection and reads back only counts and keys.

A file opens read-only, so the read can never change it, and as a DuckDB
database, so no extension loads to open a SQLite file. A MotherDuck database
opens read-write, because a read-only connection needs a read-scaling token.
Each session reads time in UTC, so a timestamp with a time zone, or a date cast
in a `query`, does not depend on the machine. A file's connection reads other
files only from the file's own folder, or inside `sandboxed`, from the folders
that names, since a view stored in the file runs whatever its author wrote.
It reads and writes no persistent secret. DuckDB replays the write-ahead logs
beside the file as it opens it, so a log that lies outside the file's own
folder, or leads out of it, is refused. A large query spills into a folder
this process creates in the system's temporary folder, never beside the file.
Inside `sandboxed`, a file and its logs open only by a path with no link in
it, and DuckDB must open the file the path named just before. On Windows,
DuckDB's own check of the folders lets a drive-relative path with a lowercase
letter out, and it resolves a share path, reaching that machine, before it
refuses it. So a file from someone else is code there, and inside `sandboxed`
on Windows a connection opens no other file at all, so a query that would
spill fails.

A token never reaches a log line or an error. It goes to DuckDB as a
connection setting, never in the connection string, and a failed read is
reported with the token replaced and without the driver's exception attached.
"""

import errno
import logging
import os
import shutil
import stat
import sys
import tempfile
import threading
import time
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, cast

import polars as pl

from veridelta.connectors.base import (
    PROBE_NEEDS_A_TABLE,
    PushdownQueryType,
    PushdownSession,
    ReaderConnector,
    mask_secrets,
    optional_module,
    read_subject,
)
from veridelta.connectors.sql import (
    SQLDialect,
    SQLPushdownCompiler,
    compile_duckdb_sandbox,
    compile_duckdb_select,
)
from veridelta.exceptions import ConfigError, ConnectorError, missing_extra
from veridelta.models import DuckDBConfig

logger = logging.getLogger(__name__)

# The tests patch each module attribute below, so no test depends on the
# extras installed where it runs.
duckdb: Any = optional_module("duckdb")

_DUCKDB_EXTRA = missing_extra("duckdb", "Reading a DuckDB file")
_UNCONNECTED = "DuckDB connector is not connected. Call connect() first."
_SESSION_UNCONNECTED = "DuckDB pushdown session is not connected. Call connect() first."
MOTHERDUCK_TOKEN_VARIABLES: Final = ("MOTHERDUCK_TOKEN", "motherduck_token")
"""Environment variables a MotherDuck token is read from, in order.

MotherDuck's own documentation spells the variable in lowercase. `veridelta
mcp` masks the value of each in its answers, since the connector sends it to
MotherDuck with no `${NAME}` in the configuration.
"""

_READ_STATEMENT_KINDS: Final = ("SELECT", "PRAGMA", "EXPLAIN", "CALL", "MULTI")
"""The `duckdb.StatementType` names a query may end with: the kinds that read rows.

SHOW, DESCRIBE, SUMMARIZE, FROM, and VALUES parse as SELECT. DuckDB 2.0
parses PIVOT as MULTI, the several statements it expands into, so MULTI stays.
"""

_DUCKDB_TYPE: Final = "duckdb:"
"""The prefix that makes DuckDB open a file as a DuckDB database, through no extension.

DuckDB strips it and opens the rest of the text as a path, so it opens the path
that `DuckDBConfig` and the MCP server check, whatever the file holds."""

_NO_HOME: Final = (
    "The DuckDB 'database' starts with ~, and the system names no home folder to expand it "
    "to. Set HOME, or USERPROFILE on Windows, or write the path itself."
)

_TWO_SLASHES: Final = (
    "The DuckDB 'database' starts with //, once ~ is expanded, and DuckDB does not open "
    "such a path as the file it names. Start the path with one /, and write a Windows "
    "share as \\\\server\\share."
)

_NO_EXTENSIONS: Final = {"autoinstall_known_extensions": False, "autoload_known_extensions": False}
"""Settings that stop DuckDB from installing or loading an extension by itself while
it opens a file, such as `httpfs` for a path that starts with a URL's scheme."""

_UNREADABLE_TYPES: Final = frozenset({"interval", "union"})
"""DuckDB types Polars cannot import: it raises on some results and panics on others."""

_NESTED_TYPES: Final = frozenset({"struct", "list", "array", "map"})
"""DuckDB types whose members can hold an unreadable type."""

WRITE_AHEAD_LOGS: Final = (
    ("WAL", ".wal"),
    ("checkpoint WAL", ".wal.checkpoint"),
    ("recovery WAL", ".wal.recovery"),
)
"""Each log DuckDB names beside a database file, with the suffix `beside` adds.

A read-only open replays `.wal`, and `.wal.checkpoint` after a crash during a
checkpoint. Only a read-write open writes `.wal.recovery`, but it is held to
the same rule."""

_SPILL_PREFIX: Final = "veridelta-duckdb-"
"""The start of the name of each folder a connection spills into, in the system's temporary folder."""

_OPEN_WAIT: Final = 60.0
"""Seconds a sandboxed open waits for another to finish before it is refused."""

_DESCRIPTORS: Final = {"linux": "/proc/self/fd", "darwin": "/dev/fd"}.get(sys.platform)
"""The folder that lists this process's open file descriptors, or None where none is known."""

_OPENING: Final = threading.Lock()
"""Held while DuckDB opens a file inside `sandboxed`, so no other such open runs meanwhile.

A descriptor that names the file must be one DuckDB opened then, so no other
sandboxed read's DuckDB connection may open one alongside it. An open that
waits longer than `_OPEN_WAIT` is refused, so a file that never finishes
opening, such as a pipe swapped in for it, blocks no other read for good."""

_FOLDER_FLAGS: Final = (
    getattr(os, "O_PATH", os.O_RDONLY)
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
"""Flags that open a folder, and refuse a link, without reading the folder."""

_LINK_ERRORS: Final = frozenset({errno.ELOOP, errno.ENOTDIR, errno.EMLINK})
"""Errors a system gives when asked to open a link, or a file, as a folder without following it."""

_LINKED: Final = (
    "The DuckDB {named} '{path}' is a link, lies in a folder reached through one, or is "
    "not a file, so it was not opened. Inside the sandbox, a DuckDB file and its logs open "
    "only by a path with no link in it, as the MCP server resolves it."
)

_UNREACHED: Final = (
    "The DuckDB {named} '{path}' could not be looked up, so it was not opened: {reason}."
)

_CHANGED: Final = (
    "DuckDB did not open the DuckDB file '{path}' anew, or the file or a log beside it "
    "changed while DuckDB opened it, so it was not read. A link or a file there was "
    "replaced after it was checked, or another read holds the file open by the same path. "
    "Read it again once no other read holds it."
)

_UNLISTED: Final = (
    "The DuckDB file '{path}' was not read: the server could not list the files it holds "
    "open in {listing}, so it cannot confirm that DuckDB opened the file it checked. Run "
    "the server where {listing} can be read, such as with /proc mounted on Linux."
)

_LEADS_OUT: Final = (
    "The DuckDB file's {named} '{path}' lies outside the folder that holds the file, or is "
    "a link that leads out of it. DuckDB would replay that log, so the file was not opened. "
    "Remove it, or put it and the file in one folder."
)

_BUSY: Final = (
    "The DuckDB file '{path}' was not read: another DuckDB file was still opening after "
    "{seconds:.0f} seconds. Read it again once that read ends, or restart the server if "
    "it never does."
)


@dataclass
class _Sandbox:
    """What a file's connection is held to inside `sandboxed`."""

    folders: tuple[str, ...]
    """The folders it may read other files from."""

    spill: str | None = None
    """The folder it spills into, which the block creates as it opens its first file."""


_sandbox: ContextVar[_Sandbox | None] = ContextVar("veridelta_duckdb_sandbox", default=None)
"""What a file's connection is held to, or None for its own folder."""

_FOLDERS_HOLD: Final = os.path.sep != "\\" and sys.platform not in ("cygwin", "msys")
r"""Whether DuckDB's `allowed_directories` holds a statement to its folders on this system.

On Windows it does not, in DuckDB 1.5. DuckDB reads a drive-relative path with
a lowercase letter, such as `c:..\x.csv`, as a name under the working
directory, so it passes the check. Then it opens the path as Windows reads it,
in the folder above that directory. DuckDB also resolves a share path, which
reaches the machine it names, before it refuses it. Cygwin and MSYS read such
paths as Windows does. A test sets this attribute.
"""


@contextmanager
def sandboxed(directories: Iterable[Path]) -> Generator[None]:
    """Hold each DuckDB file connection opened inside the block to some folders.

    A `query` runs as written, and so does each view stored in the file. A
    connection to a file reads other files, attaches databases, and loads
    extensions only from under the file's own folder, or, inside the block,
    only from under `directories`, whatever its statements ask. On Windows,
    where DuckDB's folder check does not hold, a connection opened inside the
    block opens no other file at all. The MCP server opens every side inside
    this block. A MotherDuck connection is not held, since its extension
    loads and reaches MotherDuck over the network.

    Inside the block, a file and the logs beside it open only by a path with
    no link in it, so hand the path resolved. DuckDB must then hold open the
    file that path named just before, and each log must be as it was, or the
    connection closes and the read fails. A large query spills into a folder
    the block creates in the system's temporary folder, as it opens its first
    file, and removes at its end. On Windows, such a query fails instead.

    Args:
        directories (Iterable[Path]): The folders, resolved.

    Yields:
        None: Control, for the reads to run inside the block.
    """
    sandbox = _Sandbox(tuple(_folder(directory) for directory in directories))
    token = _sandbox.set(sandbox)
    try:
        yield
    finally:
        _sandbox.reset(token)
        if sandbox.spill is not None:
            shutil.rmtree(sandbox.spill, ignore_errors=True)


def beside(path: str, suffix: str) -> str:
    """Add a suffix to a database's path as DuckDB's `AddSuffixToPath` does.

    DuckDB names each write-ahead log and its default temporary folder this way. The
    suffix goes before the first `?` in the path rather than at the end, so
    `L?/x.duckdb` has its log at `L.wal?/x.duckdb`, in another folder.

    Args:
        path (str): The database's path, as DuckDB resolves it.
        suffix (str): Such as `.wal`.

    Returns:
        str: The path with the suffix added.
    """
    mark = path.find("?")
    return f"{path}{suffix}" if mark < 0 else f"{path[:mark]}{suffix}{path[mark:]}"


def _folder(directory: Path) -> str:
    """Return a folder as DuckDB's `allowed_directories` should match it."""
    # DuckDB matches each folder as a prefix, so `/data` alone would admit `/database`.
    return os.path.join(directory, "")


class DuckDBConnector(ReaderConnector):
    """Read one DuckDB or MotherDuck table or query into Polars.

    `connect()` reads eagerly and keeps the frame, and `lazyframe()` hands it to
    the local engine as a LazyFrame over those rows. The read is the one place
    the rows are fetched, so it happens once per `connect()`. The comparison
    runs in Polars, never in DuckDB.
    """

    _unconnected = _UNCONNECTED

    def __init__(self, config: DuckDBConfig, *, probe: bool = False) -> None:
        """Initialize the connector with validated DuckDB settings.

        Args:
            config (DuckDBConfig): Frozen database, token, and table or query.
            probe (bool): Whether to read the table's columns and no rows, for a
                schema check. Only a `table` can be probed.
        """
        self._config = config
        self._probe = probe

    def connect(self) -> None:
        """Read the configured table or query into memory.

        Raises:
            ConnectorError: If the `duckdb` extra is missing, a MotherDuck
                database has no token, a column has no Polars type, or the
                read fails.
            ConfigError: If a probe was asked of a `query`.
        """
        if duckdb is None:
            raise ConnectorError(_DUCKDB_EXTRA)
        statement = self._statement()
        token = _token(self._config)
        started = time.perf_counter()
        try:
            connection, spill = _open(self._config, token)
            try:
                frame = _read(connection, statement, self._subject)
            finally:
                _close(connection, spill)
        except ConnectorError:
            raise
        except ImportError:
            # A missing pyarrow surfaces here, and it is the same missing extra.
            raise ConnectorError(_DUCKDB_EXTRA) from None
        except Exception as exc:
            logger.warning(
                "DuckDB read of %s from %s failed after %.3fs",
                self._subject,
                self._config.database,
                time.perf_counter() - started,
            )
            raise ConnectorError(
                f"DuckDB read of {self._subject} from '{self._config.database}' failed: "
                f"{mask_secrets(str(exc), token)}",
                quotes_data=True,
            ) from None
        logger.info(
            "Read %d rows of %s from %s in %.3fs",
            frame.height,
            self._subject,
            self._config.database,
            time.perf_counter() - started,
        )
        self._frame = frame.lazy()

    def _statement(self) -> str:
        """Return the SQL to read: the compiled table select, or the query as written."""
        if self._config.table is not None:
            return compile_duckdb_select(self._config.table, probe=self._probe)
        if self._probe:
            raise ConfigError(PROBE_NEEDS_A_TABLE)
        # DuckDBConfig requires exactly one of `table` and `query`.
        return cast("str", self._config.query)

    @property
    def _subject(self) -> str:
        """Name what is read without repeating any SQL."""
        return read_subject(self._config.table)


class DuckDBPushdownSession(PushdownSession):
    """Run compiled comparison SQL inside DuckDB or MotherDuck.

    Opened for two tables in one database that both set `pushdown`. It holds
    one connection from `connect()` to `close()`, opened as a read opens it:
    a file read-only, MotherDuck read-write with its token, both in UTC. Only
    counts and keys come back, and a row sample when one is asked for.
    """

    def __init__(self, config: DuckDBConfig) -> None:
        """Initialize the session for one side's connection settings.

        Args:
            config (DuckDBConfig): A DuckDB table that sets `pushdown`.
        """
        self._config = config
        self.compiler = SQLPushdownCompiler(SQLDialect.DUCKDB)
        self._connection: Any = None
        self._spill: str | None = None
        self._token: str | None = None

    def connect(self) -> None:
        """Open the database for the statements to come.

        Raises:
            ConnectorError: If the `duckdb` extra is missing, a MotherDuck
                database has no token, or the database cannot be opened.
        """
        if duckdb is None:
            raise ConnectorError(_DUCKDB_EXTRA)
        token = _token(self._config)
        try:
            connection, spill = _open(self._config, token)
        except Exception as exc:
            logger.warning("DuckDB connection to %s failed", self._config.database)
            raise ConnectorError(
                f"DuckDB connection to '{self._config.database}' failed: "
                f"{mask_secrets(str(exc), token)}"
            ) from None
        logger.info("Connected to DuckDB database %s", self._config.database)
        self._connection, self._spill, self._token = connection, spill, token

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Run one compiled statement and return its rows lazily.

        Args:
            statement (str): SQL from the DuckDB compiler.
            query_type (PushdownQueryType): Which round-trip this is, for logs
                and errors.

        Returns:
            pl.LazyFrame: The statement's result.

        Raises:
            ConnectorError: If the session is not connected, a result column
                has no Polars type, or the statement fails.
        """
        frame = self._run(statement, query_type)
        return frame.lazy()

    def close(self) -> None:
        """Close the connection. Idempotent; `connect()` opens it again."""
        if self._connection is not None:
            _close(self._connection, self._spill)
            logger.info("Closed DuckDB database %s", self._config.database)
        self._connection = self._spill = None

    def _run(self, statement: str, query_type: str) -> pl.DataFrame:
        """Run a statement on the open connection, logging and reporting without secrets."""
        if self._connection is None:
            raise ConnectorError(_SESSION_UNCONNECTED)
        started = time.perf_counter()
        try:
            frame = _read(self._connection, statement, f"the {query_type} statement")
        except ConnectorError:
            raise
        except ImportError:
            raise ConnectorError(_DUCKDB_EXTRA) from None
        except Exception as exc:
            logger.warning(
                "DuckDB %s statement on %s failed after %.3fs",
                query_type,
                self._config.database,
                time.perf_counter() - started,
            )
            raise ConnectorError(
                f"DuckDB {query_type} statement on '{self._config.database}' failed: "
                f"{mask_secrets(str(exc), self._token)}",
                quotes_data=True,
            ) from None
        logger.info(
            "Ran DuckDB %s statement on %s in %.3fs, %d rows",
            query_type,
            self._config.database,
            time.perf_counter() - started,
            frame.height,
        )
        return frame


def _token(config: DuckDBConfig) -> str | None:
    """Return the MotherDuck token to connect with: the field's, then the environment's."""
    if not config.is_motherduck:
        return None
    token = config.motherduck_token or next(
        (os.environ[name] for name in MOTHERDUCK_TOKEN_VARIABLES if os.environ.get(name)), None
    )
    if not token:
        # Without one, MotherDuck opens a browser sign-in, which a CI job never finishes.
        raise ConnectorError(
            f"MotherDuck database '{config.database}' needs a token. Set "
            "'motherduck_token', or the MOTHERDUCK_TOKEN environment variable."
        )
    return token


def _open(config: DuckDBConfig, token: str | None) -> tuple[Any, str | None]:
    """Open a file read-only, or a MotherDuck database with its token, to read in UTC.

    A file is held to its own folder, or inside `sandboxed` to the folders
    that names, before any statement of the caller's or any view in the file
    runs, with persistent secrets off. On Windows, DuckDB's folder check lets
    a lowercase drive-relative path out and resolves a share path, so outside
    `sandboxed` a file from someone else is code there. Inside `sandboxed` on
    such a system, the file opens no other file at all. A connection whose
    setup fails is closed.

    A file opens as a DuckDB database, whatever its first bytes say, with no
    extension loading by itself. Otherwise DuckDB reads a SQLite file's header
    as a reason to install and load the extension that opens it, before any
    setting of the sandbox applies.

    A leading `~` is expanded here, with the function and text the MCP
    server's check expands, so DuckDB never does. DuckDB reads `~` as `HOME`
    alone, which may be unset where the system still names a home folder.

    DuckDB would spill into `duckdb:` and the path, with `.tmp` added, from
    the working directory, and a folder beside the file can arrive holding a
    link under the name of a spill file. So the temporary folder is set to
    one this process creates: for this connection alone, or inside
    `sandboxed`, the folder the block created.

    Returns:
        tuple[Any, str | None]: The connection, and the folder to remove once
            it closes, which `_close` does, or None for none.

    Raises:
        ConnectorError: If the path still starts with `~`, since the system
            names no home folder, or starts with `//`, once `~` is expanded.
            DuckDB strips no type from `duckdb://`, so it would open a file
            whose name starts with `duckdb:` instead, by its first bytes.
            Also if a log lies outside the file's folder or leads out of it,
            or inside `sandboxed`, if the path holds a link or DuckDB opened
            another file than the path named.
    """
    setup = ["SET TimeZone = 'UTC'"]
    spill: str | None = None
    if token is None:
        path = os.path.expanduser(config.database)
        if path.startswith("~"):
            raise ConnectorError(_NO_HOME)
        if path.startswith("//"):
            raise ConnectorError(_TWO_SLASHES)
        sandbox = _sandbox.get()
        if sandbox is None:
            # A view in the file runs as its author wrote it, so it reads only beside the file.
            resolved = Path(path).resolve()
            _kept_beside(resolved)
            spill = tempfile.mkdtemp(prefix=_SPILL_PREFIX)
            try:
                connection = _connect(path, spill)
            except BaseException:
                shutil.rmtree(spill, ignore_errors=True)
                raise
            folders: tuple[str, ...] = (_folder(resolved.parent),)
        else:
            connection = _open_checked(path, sandbox)
            folders = sandbox.folders
        setup += compile_duckdb_sandbox(folders, other_files=sandbox is None or _FOLDERS_HOLD)
    else:
        # A read-only MotherDuck connection needs a read-scaling token, so it opens read-write.
        connection = duckdb.connect(config.database, config={"motherduck_token": token})
    try:
        for statement in setup:
            connection.execute(statement)
    except BaseException:
        _close(connection, spill)
        raise
    return connection, spill


def _close(connection: Any, spill: str | None) -> None:
    """Close a connection, then remove the folder it spilled into, if it has its own."""
    try:
        connection.close()
    finally:
        if spill is not None:
            shutil.rmtree(spill, ignore_errors=True)


def _connect(path: str, spill: str) -> Any:
    """Open a file read-only as a DuckDB database, with no extension, spilling into `spill`."""
    settings = {**_NO_EXTENSIONS, "temp_directory": spill}
    return duckdb.connect(f"{_DUCKDB_TYPE}{path}", read_only=True, config=settings)


def _kept_beside(resolved: Path) -> None:
    """Refuse a log of a file that lies outside its folder, or leads out of it.

    DuckDB replays a log as it opens the file, before any setting of the
    sandbox applies. A link that stays in the file's folder is followed, and
    a path that names nothing is left alone, even where a `?` in the file's
    path makes `beside` name a log in another folder.

    Args:
        resolved (Path): The file, resolved as DuckDB resolves it.

    Raises:
        ConnectorError: If a log lies outside the folder or leads out of it.
    """
    for name, suffix in WRITE_AHEAD_LOGS:
        path = beside(str(resolved), suffix)
        if os.path.lexists(path) and not Path(os.path.realpath(path)).is_relative_to(
            resolved.parent
        ):
            raise ConnectorError(_LEADS_OUT.format(named=name, path=path))


def _open_checked(path: str, sandbox: _Sandbox) -> Any:
    """Open a file by a path with no link in it, and confirm DuckDB opened the file it named.

    The file and each log are looked up first, through no link. DuckDB opens
    the path again by its text, so a folder swapped for a link in between
    would lead it to another file. Where the system lists this process's
    open descriptors, as Linux and macOS do, a descriptor that was not open
    before `duckdb.connect` must name the file looked up, so one that another
    read holds does not count. `_OPENING` keeps any other read's DuckDB
    connection from opening one meanwhile. Each log, and the file, must then
    still be as they were.

    Raises:
        ConnectorError: If the file or a log is reached through a link, the
            file does not exist, DuckDB opened no new descriptor of the file
            the path named, a log changed while DuckDB opened the file, the
            system's list of descriptors cannot be read, or another sandboxed
            open did not finish within `_OPEN_WAIT`.
    """
    paths = [path, *(beside(path, suffix) for _, suffix in WRITE_AHEAD_LOGS)]
    names = ["file", *(f"file's {name}" for name, _ in WRITE_AHEAD_LOGS)]
    if not _OPENING.acquire(timeout=_OPEN_WAIT):
        raise ConnectorError(_BUSY.format(path=path, seconds=_OPEN_WAIT))
    try:
        before = [_identity(each, name) for each, name in zip(paths, names, strict=True)]
        if before[0] is None:
            raise ConnectorError(f"The DuckDB file '{path}' does not exist, so it was not opened.")
        held = _holding(before[0], path)
        if sandbox.spill is None:
            sandbox.spill = tempfile.mkdtemp(prefix=_SPILL_PREFIX)
        connection = _connect(path, sandbox.spill)
        try:
            after = [_identity(each, name) for each, name in zip(paths, names, strict=True)]
            opened = _holding(before[0], path)
            if after != before or (held is not None and not cast("set[int]", opened) - held):
                raise ConnectorError(_CHANGED.format(path=path))
        except BaseException:
            connection.close()
            raise
    finally:
        _OPENING.release()
    return connection


def _identity(path: str, named: str) -> tuple[int, int] | None:
    """Return the device and inode of the file a path names through no link, or None for none.

    Raises:
        ConnectorError: If a part of the path is a link, or the path names
            something other than a file.
    """
    linked = ConnectorError(_LINKED.format(named=named, path=path))
    try:
        status = _looked_up(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        if exc.errno in _LINK_ERRORS:
            raise linked from None
        reason = exc.strerror or type(exc).__name__
        raise ConnectorError(_UNREACHED.format(named=named, path=path, reason=reason)) from None
    if not stat.S_ISREG(status.st_mode) or not _resolves_to_itself(path):
        raise linked
    return status.st_dev, status.st_ino


def _looked_up(path: str) -> os.stat_result:
    """Look a file up without following a link.

    On a system that looks a name up in an open folder, as Linux and macOS
    do, each folder on the path is opened in the one before it, refusing a
    link, so no folder can change between two steps. Elsewhere, as on
    Windows, the path's last part is looked up without following a link, and
    `_resolves_to_itself` checks the folders before it.
    """
    if os.stat not in os.supports_dir_fd:
        return os.stat(path, follow_symlinks=False)
    head, name = os.path.split(path)
    folder = os.open(os.sep if os.path.isabs(path) else os.curdir, _FOLDER_FLAGS)
    try:
        for part in head.split(os.sep):
            if part:
                inner = os.open(part, _FOLDER_FLAGS, dir_fd=folder)
                os.close(folder)
                folder = inner
        return os.stat(name, dir_fd=folder, follow_symlinks=False)
    finally:
        os.close(folder)


def _resolves_to_itself(path: str) -> bool:
    """Whether a path holds no link, where `_looked_up` could not refuse one in a folder."""
    if os.stat in os.supports_dir_fd:
        return True
    return os.path.normcase(os.path.realpath(path)) == os.path.normcase(os.path.abspath(path))


def _holding(identity: tuple[int, int], path: str) -> set[int] | None:
    """Return this process's descriptors that name a file, or None where the system lists none.

    DuckDB holds the database file open while its connection is, so a file
    that no new descriptor names is one DuckDB did not open. The file is
    never opened and closed here, since closing any descriptor of a file
    releases the lock DuckDB holds on it.

    Raises:
        ConnectorError: If the system's list of descriptors cannot be read,
            as on Linux without /proc mounted.
    """
    if _DESCRIPTORS is None:
        return None
    try:
        names = os.listdir(_DESCRIPTORS)
    except OSError:
        raise ConnectorError(_UNLISTED.format(path=path, listing=_DESCRIPTORS)) from None
    return {int(name) for name in names if _described(name) == identity}


def _described(name: str) -> tuple[int, int] | None:
    """Return the device and inode of the file an open descriptor names, or None for one closed."""
    try:
        status = os.fstat(int(name))
    except OSError:
        return None
    return status.st_dev, status.st_ino


def _read(connection: Any, statement: str, subject: str) -> pl.DataFrame:
    """Run the statement and convert its result, refusing a column Polars cannot hold.

    A statement whose last part returns no result, such as SET, or does not
    read, such as INSERT, is refused before it runs.
    """
    no_result = ConnectorError(
        "The configured query returned no rows to compare. Write a statement that "
        "returns rows, such as a SELECT."
    )
    parsed_statements = duckdb.extract_statements(statement)
    if parsed_statements:
        last_statement = parsed_statements[-1]
        expected_result_types = list(last_statement.expected_result_type)
        if expected_result_types == [duckdb.ExpectedResultType.NOTHING]:
            raise no_result
        if not _reads_rows(last_statement):
            kind_name = last_statement.type.name
            article = "an" if kind_name[:1] in "AEIOU" else "a"
            raise ConnectorError(
                f"The configured query ends with {article} {kind_name} statement, which "
                "Veridelta does not run: a source is read, never written. Write a query "
                "whose last statement reads rows, such as a SELECT."
            )
    relation = connection.sql(statement)
    if relation is None:
        # DuckDB 1.x answers any other statement without a result, such as a plain
        # INSERT, with no relation. A table read always selects.
        raise no_result
    for name, kind in zip(relation.columns, relation.types, strict=True):
        if _unreadable(kind):
            raise ConnectorError(
                f"Column '{name}' of {subject} holds {kind}, which Polars cannot read. "
                "Cast it in a view or a 'query', such as to VARCHAR."
            )
    return cast("pl.DataFrame", relation.pl())


def _reads_rows(parsed_statement: Any) -> bool:
    """Return whether DuckDB parses a statement as one of the kinds that read rows.

    The parser expects rows from an INSERT, an UPDATE, or a DELETE too, since
    each may carry RETURNING, so only the statement's kind tells a write apart.
    """
    read_kinds = {getattr(duckdb.StatementType, name) for name in _READ_STATEMENT_KINDS}
    return parsed_statement.type in read_kinds


def _unreadable(kind: Any) -> bool:
    """Return whether a DuckDB type, or one nested in it, has no Polars form."""
    if kind.id in _UNREADABLE_TYPES:
        return True
    if kind.id not in _NESTED_TYPES:
        return False
    # An array's members include its size, which is a number rather than a type.
    return any(_unreadable(member) for _, member in kind.children if hasattr(member, "id"))
