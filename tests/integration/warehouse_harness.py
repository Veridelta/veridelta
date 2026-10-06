# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Run the parity harness against a live warehouse instead of DuckDB.

Selected by `VERIDELTA_PARITY_BACKEND` set to `bigquery`, `databricks`,
`motherduck`, or `snowflake`, with that service's settings in the environment.
CONTRIBUTING lists them under "Live warehouse tests", and `make live` runs the
suite. The `live.yml` workflow runs it in CI, after the maintainer approves.

Each comparison loads its two frames into fresh tables and drops them after.
Snowflake and Databricks take parameterized `INSERT` statements, BigQuery a
Parquet load job, since its free sandbox refuses DML, and MotherDuck an Arrow
table. A table's name holds the minute it was made, as in
`vd_202610061200_1a2b3c4d_source`, so `drop_stale_tables` can drop the tables
a failed run left behind once they are a day old.

Pushdown goes through `DiffEngine.run_from_configs`, as `veridelta run` does,
so the shipped routing and connector run. The harness wraps the connector's
`execute_pushdown` only to record each statement for failure messages. The
local run reads both tables back through the same connector, so the local
engine sees the values as the warehouse stores them.

Each dtype is stored as the nearest type the warehouse has. A dtype with no
type that holds its values exactly raises `NotImplementedError`, as in the
Postgres backend, and a case that needs one carries a `skip_on` marker.

`MOTHERDUCK_DATABASE` may name a local DuckDB file instead of `md:veridelta_live`.
The MotherDuck backend then runs offline, which checks the shared code here
without an account.
"""

from __future__ import annotations

import importlib
import io
import math
import os
import re
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final
from unittest import mock

import polars as pl

from veridelta.connectors.duckdb import DuckDBPushdownSession
from veridelta.connectors.warehouse import (
    BigQueryConnector,
    DatabricksConnector,
    SnowflakeConnector,
    _snowflake_credentials,
)
from veridelta.engine import DiffEngine
from veridelta.models import BigQueryConfig, DatabricksConfig, DuckDBConfig, SnowflakeConfig

if TYPE_CHECKING:
    from collections.abc import Iterator

    from veridelta.connectors.base import PushdownQueryType
    from veridelta.models import DiffConfig, DiffResult, SourceRef, ValueMapProposal

SCHEMA: Final = "veridelta_live"
"""The BigQuery dataset, Databricks schema, and MotherDuck database the tables go in."""

STALE_AFTER: Final = timedelta(days=1)
"""Age past which `drop_stale_tables` drops a table the harness made."""

_NAME = re.compile(r"vd_(\d{12})_[0-9a-f]{8}_(?:source|target)")
"""A table name the harness made, with the minute it was made."""

_STAMP: Final = "%Y%m%d%H%M"

_INSERT_PARAMETERS: Final = 250
"""Most parameters in one Databricks `INSERT`, which carries a batch of rows."""


def _setting(name: str) -> str:
    """Return a required setting from the environment.

    Args:
        name (str): The environment variable.

    Returns:
        str: Its value.

    Raises:
        RuntimeError: If it is unset or empty.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Set {name} to run the parity suite against a live warehouse; "
            "see 'Live warehouse tests' in CONTRIBUTING.md."
        )
    return value


def _optional(name: str) -> str | None:
    """Return an optional setting from the environment, or None when unset or empty."""
    return os.environ.get(name) or None


def _double_quoted(name: str) -> str:
    """Quote a Snowflake or DuckDB identifier, doubling any double quote inside it."""
    return '"' + name.replace('"', '""') + '"'


def _backticked(name: str) -> str:
    """Quote a Databricks identifier, doubling any backtick inside it."""
    return "`" + name.replace("`", "``") + "`"


def _unstorable(service: str, name: str, dtype: pl.DataType) -> NotImplementedError:
    """Build the error for a column the service has no exact type for."""
    return NotImplementedError(f"{service} has no column type for {name!r}, a {dtype} column.")


class _Service(ABC):
    """One live warehouse: its settings, how it stores a frame, and how it clears tables."""

    name: ClassVar[str]
    """The value of `VERIDELTA_PARITY_BACKEND` that selects it."""

    refused_rules: ClassVar[frozenset[str]] = frozenset()
    """Rule fields its pushdown refuses with a `ConfigError` before any query."""

    connector: ClassVar[type[Any]]
    """The shipped connector a `pushdown` pair on it opens."""

    @abstractmethod
    def source(self, table: str) -> SourceRef:
        """Describe one loaded table as a source a configuration file would declare."""

    @abstractmethod
    def connect(self) -> Any:
        """Open a driver connection for creating, filling, listing, and dropping tables."""

    @abstractmethod
    def load(self, connection: Any, table: str, frame: pl.DataFrame) -> None:
        """Create a table shaped like the frame and fill it with the frame's rows."""

    @abstractmethod
    def drop(self, connection: Any, table: str) -> None:
        """Drop a table, if it exists."""

    @abstractmethod
    def tables(self, connection: Any) -> list[str]:
        """List the tables in the test schema whose names start with `vd_`."""

    def close(self, connection: Any) -> None:
        """Close a connection `connect` opened."""
        connection.close()


class _MotherDuck(_Service):
    """MotherDuck, or a local DuckDB file standing in for it."""

    name = "motherduck"
    refused_rules = frozenset({"max_levenshtein_distance"})
    connector = DuckDBPushdownSession

    def __init__(self) -> None:
        """Read the database to use, and the token when it is in MotherDuck."""
        self.database = os.environ.get("MOTHERDUCK_DATABASE", f"md:{SCHEMA}")
        config = DuckDBConfig(database=self.database, table="vd", pushdown=True)
        # The session reads MOTHERDUCK_TOKEN itself; the loader needs it too.
        self.token = _setting("MOTHERDUCK_TOKEN") if config.is_motherduck else None

    def source(self, table: str) -> SourceRef:
        """Describe a loaded table as a DuckDB source that sets `pushdown`."""
        return DuckDBConfig(database=self.database, table=table, pushdown=True)

    def connect(self) -> Any:
        """Open the database read-write, as the loader must."""
        import duckdb

        if self.token is None:
            return duckdb.connect(self.database)
        return duckdb.connect(self.database, config={"motherduck_token": self.token})

    def load(self, connection: Any, table: str, frame: pl.DataFrame) -> None:
        """Copy the frame's Arrow table into a new table, types and all."""
        connection.register("vd_frame", frame.to_arrow())
        try:
            connection.execute(f"CREATE TABLE {_double_quoted(table)} AS SELECT * FROM vd_frame")
        finally:
            connection.unregister("vd_frame")

    def drop(self, connection: Any, table: str) -> None:
        """Drop the table."""
        connection.execute(f"DROP TABLE IF EXISTS {_double_quoted(table)}")

    def tables(self, connection: Any) -> list[str]:
        """List this database's `vd_` tables; MotherDuck also lists the account's others."""
        rows = connection.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = current_database() AND starts_with(table_name, 'vd_')"
        ).fetchall()
        return [row[0] for row in rows]


_SNOWFLAKE_TYPES: Final[dict[type[pl.DataType], str]] = {
    pl.Int8: "NUMBER(3, 0)",
    pl.Int16: "NUMBER(5, 0)",
    pl.Int32: "NUMBER(10, 0)",
    pl.Int64: "NUMBER(19, 0)",
    pl.UInt8: "NUMBER(3, 0)",
    pl.UInt16: "NUMBER(5, 0)",
    pl.UInt32: "NUMBER(10, 0)",
    pl.UInt64: "NUMBER(20, 0)",
    pl.Float32: "FLOAT",
    pl.Float64: "FLOAT",
    pl.String: "VARCHAR",
    pl.Boolean: "BOOLEAN",
    pl.Date: "DATE",
}
"""Snowflake column type for each dtype that maps without a parameter."""


def _snowflake_value(value: object) -> object:
    """Return a value the Snowflake driver can write into an `INSERT`.

    The driver writes a float as its `repr`, which for NaN and the infinities is
    not SQL. Snowflake reads each from text, which the driver quotes.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return "NaN" if math.isnan(value) else ("inf" if value > 0 else "-inf")
    return value


class _Snowflake(_Service):
    """Snowflake, signed in with a key pair."""

    name = "snowflake"
    connector = SnowflakeConnector

    def __init__(self) -> None:
        """Read the account settings; the key file holds the private key."""
        self.config = SnowflakeConfig(
            table="vd",
            account=_setting("SNOWFLAKE_ACCOUNT"),
            user=_setting("SNOWFLAKE_USER"),
            warehouse=_setting("SNOWFLAKE_WAREHOUSE"),
            database=_setting("SNOWFLAKE_DATABASE"),
            schema_name="PUBLIC",
            role=_optional("SNOWFLAKE_ROLE"),
            private_key_path=_setting("SNOWFLAKE_PRIVATE_KEY_PATH"),
            private_key_passphrase=_optional("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        )

    def source(self, table: str) -> SourceRef:
        """Describe a loaded table as a Snowflake source."""
        return self.config.model_copy(update={"table": table})

    def connect(self) -> Any:
        """Sign in as the shipped connector does."""
        import snowflake.connector

        config = self.config
        return snowflake.connector.connect(
            account=config.account,
            user=config.user,
            warehouse=config.warehouse,
            database=config.database,
            schema=config.schema_name,
            role=config.role,
            **_snowflake_credentials(config),
        )

    def load(self, connection: Any, table: str, frame: pl.DataFrame) -> None:
        """Create the table with quoted names, which keep their case, and insert the rows.

        The compiled SQL quotes every name, and a quoted Snowflake name matches
        only its exact case. The driver turns the rows into one `INSERT`.
        """
        columns = ", ".join(
            f"{_double_quoted(name)} {self._column_type(name, dtype)}"
            for name, dtype in frame.schema.items()
        )
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {_double_quoted(table)} ({columns})")
            if frame.height:
                marks = ", ".join(["%s"] * frame.width)
                rows = [tuple(_snowflake_value(v) for v in row) for row in frame.iter_rows()]
                cursor.executemany(f"INSERT INTO {_double_quoted(table)} VALUES ({marks})", rows)

    def drop(self, connection: Any, table: str) -> None:
        """Drop the table."""
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {_double_quoted(table)}")

    def tables(self, connection: Any) -> list[str]:
        """List the schema's `vd_` tables."""
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = CURRENT_SCHEMA() AND STARTSWITH(table_name, 'vd_')"
            )
            return [row[0] for row in cursor.fetchall()]

    def _column_type(self, name: str, dtype: pl.DataType) -> str:
        """Return the Snowflake type that stores a column without loss."""
        if isinstance(dtype, pl.Decimal):
            return f"NUMBER({dtype.precision or 38}, {dtype.scale})"
        if isinstance(dtype, pl.Datetime):
            digits = 9 if dtype.time_unit == "ns" else 6
            kind = "TIMESTAMP_NTZ" if dtype.time_zone is None else "TIMESTAMP_TZ"
            return f"{kind}({digits})"
        column_type = _SNOWFLAKE_TYPES.get(type(dtype))
        if column_type is None:
            raise _unstorable("Snowflake", name, dtype)
        return column_type


_DATABRICKS_TYPES: Final[dict[type[pl.DataType], str]] = {
    pl.Int8: "TINYINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INT",
    pl.Int64: "BIGINT",
    pl.UInt8: "SMALLINT",
    pl.UInt16: "INT",
    pl.UInt32: "BIGINT",
    pl.UInt64: "DECIMAL(20, 0)",
    pl.Float32: "FLOAT",
    pl.Float64: "DOUBLE",
    pl.String: "STRING",
    pl.Boolean: "BOOLEAN",
    pl.Date: "DATE",
}
"""Databricks column type for each dtype that maps without a parameter."""


class _Databricks(_Service):
    """Databricks SQL, in the `workspace` catalog of a Free Edition workspace."""

    name = "databricks"
    connector = DatabricksConnector

    def __init__(self) -> None:
        """Read the workspace settings."""
        self.config = DatabricksConfig(
            table="vd",
            server_hostname=_setting("DATABRICKS_HOST"),
            http_path=_setting("DATABRICKS_HTTP_PATH"),
            access_token=_setting("DATABRICKS_TOKEN"),
            catalog="workspace",
            schema_name=SCHEMA,
        )

    def source(self, table: str) -> SourceRef:
        """Describe a loaded table as a Databricks source."""
        return self.config.model_copy(update={"table": table})

    def connect(self) -> Any:
        """Open a session as the shipped connector does."""
        from databricks import sql

        config = self.config
        return sql.connect(
            server_hostname=config.server_hostname,
            http_path=config.http_path,
            access_token=config.access_token,
            catalog=config.catalog,
            schema=config.schema_name,
        )

    def load(self, connection: Any, table: str, frame: pl.DataFrame) -> None:
        """Create the table and insert the rows, a batch per statement.

        The driver's `executemany` sends one request a row, so each statement
        carries as many rows as fit in its parameter budget.
        """
        from databricks.sql.parameters import native

        columns = ", ".join(
            f"{_backticked(name)} {self._column_type(name, dtype)}"
            for name, dtype in frame.schema.items()
        )
        batch = max(1, _INSERT_PARAMETERS // max(frame.width, 1))
        rows = list(frame.iter_rows())
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {_backticked(table)} ({columns})")
            for start in range(0, len(rows), batch):
                groups: list[str] = []
                parameters: list[Any] = []
                for row in rows[start : start + batch]:
                    groups.append("(" + ", ".join(["?"] * len(row)) + ")")
                    parameters.extend(self._parameter(value, native) for value in row)
                cursor.execute(
                    f"INSERT INTO {_backticked(table)} VALUES {', '.join(groups)}", parameters
                )

    def drop(self, connection: Any, table: str) -> None:
        """Drop the table."""
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {_backticked(table)}")

    def tables(self, connection: Any) -> list[str]:
        """List the schema's `vd_` tables."""
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'vd_*'")
            return [row[1] for row in cursor.fetchall()]

    @staticmethod
    def _parameter(value: object, native: Any) -> Any:
        """Bind a value, keeping a naive datetime naive.

        The driver binds every `datetime` as `TIMESTAMP`, which reads a naive one
        in the session's time zone.
        """
        if isinstance(value, datetime) and value.tzinfo is None:
            return native.TimestampNTZParameter(value=value)
        return native.dbsql_parameter_from_primitive(value)

    def _column_type(self, name: str, dtype: pl.DataType) -> str:
        """Return the Databricks type that stores a column without loss."""
        if isinstance(dtype, pl.Decimal):
            return f"DECIMAL({dtype.precision or 38}, {dtype.scale})"
        if isinstance(dtype, pl.Datetime) and dtype.time_unit != "ns":
            return "TIMESTAMP_NTZ" if dtype.time_zone is None else "TIMESTAMP"
        column_type = _DATABRICKS_TYPES.get(type(dtype))
        if column_type is None:
            raise _unstorable("Databricks", name, dtype)
        return column_type


_BIGQUERY_TYPES: Final[dict[type[pl.DataType], str]] = {
    pl.Int8: "INT64",
    pl.Int16: "INT64",
    pl.Int32: "INT64",
    pl.Int64: "INT64",
    pl.UInt8: "INT64",
    pl.UInt16: "INT64",
    pl.UInt32: "INT64",
    pl.Float32: "FLOAT64",
    pl.Float64: "FLOAT64",
    pl.String: "STRING",
    pl.Boolean: "BOOL",
    pl.Date: "DATE",
}
"""BigQuery column type for each dtype that maps without a parameter."""

_BIGQUERY_BYTES_BILLED: Final = 10**9
"""Most bytes one query may bill, so a runaway statement costs little of the sandbox's 1 TiB."""


class _BigQuery(_Service):
    """BigQuery, in a sandbox project or any other."""

    name = "bigquery"
    connector = BigQueryConnector

    def __init__(self) -> None:
        """Read the project, and the key file unless default credentials apply."""
        self.config = BigQueryConfig(
            table="vd",
            project=_setting("BQ_PROJECT"),
            dataset=SCHEMA,
            location=_optional("BQ_LOCATION"),
            credentials_path=_optional("BQ_CREDENTIALS_PATH"),
            maximum_bytes_billed=_BIGQUERY_BYTES_BILLED,
        )

    def source(self, table: str) -> SourceRef:
        """Describe a loaded table as a BigQuery source."""
        return self.config.model_copy(update={"table": table})

    def connect(self) -> Any:
        """Create a client as the shipped connector does."""
        bigquery: Any = importlib.import_module("google.cloud.bigquery")
        config = self.config
        if config.credentials_path is None:
            return bigquery.Client(project=config.project, location=config.location)
        return bigquery.Client.from_service_account_json(
            config.credentials_path, project=config.project, location=config.location
        )

    def load(self, connection: Any, table: str, frame: pl.DataFrame) -> None:
        """Load the frame as Parquet into a table with an explicit schema.

        The sandbox refuses `INSERT`, so a load job writes the rows. Unsigned
        integers are written as the signed or decimal type the schema names,
        since BigQuery reads no unsigned Parquet type.
        """
        bigquery: Any = importlib.import_module("google.cloud.bigquery")
        fields, columns = [], []
        for name, dtype in frame.schema.items():
            kind, precision, scale, cast = self._column_type(name, dtype)
            digits = {} if precision is None else {"precision": precision, "scale": scale}
            fields.append(bigquery.SchemaField(name, kind, **digits))
            columns.append(pl.col(name) if cast is None else pl.col(name).cast(cast))
        buffer = io.BytesIO()
        frame.select(columns).write_parquet(buffer)
        buffer.seek(0)
        job = connection.load_table_from_file(
            buffer,
            self._reference(table),
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET, schema=fields
            ),
        )
        job.result()

    def drop(self, connection: Any, table: str) -> None:
        """Drop the table."""
        connection.delete_table(self._reference(table), not_found_ok=True)

    def tables(self, connection: Any) -> list[str]:
        """List the dataset's `vd_` tables."""
        dataset = f"{self.config.project}.{SCHEMA}"
        names = (item.table_id for item in connection.list_tables(dataset))
        return [name for name in names if name.startswith("vd_")]

    def _reference(self, table: str) -> str:
        """Return the table's full name."""
        return f"{self.config.project}.{SCHEMA}.{table}"

    def _column_type(
        self, name: str, dtype: pl.DataType
    ) -> tuple[str, int | None, int | None, pl.DataType | None]:
        """Return the BigQuery type, its precision and scale, and the dtype to write it as."""
        if isinstance(dtype, pl.Decimal):
            precision, scale = dtype.precision or 38, dtype.scale
            # NUMERIC holds 29 digits before the point and 9 after; BIGNUMERIC 38 and 38.
            kind = "NUMERIC" if scale <= 9 and precision - scale <= 29 else "BIGNUMERIC"
            return kind, precision, scale, None
        if isinstance(dtype, pl.UInt64):
            return "NUMERIC", 20, 0, pl.Decimal(20, 0)
        if isinstance(dtype, pl.Datetime) and dtype.time_unit != "ns":
            return ("DATETIME" if dtype.time_zone is None else "TIMESTAMP"), None, None, None
        mapped = _BIGQUERY_TYPES.get(type(dtype))
        if mapped is None:
            raise _unstorable("BigQuery", name, dtype)
        unsigned = isinstance(dtype, pl.UInt8 | pl.UInt16 | pl.UInt32)
        return mapped, None, None, (pl.Int64() if unsigned else None)


SERVICES: Final[dict[str, type[_Service]]] = {
    service.name: service for service in (_BigQuery, _Databricks, _MotherDuck, _Snowflake)
}
"""Each live warehouse by the `VERIDELTA_PARITY_BACKEND` value that selects it."""


def _table_names(now: datetime) -> tuple[str, str]:
    """Name a fresh source and target table, stamped with the minute."""
    stem = f"vd_{now.strftime(_STAMP)}_{uuid.uuid4().hex[:8]}"
    return f"{stem}_source", f"{stem}_target"


def is_stale(table: str, now: datetime) -> bool:
    """Return whether a table is one the harness made more than a day before `now`.

    Args:
        table (str): A table name.
        now (datetime): The current time, in UTC.

    Returns:
        bool: True for a harness table older than `STALE_AFTER`. Any other
            name, `vd_` or not, is never stale, so no one's table is dropped.
    """
    match = _NAME.fullmatch(table)
    if match is None:
        return False
    made = datetime.strptime(match.group(1), _STAMP).replace(tzinfo=UTC)
    return now - made > STALE_AFTER


@contextmanager
def _recorded(connector: type[Any]) -> Iterator[list[str]]:
    """Record each statement the connector executes while the block runs.

    Args:
        connector (type[Any]): A shipped connector class.

    Yields:
        list[str]: The statements, in execution order.
    """
    statements: list[str] = []
    execute = connector.execute_pushdown

    def recording(
        self: Any, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        statements.append(statement)
        result: pl.LazyFrame = execute(self, statement, query_type)
        return result

    with mock.patch.object(connector, "execute_pushdown", recording):
        yield statements


class WarehouseBackend:
    """The parity backend for one live warehouse."""

    def __init__(self, service: _Service) -> None:
        """Wrap a service.

        Args:
            service (_Service): The warehouse to compare in.
        """
        self.service = service
        self.refused_rules = service.refused_rules

    @contextmanager
    def loaded_tables(
        self, source: pl.DataFrame, target: pl.DataFrame
    ) -> Iterator[tuple[str, str]]:
        """Load both frames into fresh tables and drop them afterwards.

        The loading connection closes before the comparison opens its own, since
        a local DuckDB file opens read-only only when nothing else holds it.

        Args:
            source (pl.DataFrame): Rows for the source table.
            target (pl.DataFrame): Rows for the target table.

        Yields:
            tuple[str, str]: The source and target table names.
        """
        names = _table_names(datetime.now(UTC))
        try:
            connection = self.service.connect()
            try:
                self.service.load(connection, names[0], source)
                self.service.load(connection, names[1], target)
            finally:
                self.service.close(connection)
            yield names
        finally:
            connection = self.service.connect()
            try:
                for name in names:
                    self.service.drop(connection, name)
            finally:
                self.service.close(connection)

    def run_pushdown(
        self, config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame
    ) -> tuple[DiffResult, list[str]]:
        """Load the frames and compare them inside the warehouse.

        Args:
            config (DiffConfig): Comparison rules and keys.
            source (pl.DataFrame): Source rows.
            target (pl.DataFrame): Target rows.

        Returns:
            tuple[DiffResult, list[str]]: The pushdown result and the SQL that
                produced it, in execution order.
        """
        with (
            self.loaded_tables(source, target) as (source_table, target_table),
            _recorded(self.service.connector) as statements,
        ):
            result = DiffEngine.run_from_configs(
                config, self.service.source(source_table), self.service.source(target_table)
            )
        return result, statements

    def run_value_map_pushdown(
        self,
        config: DiffConfig,
        source: pl.DataFrame,
        target: pl.DataFrame,
        *,
        min_confidence: float,
        min_support: int,
        sample_fraction: float,
    ) -> tuple[list[ValueMapProposal], list[str]]:
        """Load the frames and propose value maps inside the warehouse.

        Args:
            config (DiffConfig): Comparison rules and keys.
            source (pl.DataFrame): Source rows.
            target (pl.DataFrame): Target rows.
            min_confidence (float): Share of rows that must agree.
            min_support (int): Agreeing rows a proposal needs.
            sample_fraction (float): Share of source keys to read.

        Returns:
            tuple[list[ValueMapProposal], list[str]]: The proposals and the SQL
                that produced them, in execution order.
        """
        with (
            self.loaded_tables(source, target) as (source_table, target_table),
            _recorded(self.service.connector) as statements,
        ):
            proposals = DiffEngine.propose_value_maps_from_configs(
                config,
                self.service.source(source_table),
                self.service.source(target_table),
                min_confidence=min_confidence,
                min_support=min_support,
                sample_fraction=sample_fraction,
            )
        return proposals, statements

    def run_local(
        self, config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame
    ) -> DiffResult:
        """Load the frames, read them back, and compare them locally.

        Args:
            config (DiffConfig): Comparison rules and keys.
            source (pl.DataFrame): Source rows.
            target (pl.DataFrame): Target rows.

        Returns:
            DiffResult: The local engine's verdict on the tables as the warehouse stores them.
        """
        with self.loaded_tables(source, target) as (source_table, target_table):
            stored_source = self._read(source_table)
            stored_target = self._read(target_table)
        return DiffEngine(config, stored_source.lazy(), stored_target.lazy()).run()

    def _read(self, table: str) -> pl.DataFrame:
        """Read a whole table through the shipped connector."""
        connector = self.service.connector(self.service.source(table))
        connector.connect()
        try:
            statement = f"SELECT * FROM {connector.compiler._quote_relation(table)}"
            frame: pl.DataFrame = connector.execute_pushdown(statement, "samples").collect()
            return frame
        finally:
            connector.close()


def backend(name: str) -> WarehouseBackend:
    """Return the parity backend for a live warehouse, reading its settings.

    Args:
        name (str): A key of `SERVICES`.

    Returns:
        WarehouseBackend: The backend.
    """
    return WarehouseBackend(SERVICES[name]())


def drop_stale_tables(name: str, now: datetime | None = None) -> list[str]:
    """Drop the tables a failed run left behind on a warehouse, once a day old.

    Args:
        name (str): A key of `SERVICES`.
        now (datetime | None): The current time in UTC. Defaults to now.

    Returns:
        list[str]: The tables dropped.
    """
    service = SERVICES[name]()
    moment = now or datetime.now(UTC)
    connection = service.connect()
    try:
        stale = [table for table in service.tables(connection) if is_stale(table, moment)]
        for table in stale:
            service.drop(connection, table)
    finally:
        service.close(connection)
    return stale
