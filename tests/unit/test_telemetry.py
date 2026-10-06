# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the OpenTelemetry metrics export."""

import json
import logging
import os
import socket
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path
from urllib.parse import quote

import polars as pl
import pytest
from google.protobuf import json_format
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.metrics.v1.metrics_pb2 import MetricsData
from pytest_mock import MockerFixture

from tests.otlp_collector import Collector, running_collector
from veridelta import __version__
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import (
    BigQueryConfig,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffResult,
    DiffSummary,
    DuckDBConfig,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
)
from veridelta.telemetry import render_otlp_metrics, send_otlp_metrics, write_otlp_metrics

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_SECRET = "hunter2-do-not-export"

_OBSERVED = 1_790_000_000_123_456_789
"""A fixed observation time, in nanoseconds since the epoch."""

_METRIC_NAMES = [
    "veridelta.dataset.rows",
    "veridelta.diff.rows",
    "veridelta.column.mismatched_rows",
    "veridelta.diff.mismatch_ratio",
    "veridelta.diff.match",
]


def _drift() -> DiffResult:
    """Run a comparison with one added, one removed, and one changed row.

    `val` differs on one shared key; `qty` is compared and always agrees.

    Returns:
        DiffResult: Four source rows, four target rows, three differences.
    """
    source = pl.DataFrame({"id": [1, 2, 3, 4], "val": ["A", "B", "C", "D"], "qty": [1, 2, 3, 4]})
    target = pl.DataFrame({"id": [2, 3, 4, 5], "val": ["B", "X", "D", "E"], "qty": [2, 3, 4, 5]})
    return DiffEngine(DiffConfig(primary_keys=["id"]), source.lazy(), target.lazy()).run()


def _match() -> DiffResult:
    """Run a comparison of two identical frames.

    Returns:
        DiffResult: A matching result.
    """
    frame = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
    return DiffEngine(DiffConfig(primary_keys=["id"]), frame.lazy(), frame.lazy()).run()


def _request(text: str) -> ExportMetricsServiceRequest:
    """Parse an export as an OTLP/JSON receiver does, refusing unknown fields.

    Args:
        text (str): Rendered export.

    Returns:
        ExportMetricsServiceRequest: The parsed message.
    """
    return json_format.Parse(text, ExportMetricsServiceRequest())


def _metrics(text: str) -> dict[str, dict[str, object]]:
    """Index the rendered metrics by name.

    Args:
        text (str): Rendered export.

    Returns:
        dict[str, dict[str, object]]: Each metric's JSON object, by name.
    """
    (resource_metrics,) = json.loads(text)["resourceMetrics"]
    (scope_metrics,) = resource_metrics["scopeMetrics"]
    return {metric["name"]: metric for metric in scope_metrics["metrics"]}


def _points(text: str, name: str) -> dict[str | None, int | float]:
    """Read one metric's data points, keyed by their single attribute's value.

    Args:
        text (str): Rendered export.
        name (str): Metric name.

    Returns:
        dict[str | None, int | float]: Each point's value, keyed by the value
            of its one attribute, or by None for a point without attributes.
    """
    metric = _request(text).resource_metrics[0].scope_metrics[0]
    (selected,) = [candidate for candidate in metric.metrics if candidate.name == name]
    values: dict[str | None, int | float] = {}
    for point in selected.gauge.data_points:
        key = point.attributes[0].value.string_value if point.attributes else None
        values[key] = point.as_int if point.WhichOneof("value") == "as_int" else point.as_double
    return values


def _resource(text: str) -> dict[str, str]:
    """Read the resource attributes as a plain mapping.

    Args:
        text (str): Rendered export.

    Returns:
        dict[str, str]: Attribute keys to their string values.
    """
    attributes = _request(text).resource_metrics[0].resource.attributes
    return {attribute.key: attribute.value.string_value for attribute in attributes}


@pytest.fixture(autouse=True)
def _clear_otel_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset the OpenTelemetry variables, so the shell running the tests cannot change an export."""
    for name in list(os.environ):
        if name.startswith("OTEL_"):
            monkeypatch.delenv(name)


class TestOTLPShape:
    """Validate that the export is an OTLP/JSON metrics request a collector accepts."""

    def test_it_parses_as_a_metrics_export(self) -> None:
        """Ensure the official OTLP message accepts the export, unknown fields refused."""
        request = _request(render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED))

        (resource_metrics,) = request.resource_metrics
        (scope_metrics,) = resource_metrics.scope_metrics
        assert scope_metrics.scope.name == "veridelta"
        assert scope_metrics.scope.version == __version__
        assert [metric.name for metric in scope_metrics.metrics] == _METRIC_NAMES

    def test_it_is_a_line_of_the_file_exporter_format(self) -> None:
        """Ensure the export is also a `MetricsData`, the object each line of an OTLP file holds."""
        text = render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED)

        data = json_format.Parse(text, MetricsData())

        assert [metric.name for metric in data.resource_metrics[0].scope_metrics[0].metrics] == (
            _METRIC_NAMES
        )

    def test_it_reports_every_metric_as_a_gauge(self) -> None:
        """Ensure each value is a snapshot of this run, not a sum to accumulate."""
        request = _request(render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED))

        metrics = request.resource_metrics[0].scope_metrics[0].metrics
        assert {metric.WhichOneof("data") for metric in metrics} == {"gauge"}

    def test_it_writes_64_bit_integers_as_strings(self) -> None:
        """Ensure times and counts follow the protobuf JSON mapping for 64-bit integers.

        The parser accepts JSON numbers too, so this reads the raw JSON.
        """
        text = render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED)

        points = [
            point
            for metric in _metrics(text).values()
            for point in metric["gauge"]["dataPoints"]  # type: ignore[index]
        ]
        assert points
        assert all(point["timeUnixNano"] == str(_OBSERVED) for point in points)
        integers = [point["asInt"] for point in points if "asInt" in point]
        assert integers
        assert all(isinstance(value, str) for value in integers)

    def test_it_stamps_points_with_the_current_time_by_default(self) -> None:
        """Ensure an export without a given time is stamped when it is rendered."""
        before = time.time_ns()
        request = _request(render_otlp_metrics(_drift()))
        after = time.time_ns()

        metrics = request.resource_metrics[0].scope_metrics[0].metrics
        stamps = {point.time_unix_nano for metric in metrics for point in metric.gauge.data_points}
        (stamp,) = stamps
        assert before <= stamp <= after

    def test_it_gives_each_metric_a_unit_a_backend_can_read(self) -> None:
        """Pin the units: rows as `{row}`, the ratio as `1`, the verdict unitless.

        A Prometheus backend appends `_ratio` to a gauge whose unit is `1`, so
        the verdict carries no unit rather than reading as a ratio.
        """
        metrics = _metrics(render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED))

        assert {name: metric.get("unit") for name, metric in metrics.items()} == {
            "veridelta.dataset.rows": "{row}",
            "veridelta.diff.rows": "{row}",
            "veridelta.column.mismatched_rows": "{row}",
            "veridelta.diff.mismatch_ratio": "1",
            "veridelta.diff.match": None,
        }
        assert all(metric["description"] for metric in metrics.values())


class TestOTLPValues:
    """Validate the counts, column drift, and verdict the export carries."""

    @pytest.mark.parametrize(
        ("metric", "points"),
        [
            pytest.param("veridelta.dataset.rows", {"source": 4, "target": 4}, id="rows-by-side"),
            pytest.param(
                "veridelta.diff.rows", {"added": 1, "removed": 1, "changed": 1}, id="rows-by-kind"
            ),
            # A column that stops drifting reports zero rather than vanishing.
            pytest.param(
                "veridelta.column.mismatched_rows", {"val": 1, "qty": 0}, id="every-compared-column"
            ),
        ],
    )
    def test_it_reports_one_point_per_label(self, metric: str, points: dict[str, int]) -> None:
        """Ensure each count is one point, labeled by side, kind, or column."""
        assert _points(render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED), metric) == points

    def test_it_reports_a_drifting_column_missing_from_the_compared_list(self) -> None:
        """Ensure a hand-built result's drift is exported even without `compared_columns`."""
        result = DiffResult(
            summary=DiffSummary(
                total_rows_source=10,
                total_rows_target=10,
                added_count=0,
                removed_count=0,
                changed_count=2,
                column_mismatches={"amount": 2},
                is_match=False,
            ),
            added=pl.DataFrame(),
            removed=pl.DataFrame(),
            changed=pl.DataFrame(),
        )

        text = render_otlp_metrics(result, time_unix_nano=_OBSERVED)

        assert _points(text, "veridelta.column.mismatched_rows") == {"amount": 2}

    def test_it_leaves_out_column_drift_when_no_column_was_compared(self) -> None:
        """Ensure a key-only comparison exports no empty column metric."""
        frame = pl.DataFrame({"id": [1, 2]})
        result = DiffEngine(DiffConfig(primary_keys=["id"]), frame.lazy(), frame.lazy()).run()

        metrics = _metrics(render_otlp_metrics(result, time_unix_nano=_OBSERVED))

        assert "veridelta.column.mismatched_rows" not in metrics
        assert len(metrics) == len(_METRIC_NAMES) - 1

    def test_it_reports_the_mismatch_ratio(self) -> None:
        """Ensure the ratio is the summary's: differences over source rows."""
        result = _drift()

        text = render_otlp_metrics(result, time_unix_nano=_OBSERVED)

        assert _points(text, "veridelta.diff.mismatch_ratio") == {None: 0.75}
        assert result.summary.mismatch_ratio == 0.75

    @pytest.mark.parametrize(("run", "verdict"), [(_match, 1), (_drift, 0)])
    def test_it_reports_the_verdict_as_one_or_zero(
        self, run: Callable[[], DiffResult], verdict: int
    ) -> None:
        """Ensure a match within the threshold is 1 and drift is 0."""
        text = render_otlp_metrics(run(), time_unix_nano=_OBSERVED)

        assert _points(text, "veridelta.diff.match") == {None: verdict}


class TestOTLPResource:
    """Validate the resource attributes that say which comparison ran."""

    def test_it_names_the_service_and_its_version(self) -> None:
        """Ensure a result alone yields only the service attributes."""
        text = render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED)

        assert _resource(text) == {"service.name": "veridelta", "service.version": __version__}

    def test_it_records_the_configuration_and_both_sides(self) -> None:
        """Ensure the configuration path and each side's type and name are attributes."""
        text = render_otlp_metrics(
            _drift(),
            config_path=Path("configs/orders.yaml"),
            source=SourceConfig(path="legacy/orders.csv"),
            target=SnowflakeConfig(
                table="MODERN.ORDERS",
                account="a",
                user="u",
                warehouse="w",
                database="d",
                schema_name="s",
            ),
            time_unix_nano=_OBSERVED,
        )

        assert _resource(text) == {
            "service.name": "veridelta",
            "service.version": __version__,
            "veridelta.config.path": str(Path("configs/orders.yaml")),
            "veridelta.source.type": "file",
            "veridelta.source.name": "legacy/orders.csv",
            "veridelta.target.type": "snowflake",
            "veridelta.target.name": "MODERN.ORDERS",
        }

    @pytest.mark.parametrize(
        ("side", "kind", "name"),
        [
            pytest.param(SourceConfig(path="data/orders.parquet"), "file", "data/orders.parquet"),
            pytest.param(
                DatabricksConfig(table="main.sales.orders", server_hostname="h", http_path="/sql"),
                "databricks",
                "main.sales.orders",
            ),
            pytest.param(
                BigQueryConfig(table="sales.orders", project="acme-analytics"),
                "bigquery",
                "sales.orders",
            ),
            pytest.param(
                DeltaLakeConfig(table_uri="s3://lake/events"), "delta", "s3://lake/events"
            ),
            pytest.param(
                IcebergConfig(table_uri="warehouse/orders"), "iceberg", "warehouse/orders"
            ),
            pytest.param(
                DatabaseConfig(uri="postgresql://analyst@db.internal/sales", table="legacy.orders"),
                "database",
                "legacy.orders",
            ),
            pytest.param(
                DatabaseConfig(uri="sqlite:///orders.db", query="SELECT * FROM orders"),
                "database",
                None,
                id="database-query",
            ),
            pytest.param(
                DuckDBConfig(database="warehouse.duckdb", table="main.orders"),
                "duckdb",
                "main.orders",
                id="duckdb",
            ),
            pytest.param(
                DuckDBConfig(database="md:sales", query="SELECT * FROM orders"),
                "duckdb",
                None,
                id="duckdb-query",
            ),
        ],
    )
    def test_it_names_each_kind_of_source(
        self, side: SourceRef, kind: str, name: str | None
    ) -> None:
        """Ensure a side is named by its table or path, and a query names nothing."""
        attributes = _resource(render_otlp_metrics(_drift(), source=side))

        assert attributes["veridelta.source.type"] == kind
        assert attributes.get("veridelta.source.name") == name

    @pytest.mark.parametrize(
        ("path", "name"),
        [
            pytest.param(
                "https://user:pw@files.example.com:8443/data/orders.csv?X-Amz-Signature=abc#top",
                "https://files.example.com:8443/data/orders.csv",
                id="signed-url",
            ),
            pytest.param(
                "https://ghp_token@raw.example.com/acme/orders.csv",
                "https://raw.example.com/acme/orders.csv",
                id="token-as-user",
            ),
            pytest.param("s3://lake/orders/part-0.parquet", "s3://lake/orders/part-0.parquet"),
            pytest.param("C:\\data\\orders.csv", "C:\\data\\orders.csv", id="windows-path"),
            pytest.param("file:///srv/data/orders.csv", "file:///srv/data/orders.csv"),
        ],
    )
    def test_it_keeps_only_the_scheme_host_and_path_of_a_url(self, path: str, name: str) -> None:
        """Ensure credentials in a URL's user part or query never reach the export."""
        attributes = _resource(render_otlp_metrics(_drift(), target=SourceConfig(path=path)))

        assert attributes["veridelta.target.name"] == name

    @pytest.mark.parametrize(
        ("path", "name"),
        [
            pytest.param(
                "abfss://events@lake.dfs.core.windows.net/raw/orders?sv=2024-11-04&sig=abc",
                "abfss://events@lake.dfs.core.windows.net/raw/orders",
                id="abfss",
            ),
            pytest.param(
                "wasbs://events@lake.blob.core.windows.net/raw/orders",
                "wasbs://events@lake.blob.core.windows.net/raw/orders",
                id="wasbs",
            ),
            pytest.param(
                "abfss://events:hunter2@lake.dfs.core.windows.net/raw/orders",
                "abfss://lake.dfs.core.windows.net/raw/orders",
                id="password-in-user-part",
            ),
        ],
    )
    def test_it_keeps_an_azure_container_but_never_a_password(self, path: str, name: str) -> None:
        """Ensure `container@account` keeps its container, which Azure puts where a user goes."""
        side = DeltaLakeConfig(table_uri=path)

        attributes = _resource(render_otlp_metrics(_drift(), source=side))

        assert attributes["veridelta.source.name"] == name

    def test_it_leaves_out_a_name_that_does_not_parse_as_a_url(self) -> None:
        """Ensure a path the URL parser rejects is dropped rather than exported raw."""
        side = DeltaLakeConfig(table_uri="https://[::1/lake/events?sig=abc")

        attributes = _resource(render_otlp_metrics(_drift(), source=side))

        assert attributes["veridelta.source.type"] == "delta"
        assert "veridelta.source.name" not in attributes

    @pytest.mark.parametrize(
        "side",
        [
            pytest.param(
                SnowflakeConfig(
                    table="T",
                    account="a",
                    user="u",
                    warehouse="w",
                    database="d",
                    schema_name="s",
                    password=_SECRET,
                ),
                id="snowflake-password",
            ),
            pytest.param(
                DatabricksConfig(
                    table="t", server_hostname="h", http_path="/sql", access_token=_SECRET
                ),
                id="databricks-token",
            ),
            pytest.param(
                BigQueryConfig(
                    table="sales.orders",
                    project="acme-analytics",
                    credentials_path=f"/keys/{_SECRET}.json",
                ),
                id="bigquery-key-file",
            ),
            pytest.param(
                DeltaLakeConfig(
                    table_uri="s3://lake/events", storage_options={"AWS_SECRET_ACCESS_KEY": _SECRET}
                ),
                id="delta-storage-options",
            ),
            pytest.param(
                IcebergConfig(
                    table_uri="s3://lake/iceberg/events",
                    storage_options={"AWS_SECRET_ACCESS_KEY": _SECRET},
                ),
                id="iceberg-storage-options",
            ),
            pytest.param(
                SourceConfig(
                    path="s3://lake/orders.parquet",
                    format="parquet",
                    options={"storage_options": {"aws_secret_access_key": _SECRET}},
                ),
                id="file-storage-options",
            ),
            pytest.param(
                DatabaseConfig(
                    uri="postgresql://analyst@db.internal/sales", password=_SECRET, table="t"
                ),
                id="database-password",
            ),
            pytest.param(
                DatabaseConfig(uri=f"postgresql://analyst:{_SECRET}@db.internal/sales", table="t"),
                id="database-uri",
            ),
            pytest.param(
                DatabaseConfig(
                    uri="postgresql://analyst@db.internal/sales",
                    query=f"SELECT * FROM orders WHERE note = '{_SECRET}'",
                ),
                id="database-query",
            ),
            pytest.param(
                DuckDBConfig(database="md:db.internal", table="orders", motherduck_token=_SECRET),
                id="duckdb-token",
            ),
            pytest.param(
                DuckDBConfig(
                    database="/srv/db.internal/warehouse.duckdb",
                    query=f"SELECT * FROM orders WHERE note = '{_SECRET}'",
                ),
                id="duckdb-query",
            ),
        ],
    )
    def test_it_never_exports_a_credential_uri_or_query(self, side: SourceRef) -> None:
        """Ensure metrics bound for a third-party backend carry no secret or SQL."""
        text = render_otlp_metrics(_drift(), source=side, target=side)

        assert _SECRET not in text
        assert "db.internal" not in text


class TestOTLPEnvironment:
    """Validate the resource attributes the OpenTelemetry environment variables add."""

    def test_it_adds_the_attributes_the_environment_names(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure `OTEL_RESOURCE_ATTRIBUTES` tags a run alongside Veridelta's attributes."""
        monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=prod,team=data")

        text = render_otlp_metrics(_drift(), source=SourceConfig(path="orders.csv"))

        assert _resource(text) == {
            "service.name": "veridelta",
            "service.version": __version__,
            "deployment.environment": "prod",
            "team": "data",
            "veridelta.source.type": "file",
            "veridelta.source.name": "orders.csv",
        }

    @pytest.mark.parametrize(
        ("variables", "service"),
        [
            pytest.param(
                {"OTEL_SERVICE_NAME": "orders-migration"}, "orders-migration", id="service-name"
            ),
            pytest.param(
                {"OTEL_RESOURCE_ATTRIBUTES": "service.name=from-attributes"},
                "from-attributes",
                id="resource-attribute",
            ),
            # The specification gives `OTEL_SERVICE_NAME` precedence.
            pytest.param(
                {
                    "OTEL_SERVICE_NAME": "orders-migration",
                    "OTEL_RESOURCE_ATTRIBUTES": "service.name=from-attributes",
                },
                "orders-migration",
                id="both",
            ),
            # An empty variable counts as unset.
            pytest.param({"OTEL_SERVICE_NAME": ""}, "veridelta", id="empty-service-name"),
        ],
    )
    def test_it_takes_the_service_name_from_the_environment(
        self, monkeypatch: pytest.MonkeyPatch, variables: dict[str, str], service: str
    ) -> None:
        """Ensure the environment renames the service, as an OpenTelemetry SDK lets it."""
        for name, value in variables.items():
            monkeypatch.setenv(name, value)

        assert _resource(render_otlp_metrics(_drift()))["service.name"] == service

    def test_it_trims_and_percent_decodes_each_item(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Ensure items read as the specification writes them, skipping blank ones."""
        monkeypatch.setenv(
            "OTEL_RESOURCE_ATTRIBUTES",
            " team = data%20platform ,, region%2Fzone=eu%2Cwest ,"
            "city=Z%C3%BCrich,town=Z\u00fcrich,checksum=ab==,",
        )

        assert _resource(render_otlp_metrics(_drift())) == {
            "service.name": "veridelta",
            "service.version": __version__,
            "team": "data platform",
            "region/zone": "eu,west",
            "city": "Z\u00fcrich",
            "town": "Z\u00fcrich",
            "checksum": "ab==",
        }

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("team", id="missing-equals-sign"),
            pytest.param(" =data", id="empty-key"),
            pytest.param("team=50%", id="truncated-escape"),
            pytest.param("team=%zz", id="non-hex-escape"),
            pytest.param("team=%C3", id="invalid-utf-8"),
            # One bad item discards the items before it too.
            pytest.param("deployment.environment=prod,team", id="valid-item-first"),
        ],
    )
    def test_it_discards_a_malformed_variable_with_one_warning(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, value: str
    ) -> None:
        """Ensure a malformed value is dropped whole, as the specification asks, and never logged."""
        monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", f"{value},note={_SECRET}")
        monkeypatch.setenv("OTEL_SERVICE_NAME", "orders-migration")
        result = _drift()

        with caplog.at_level(logging.WARNING, logger="veridelta.telemetry"):
            text = render_otlp_metrics(result)

        assert _resource(text) == {
            "service.name": "orders-migration",
            "service.version": __version__,
        }
        (record,) = caplog.records
        assert (record.name, record.levelno) == ("veridelta.telemetry", logging.WARNING)
        assert "OTEL_RESOURCE_ATTRIBUTES" in record.getMessage()
        assert _SECRET not in caplog.text

    def test_it_keeps_its_own_attributes_over_the_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure the environment cannot relabel the version or what was compared."""
        monkeypatch.setenv(
            "OTEL_RESOURCE_ATTRIBUTES",
            "service.version=9.9.9,veridelta.source.type=spoofed,"
            "veridelta.config.path=other.yaml,team=data,team=platform",
        )

        text = render_otlp_metrics(
            _drift(), config_path="veridelta.yaml", source=SourceConfig(path="orders.csv")
        )

        keys = [
            attribute.key for attribute in _request(text).resource_metrics[0].resource.attributes
        ]
        assert len(keys) == len(set(keys))
        assert _resource(text) == {
            "service.name": "veridelta",
            "service.version": __version__,
            "team": "platform",
            "veridelta.config.path": "veridelta.yaml",
            "veridelta.source.type": "file",
            "veridelta.source.name": "orders.csv",
        }

    def test_it_keeps_the_scope_named_for_veridelta(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Ensure a renamed service still reports Veridelta as the instrumentation scope."""
        monkeypatch.setenv("OTEL_SERVICE_NAME", "orders-migration")

        scope = _request(render_otlp_metrics(_drift())).resource_metrics[0].scope_metrics[0].scope

        assert (scope.name, scope.version) == ("veridelta", __version__)


class TestWriteOTLPMetrics:
    """Validate the file a collector or an HTTP request reads."""

    def test_it_writes_one_line_creating_parent_directories(self, tmp_path: Path) -> None:
        """Ensure the file is the export on one line, ended by a bare newline on every platform.

        The Collector's file receiver reads one export per line, and the same
        bytes on Windows keep the file identical wherever CI runs.
        """
        destination = tmp_path / "out" / "metrics" / "otel.json"

        written = write_otlp_metrics(_drift(), destination, time_unix_nano=_OBSERVED)

        assert written == destination
        export = render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED)
        assert destination.read_bytes() == f"{export}\n".encode()

    def test_it_keeps_any_column_name_on_the_one_line(self, tmp_path: Path) -> None:
        """Ensure a line break or non-ASCII character in a name is escaped, not written raw."""
        names = ["line\nbreak", "caf\u00e9", "para\u2029graph"]
        source = pl.DataFrame({"id": [1], **{name: ["a"] for name in names}})
        target = pl.DataFrame({"id": [1], **{name: ["b"] for name in names}})
        result = DiffEngine(DiffConfig(primary_keys=["id"]), source.lazy(), target.lazy()).run()

        written = write_otlp_metrics(result, tmp_path / "otel.json", time_unix_nano=_OBSERVED)

        content = written.read_text(encoding="utf-8")
        assert len(content.splitlines()) == 1
        assert content.isascii()
        assert _points(content, "veridelta.column.mismatched_rows") == dict.fromkeys(names, 1)


_HEADER_SECRET = "s3cret token"
"""An API key a header carries, with a space that the variable percent-encodes."""


@pytest.fixture
def collector(monkeypatch: pytest.MonkeyPatch) -> Iterator[Collector]:
    """Serve a stand-in collector, reached without any proxy the shell sets."""
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    with running_collector() as stand_in:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", stand_in.url)
        yield stand_in


def _closed_port() -> int:
    """Return a local port that nothing listens on."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
    return port


class TestSendOTLPMetrics:
    """Validate `send_otlp_metrics`, which posts the export to an OTLP/HTTP endpoint."""

    def test_it_posts_the_export_to_the_metrics_path_of_the_endpoint(
        self, collector: Collector
    ) -> None:
        """Ensure the body is the export a file holds, sent as JSON to `/v1/metrics`."""
        reached = send_otlp_metrics(
            _drift(), config_path="veridelta.yaml", time_unix_nano=_OBSERVED
        )

        (request,) = collector.received
        assert reached == f"{collector.url}/v1/metrics"
        assert request.path == "/v1/metrics"
        assert request.headers["content-type"] == "application/json"
        expected = render_otlp_metrics(
            _drift(), config_path="veridelta.yaml", time_unix_nano=_OBSERVED
        )
        assert request.body == expected.encode()
        assert _request(request.body.decode()).resource_metrics

    @pytest.mark.parametrize(
        ("suffix", "path"),
        [
            pytest.param("/", "/v1/metrics", id="trailing-slash"),
            pytest.param("/otlp", "/otlp/v1/metrics", id="base-path"),
            pytest.param("/otlp/", "/otlp/v1/metrics", id="base-path-slash"),
        ],
    )
    def test_it_appends_the_metrics_path_to_any_base(
        self, collector: Collector, monkeypatch: pytest.MonkeyPatch, suffix: str, path: str
    ) -> None:
        """Ensure a base endpoint gains `v1/metrics` once, after its own path."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.url + suffix)

        send_otlp_metrics(_drift())

        assert [request.path for request in collector.received] == [path]

    def test_it_posts_to_the_metrics_endpoint_as_written(
        self, collector: Collector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure the metrics endpoint wins over the base one, and gains no path."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", f"{collector.url}/ingest")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", f"http://127.0.0.1:{_closed_port()}")

        send_otlp_metrics(_drift())

        assert [request.path for request in collector.received] == ["/ingest"]

    def test_it_defaults_to_the_collector_port_on_this_machine(self, mocker: MockerFixture) -> None:
        """Ensure no endpoint means the specification's default, `http://localhost:4318`."""
        opened = mocker.patch.object(urllib.request.OpenerDirector, "open")

        reached = send_otlp_metrics(_drift())

        (request,) = opened.call_args.args
        assert request.full_url == reached == "http://localhost:4318/v1/metrics"
        assert opened.call_args.kwargs == {"timeout": 10.0}

    def test_it_sends_the_headers_the_variables_set(
        self, collector: Collector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure each header arrives percent-decoded, and the content type stays JSON."""
        monkeypatch.setenv(
            "OTEL_EXPORTER_OTLP_HEADERS",
            f"api-key={quote(_HEADER_SECRET)},x-team=data,content-type=text/plain",
        )

        send_otlp_metrics(_drift())

        (request,) = collector.received
        assert request.headers["api-key"] == _HEADER_SECRET
        assert request.headers["x-team"] == "data"
        assert request.headers["content-type"] == "application/json"

    def test_the_metrics_headers_replace_the_general_ones(
        self, collector: Collector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure the metrics variable wins whole, as the OpenTelemetry SDKs read it."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-general=1")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "x-metrics=1")

        send_otlp_metrics(_drift())

        (request,) = collector.received
        assert "x-metrics" in request.headers
        assert "x-general" not in request.headers

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param(f"api-key{_HEADER_SECRET}", id="no-equals"),
            pytest.param(f"api-key={_HEADER_SECRET}%zz", id="bad-escape"),
            pytest.param(f"api key={quote(_HEADER_SECRET)}", id="bad-name"),
            pytest.param(f"api-key={quote(_HEADER_SECRET)}%0D%0AX-Evil:%201", id="line-break"),
        ],
    )
    def test_it_refuses_malformed_headers_without_repeating_them(
        self, collector: Collector, monkeypatch: pytest.MonkeyPatch, headers: str
    ) -> None:
        """Ensure an unusable header fails before any request, naming only the variable."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", headers)

        with pytest.raises(ConfigError, match=r"^OTEL_EXPORTER_OTLP_METRICS_HEADERS must") as info:
            send_otlp_metrics(_drift())

        assert "s3cret" not in str(info.value)
        assert info.value.__cause__ is None
        assert collector.received == []

    @pytest.mark.parametrize(
        ("name", "protocol"),
        [
            pytest.param("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc", id="grpc"),
            pytest.param("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf", id="protobuf"),
            pytest.param("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/protobuf", id="metrics"),
        ],
    )
    def test_it_refuses_a_protocol_it_does_not_send(
        self, collector: Collector, monkeypatch: pytest.MonkeyPatch, name: str, protocol: str
    ) -> None:
        """Ensure a configured protocol other than JSON over HTTP fails rather than goes ignored."""
        monkeypatch.setenv(name, protocol)

        with pytest.raises(ConfigError, match=f"{name} is '{protocol}'"):
            send_otlp_metrics(_drift())

        assert collector.received == []

    def test_the_metrics_protocol_wins_over_the_general_one(
        self, collector: Collector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure `http/json` for metrics sends, whatever the general protocol says."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/json")

        send_otlp_metrics(_drift())

        assert len(collector.received) == 1

    @pytest.mark.parametrize(
        "endpoint",
        [
            pytest.param("localhost:4318", id="no-scheme"),
            pytest.param("ftp://collector.internal", id="ftp"),
            pytest.param("http://", id="no-host"),
            pytest.param("http://collector.internal:port", id="bad-port"),
            pytest.param("http://collector.internal:0", id="port-zero"),
        ],
    )
    def test_it_refuses_an_endpoint_that_is_not_an_http_url(
        self, monkeypatch: pytest.MonkeyPatch, endpoint: str
    ) -> None:
        """Ensure a URL the standard library cannot post to fails with the variable named."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)

        with pytest.raises(ConfigError, match=r"^OTEL_EXPORTER_OTLP_ENDPOINT must be an http"):
            send_otlp_metrics(_drift())

    def test_it_refuses_credentials_inside_the_endpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a password in the URL is refused without being repeated."""
        monkeypatch.setenv(
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "https://ingest:s3cret@otlp.example.com/v1/m"
        )

        with pytest.raises(ConfigError, match="must hold no user name or password") as info:
            send_otlp_metrics(_drift())

        assert "s3cret" not in str(info.value)

    @pytest.mark.parametrize("timeout", ["soon", "0", "-5", "1.5"])
    def test_it_refuses_a_timeout_that_is_not_whole_milliseconds(
        self, monkeypatch: pytest.MonkeyPatch, timeout: str
    ) -> None:
        """Ensure a timeout is a whole number of milliseconds above 0."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_TIMEOUT", timeout)

        with pytest.raises(ConfigError, match=r"^OTEL_EXPORTER_OTLP_TIMEOUT must be a whole"):
            send_otlp_metrics(_drift())

    def test_it_stops_waiting_when_the_timeout_ends(
        self, collector: Collector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a slow endpoint fails the send once the metrics timeout passes."""
        collector.delay = 1.0
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT", "100")

        with pytest.raises(ConnectorError, match=r"no answer came within 0\.1 seconds"):
            send_otlp_metrics(_drift())

    def test_it_reports_an_http_error_without_any_secret(
        self, collector: Collector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a refusal names the status and the endpoint, but no header or query."""
        collector.status = 500
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", f"{collector.url}/m?key=s3cret")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", f"api-key={quote(_HEADER_SECRET)}")

        with pytest.raises(ConnectorError) as info:
            send_otlp_metrics(_drift())

        assert str(info.value) == (
            f"Sending metrics to {collector.url}/m failed: "
            "the endpoint answered with HTTP 500 Internal Server Error."
        )
        assert info.value.__cause__ is None

    def test_it_refuses_a_redirect_so_no_header_reaches_another_host(
        self, collector: Collector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a redirect fails the send, and the second host receives nothing."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", f"api-key={quote(_HEADER_SECRET)}")
        with running_collector() as elsewhere:
            collector.status = 307
            collector.location = f"{elsewhere.url}/v1/metrics"

            with pytest.raises(ConnectorError, match="redirect, which Veridelta does not follow"):
                send_otlp_metrics(_drift())

            assert elsewhere.received == []

    def test_it_reports_an_endpoint_nothing_listens_on(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a refused connection is a connector error naming the endpoint."""
        monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
        url = f"http://127.0.0.1:{_closed_port()}"
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", url)

        with pytest.raises(ConnectorError, match=f"^Sending metrics to {url}/v1/metrics failed: "):
            send_otlp_metrics(_drift())

    def test_it_reports_a_connection_that_timed_out(self, mocker: MockerFixture) -> None:
        """Ensure a connection the timeout cut short reads as one, not as a raw socket error."""
        mocker.patch.object(
            urllib.request.OpenerDirector,
            "open",
            side_effect=urllib.error.URLError(TimeoutError("timed out")),
        )

        with pytest.raises(ConnectorError, match="no answer came within 10 seconds"):
            send_otlp_metrics(_drift())

    def test_it_reports_a_connection_closed_without_an_answer(self, collector: Collector) -> None:
        """Ensure an endpoint that hangs up is a connector error, not a crash."""
        collector.hang_up = True

        with pytest.raises(ConnectorError, match=r"^Sending metrics to .* failed: "):
            send_otlp_metrics(_drift())

    def test_it_logs_the_endpoint_without_its_query(
        self,
        collector: Collector,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Ensure the log line names where the export went, and nothing secret."""
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", f"{collector.url}/m?key=s3cret")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", f"api-key={quote(_HEADER_SECRET)}")

        with caplog.at_level(logging.INFO, logger="veridelta.telemetry"):
            send_otlp_metrics(_drift())
            collector.status = 503
            with pytest.raises(ConnectorError):
                send_otlp_metrics(_drift())

        messages = [record.getMessage() for record in caplog.records]
        assert messages[0].startswith(f"Sent metrics to {collector.url}/m in ")
        assert messages[1].startswith(f"Sending metrics to {collector.url}/m failed after ")
        assert "s3cret" not in caplog.text
