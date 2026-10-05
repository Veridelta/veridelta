# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the OpenTelemetry metrics export."""

import json
import time
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from google.protobuf import json_format
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.metrics.v1.metrics_pb2 import MetricsData

from veridelta import __version__
from veridelta.engine import DiffEngine
from veridelta.models import (
    BigQueryConfig,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffResult,
    DiffSummary,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
)
from veridelta.telemetry import render_otlp_metrics, write_otlp_metrics

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


@pytest.mark.unit
@pytest.mark.fast
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


@pytest.mark.unit
@pytest.mark.fast
class TestOTLPValues:
    """Validate the counts, column drift, and verdict the export carries."""

    def test_it_counts_the_rows_on_each_side(self) -> None:
        """Ensure each side's row total is one point, labeled by side."""
        text = render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED)

        assert _points(text, "veridelta.dataset.rows") == {"source": 4, "target": 4}

    def test_it_counts_added_removed_and_changed_rows(self) -> None:
        """Ensure each kind of difference is one point, labeled by kind."""
        text = render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED)

        assert _points(text, "veridelta.diff.rows") == {"added": 1, "removed": 1, "changed": 1}

    def test_it_reports_every_compared_column_including_those_without_drift(self) -> None:
        """Ensure a column that stops drifting reports zero rather than vanishing."""
        text = render_otlp_metrics(_drift(), time_unix_nano=_OBSERVED)

        assert _points(text, "veridelta.column.mismatched_rows") == {"val": 1, "qty": 0}

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


@pytest.mark.unit
@pytest.mark.fast
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
        ],
    )
    def test_it_never_exports_a_credential_uri_or_query(self, side: SourceRef) -> None:
        """Ensure metrics bound for a third-party backend carry no secret or SQL."""
        text = render_otlp_metrics(_drift(), source=side, target=side)

        assert _SECRET not in text
        assert "db.internal" not in text


@pytest.mark.unit
@pytest.mark.fast
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
