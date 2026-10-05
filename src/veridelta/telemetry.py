# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""OpenTelemetry metrics for comparison results.

Renders a `DiffResult` as one OTLP/JSON metrics export: the body an OTLP/HTTP
endpoint accepts at `/v1/metrics`, written on a single line so the
OpenTelemetry Collector's `otlpjsonfile` receiver can read the file as well.
No OpenTelemetry SDK is needed. The JSON follows the protobuf JSON mapping
that OTLP specifies, so 64-bit integers are written as strings.

Every value is a gauge: a snapshot of one run, which a backend graphs over
time rather than adds up. The export carries counts, column names, and what
was compared, never row values, connection URIs, credentials, or query text,
since metrics usually end up in a third-party backend.
"""

import json
import time
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit, urlunsplit

from veridelta import __version__
from veridelta.models import DeltaLakeConfig, DiffResult, IcebergConfig, SourceConfig, SourceRef

SERVICE_NAME: Final[str] = "veridelta"
"""`service.name` resource attribute and instrumentation scope name."""

_ROW_UNIT: Final[str] = "{row}"
"""UCUM annotation for a count of rows; backends treat it as dimensionless."""

_JSONObject = dict[str, object]


def _attribute(key: str, value: str) -> _JSONObject:
    """Build one OTLP key-value attribute holding a string.

    Args:
        key (str): Attribute name.
        value (str): Attribute value.

    Returns:
        _JSONObject: The attribute in OTLP/JSON form.
    """
    return {"key": key, "value": {"stringValue": value}}


def _point(
    value: int | float, observed: int, attributes: dict[str, str] | None = None
) -> _JSONObject:
    """Build one gauge data point.

    Args:
        value (int | float): The measurement. An `int` becomes `asInt`, written
            as a string as the JSON mapping requires; a `float` becomes `asDouble`.
        observed (int): Observation time, in nanoseconds since the epoch.
        attributes (dict[str, str] | None): Attributes that tell this point
            apart from the metric's others.

    Returns:
        _JSONObject: The data point in OTLP/JSON form.
    """
    point: _JSONObject = {}
    if attributes:
        point["attributes"] = [_attribute(key, item) for key, item in attributes.items()]
    point["timeUnixNano"] = str(observed)
    if isinstance(value, int):
        point["asInt"] = str(value)
    else:
        point["asDouble"] = value
    return point


def _gauge(name: str, description: str, unit: str, points: list[_JSONObject]) -> _JSONObject:
    """Build one gauge metric.

    Args:
        name (str): Metric name.
        description (str): What the metric measures.
        unit (str): UCUM unit, or empty for none.
        points (list[_JSONObject]): The metric's data points.

    Returns:
        _JSONObject: The metric in OTLP/JSON form.
    """
    metric: _JSONObject = {"name": name, "description": description}
    if unit:
        metric["unit"] = unit
    metric["gauge"] = {"dataPoints": points}
    return metric


def _locator(text: str) -> str | None:
    """Reduce a path or URL to what may leave the run.

    A URL can carry a credential in its user part, such as a token used as
    the user name, and a signature in its query, as a pre-signed object-store
    link does. Only its scheme, host, port, and path are kept. Anything
    without a host, such as a local or Windows path, is kept as written.

    Args:
        text (str): Path or URL from a source configuration.

    Returns:
        str | None: The reduced locator, or None when the text cannot be
            parsed as a URL and so cannot be reduced safely.
    """
    try:
        parts = urlsplit(text)
    except ValueError:
        return None
    if not (parts.scheme and parts.netloc):
        return text
    host = parts.netloc.rpartition("@")[2]
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def _side_name(side: SourceRef) -> str | None:
    """Name a source by its table or path, without anything secret.

    Warehouse and database tables are validated identifiers, so they are kept
    as configured. A database `query` is the user's own SQL, which is never
    exported, and a database `uri` names a server and a login, so a query
    source has no name.

    Args:
        side (SourceRef): Source or target configuration.

    Returns:
        str | None: The name to export, or None when there is none to give.
    """
    if isinstance(side, SourceConfig):
        return _locator(side.path)
    if isinstance(side, (DeltaLakeConfig, IcebergConfig)):
        return _locator(side.table_uri)
    return side.table


def _resource_attributes(
    config_path: str | Path | None, source: SourceRef | None, target: SourceRef | None
) -> list[_JSONObject]:
    """Describe which comparison ran.

    Args:
        config_path (str | Path | None): Configuration file the run used.
        source (SourceRef | None): Source configuration.
        target (SourceRef | None): Target configuration.

    Returns:
        list[_JSONObject]: Resource attributes in OTLP/JSON form.
    """
    attributes = [
        _attribute("service.name", SERVICE_NAME),
        _attribute("service.version", __version__),
    ]
    if config_path is not None:
        attributes.append(_attribute("veridelta.config.path", str(config_path)))
    for role, side in (("source", source), ("target", target)):
        if side is None:
            continue
        attributes.append(_attribute(f"veridelta.{role}.type", side.type))
        name = _side_name(side)
        if name is not None:
            attributes.append(_attribute(f"veridelta.{role}.name", name))
    return attributes


def _metrics(result: DiffResult, observed: int) -> list[_JSONObject]:
    """Measure one comparison.

    Args:
        result (DiffResult): Completed comparison.
        observed (int): Observation time, in nanoseconds since the epoch.

    Returns:
        list[_JSONObject]: Gauge metrics in OTLP/JSON form.
    """
    summary = result.summary
    metrics = [
        _gauge(
            "veridelta.dataset.rows",
            "Rows in each dataset.",
            _ROW_UNIT,
            [
                _point(summary.total_rows_source, observed, {"veridelta.side": "source"}),
                _point(summary.total_rows_target, observed, {"veridelta.side": "target"}),
            ],
        ),
        _gauge(
            "veridelta.diff.rows",
            "Rows only in the target (added), only in the source (removed), "
            "or in both with a differing value (changed).",
            _ROW_UNIT,
            [
                _point(summary.added_count, observed, {"veridelta.diff.kind": "added"}),
                _point(summary.removed_count, observed, {"veridelta.diff.kind": "removed"}),
                _point(summary.changed_count, observed, {"veridelta.diff.kind": "changed"}),
            ],
        ),
    ]
    # Every compared column gets a point, so one that stops drifting reads
    # zero instead of vanishing; `column_mismatches` lists drifting ones only.
    columns = list(dict.fromkeys([*result.compared_columns, *summary.column_mismatches]))
    if columns:
        metrics.append(
            _gauge(
                "veridelta.column.mismatched_rows",
                "Changed rows whose value in this column differs.",
                _ROW_UNIT,
                [
                    _point(
                        summary.column_mismatches.get(column, 0),
                        observed,
                        {"veridelta.column.name": column},
                    )
                    for column in columns
                ],
            )
        )
    metrics += [
        _gauge(
            "veridelta.diff.mismatch_ratio",
            "Added, removed, and changed rows as a share of the source rows.",
            "1",
            [_point(summary.mismatch_ratio, observed)],
        ),
        # Unitless: a backend such as Prometheus would read a unit of `1` as
        # a ratio and name the series accordingly.
        _gauge(
            "veridelta.diff.match",
            "1 when the comparison matched within its threshold, 0 when it drifted.",
            "",
            [_point(int(summary.is_match), observed)],
        ),
    ]
    return metrics


def render_otlp_metrics(
    result: DiffResult,
    *,
    config_path: str | Path | None = None,
    source: SourceRef | None = None,
    target: SourceRef | None = None,
    time_unix_nano: int | None = None,
) -> str:
    """Render a comparison as an OTLP/JSON metrics export.

    Args:
        result (DiffResult): Completed comparison.
        config_path (str | Path | None): Configuration file the run used,
            recorded as `veridelta.config.path`.
        source (SourceRef | None): Source configuration, recorded by type and
            by table or path.
        target (SourceRef | None): Target configuration, recorded likewise.
        time_unix_nano (int | None): Observation time, in nanoseconds since
            the epoch. Defaults to now.

    Returns:
        str: One line of JSON holding an `ExportMetricsServiceRequest`.
    """
    observed = time.time_ns() if time_unix_nano is None else time_unix_nano
    document = {
        "resourceMetrics": [
            {
                "resource": {"attributes": _resource_attributes(config_path, source, target)},
                "scopeMetrics": [
                    {
                        "scope": {"name": SERVICE_NAME, "version": __version__},
                        "metrics": _metrics(result, observed),
                    }
                ],
            }
        ]
    }
    # ASCII escapes keep the export on one line whatever a column is called.
    return json.dumps(document, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def write_otlp_metrics(
    result: DiffResult,
    path: str | Path,
    *,
    config_path: str | Path | None = None,
    source: SourceRef | None = None,
    target: SourceRef | None = None,
    time_unix_nano: int | None = None,
) -> Path:
    """Write a comparison's OTLP/JSON metrics export to disk.

    The file holds one line, ending in a newline, so it can be sent as is to
    an OTLP/HTTP endpoint or read by the Collector's `otlpjsonfile` receiver.

    Args:
        result (DiffResult): Completed comparison.
        path (str | Path): Destination file. Parent directories are created.
        config_path (str | Path | None): As for `render_otlp_metrics`.
        source (SourceRef | None): As for `render_otlp_metrics`.
        target (SourceRef | None): As for `render_otlp_metrics`.
        time_unix_nano (int | None): As for `render_otlp_metrics`.

    Returns:
        Path: The file that was written.
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    export = render_otlp_metrics(
        result,
        config_path=config_path,
        source=source,
        target=target,
        time_unix_nano=time_unix_nano,
    )
    destination.write_text(export + "\n", encoding="utf-8")
    return destination


__all__: Final[list[str]] = [
    "SERVICE_NAME",
    "render_otlp_metrics",
    "write_otlp_metrics",
]
