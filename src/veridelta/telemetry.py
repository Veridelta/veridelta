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

The standard `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` variables add
resource attributes, as they do for an OpenTelemetry SDK.
"""

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Final
from urllib.parse import unquote_to_bytes, urlsplit, urlunsplit

from veridelta import __version__
from veridelta.models import DeltaLakeConfig, DiffResult, IcebergConfig, SourceConfig, SourceRef

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

SERVICE_NAME: Final[str] = "veridelta"
"""`service.name` resource attribute and instrumentation scope name."""

_ROW_UNIT: Final[str] = "{row}"
"""UCUM annotation for a count of rows; backends treat it as dimensionless."""

_CONTAINER_SCHEMES: Final[frozenset[str]] = frozenset({"abfs", "abfss", "wasb", "wasbs"})
"""Azure schemes whose `container@account` user part names a container, not a login."""

_BAD_ESCAPE: Final[re.Pattern[str]] = re.compile(r"%(?![0-9A-Fa-f]{2})")
"""A `%` that does not start a two-digit hexadecimal escape."""

_JSONObject = dict[str, object]


def _attribute(key: str, value: str) -> _JSONObject:
    """Build one OTLP key-value attribute holding a string."""
    return {"key": key, "value": {"stringValue": value}}


def _point(
    value: int | float, observed: int, attributes: dict[str, str] | None = None
) -> _JSONObject:
    """Build one gauge data point."""
    point: _JSONObject = {}
    if attributes:
        point["attributes"] = [_attribute(key, item) for key, item in attributes.items()]
    # The OTLP JSON mapping writes 64-bit integers, such as `asInt`, as strings.
    point["timeUnixNano"] = str(observed)
    if isinstance(value, int):
        point["asInt"] = str(value)
    else:
        point["asDouble"] = value
    return point


def _gauge(name: str, description: str, unit: str, points: list[_JSONObject]) -> _JSONObject:
    """Build one gauge metric."""
    metric: _JSONObject = {"name": name, "description": description}
    if unit:
        metric["unit"] = unit
    metric["gauge"] = {"dataPoints": points}
    return metric


def _locator(text: str) -> str | None:
    """Reduce a path or URL to what may leave the run."""
    try:
        parts = urlsplit(text)
    except ValueError:
        return None
    if not (parts.scheme and parts.netloc):
        return text
    # The user part can hold a credential, such as a token, and a pre-signed object-store
    # link carries a signature in its query.
    user, _, host = parts.netloc.rpartition("@")
    if user and parts.scheme in _CONTAINER_SCHEMES and ":" not in user:
        host = f"{user}@{host}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def _side_name(side: SourceRef) -> str | None:
    """Name a source by its table or path, without anything secret."""
    if isinstance(side, SourceConfig):
        return _locator(side.path)
    if isinstance(side, (DeltaLakeConfig, IcebergConfig)):
        return _locator(side.table_uri)
    # Warehouse and database tables are validated identifiers, safe to export as configured.
    # A database `query` has no table, so neither its SQL nor its `uri` leaves the run.
    return side.table


def _decoded(text: str) -> str:
    """Trim and percent-decode one key or value, raising ValueError if it is malformed."""
    text = text.strip()
    if _BAD_ESCAPE.search(text):
        raise ValueError
    return unquote_to_bytes(text).decode()


def _parsed_attributes(text: str) -> dict[str, str]:
    """Parse comma-separated `key=value` items, raising ValueError on any malformed one."""
    attributes: dict[str, str] = {}
    for item in filter(str.strip, text.split(",")):
        key, separator, value = item.partition("=")
        if not separator or not key.strip():
            raise ValueError
        attributes[_decoded(key)] = _decoded(value)
    return attributes


def _environment_attributes() -> dict[str, str]:
    """Read the resource attributes the OpenTelemetry environment variables set."""
    try:
        attributes = _parsed_attributes(os.environ.get("OTEL_RESOURCE_ATTRIBUTES", ""))
    except ValueError:
        # The specification asks to discard the whole variable, and the value may hold a secret.
        logger.warning(
            "Ignoring OTEL_RESOURCE_ATTRIBUTES: it must hold comma-separated key=value items, "
            "percent-encoded as UTF-8."
        )
        attributes = {}
    if service_name := os.environ.get("OTEL_SERVICE_NAME"):
        attributes["service.name"] = service_name
    return attributes


def _resource_attributes(
    config_path: str | Path | None, source: SourceRef | None, target: SourceRef | None
) -> list[_JSONObject]:
    """Describe which comparison ran, after any attributes the environment sets."""
    # The environment may rename the service, but Veridelta's own attributes win.
    attributes = {"service.name": SERVICE_NAME, **_environment_attributes()}
    attributes["service.version"] = __version__
    if config_path is not None:
        attributes["veridelta.config.path"] = str(config_path)
    for role, side in (("source", source), ("target", target)):
        if side is None:
            continue
        attributes[f"veridelta.{role}.type"] = side.type
        name = _side_name(side)
        if name is not None:
            attributes[f"veridelta.{role}.name"] = name
    return [_attribute(key, value) for key, value in attributes.items()]


def _metrics(result: DiffResult, observed: int) -> list[_JSONObject]:
    """Measure one comparison."""
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

    The file holds one line, ending in a newline, so it can be sent as is to an
    OTLP/HTTP endpoint or read by the Collector's `otlpjsonfile` receiver.

    Args:
        result (DiffResult): Completed comparison.
        path (str | Path): Destination file. Parent directories are created.
        config_path (str | Path | None): As for `render_otlp_metrics`.
        source (SourceRef | None): As for `render_otlp_metrics`.
        target (SourceRef | None): As for `render_otlp_metrics`.
        time_unix_nano (int | None): As for `render_otlp_metrics`.

    Returns:
        Path: The file that was written.

    Examples:
        >>> import polars as pl
        >>> from veridelta.engine import DiffEngine
        >>> from veridelta.models import DiffConfig
        >>> source = pl.LazyFrame({"id": [1, 2], "amount": [10.0, 20.0]})
        >>> target = pl.LazyFrame({"id": [1, 2], "amount": [10.0, 21.5]})
        >>> result = DiffEngine(DiffConfig(primary_keys=["id"]), source, target).run()
        >>> import json
        >>> export = json.loads(render_otlp_metrics(result, time_unix_nano=0))
        >>> [
        ...     metric["name"]
        ...     for metric in export["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        ... ]
        ['veridelta.dataset.rows', 'veridelta.diff.rows',
         'veridelta.column.mismatched_rows', 'veridelta.diff.mismatch_ratio',
         'veridelta.diff.match']
        >>> path = write_otlp_metrics(result, "metrics.json")  # doctest: +SKIP
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
    # A bare newline everywhere, so Windows writes the same bytes as Linux.
    destination.write_text(export + "\n", encoding="utf-8", newline="\n")
    return destination


__all__: Final[list[str]] = [
    "SERVICE_NAME",
    "render_otlp_metrics",
    "write_otlp_metrics",
]
