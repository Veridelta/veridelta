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

`send_otlp_metrics` posts the same export to an OTLP/HTTP endpoint, which the
standard `OTEL_EXPORTER_OTLP_*` variables configure. Their headers often carry
an API key, so no header value reaches a log line or an error, and a redirect
is refused rather than followed to a second host.
"""

import http.client
import ipaddress
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from email.message import Message
from pathlib import Path
from typing import IO, Final, cast
from urllib.parse import unquote_to_bytes, urlsplit, urlunsplit

from veridelta import __version__
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import (
    DeltaLakeConfig,
    DiffResult,
    IcebergConfig,
    SourceConfig,
    SourceRef,
    redacted_location,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

SERVICE_NAME: Final[str] = "veridelta"
"""`service.name` resource attribute and instrumentation scope name."""

_ROW_UNIT: Final[str] = "{row}"
"""UCUM annotation for a count of rows; backends treat it as dimensionless."""

_BAD_ESCAPE: Final[re.Pattern[str]] = re.compile(r"%(?![0-9A-Fa-f]{2})")
"""A `%` that does not start a two-digit hexadecimal escape."""

_DEFAULT_ENDPOINT: Final[str] = "http://localhost:4318"
"""Base URL an OTLP/HTTP exporter sends to when none is set, as the specification gives it."""

_METRICS_PATH: Final[str] = "v1/metrics"
"""Path the specification appends to a base endpoint for metrics."""

_DEFAULT_TIMEOUT_MS: Final[int] = 10_000
"""Milliseconds to wait for the endpoint when no timeout is set, as the specification gives it."""

_JSON_PROTOCOL: Final[str] = "http/json"
"""The one OTLP protocol Veridelta sends: HTTP, with the export as JSON."""

_HEADER_NAME: Final[re.Pattern[str]] = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
"""An HTTP header name: one token, as RFC 9110 defines it."""

_HEADER_BREAK: Final[re.Pattern[str]] = re.compile(r"[\r\n\x00]")
"""A character that would end a header value early, or that HTTP refuses in one."""

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


def _side_name(side: SourceRef) -> str | None:
    """Name a source by its table or path, without anything secret."""
    if isinstance(side, SourceConfig):
        return redacted_location(side.path)
    if isinstance(side, (DeltaLakeConfig, IcebergConfig)):
        return redacted_location(side.table_uri)
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


def _otlp_variable(option: str) -> tuple[str, str] | None:
    """Return the variable that sets an exporter option, and its value.

    The metrics variable wins over the general one, and an empty value counts
    as unset, as the specification requires.
    """
    for name in (f"OTEL_EXPORTER_OTLP_METRICS_{option}", f"OTEL_EXPORTER_OTLP_{option}"):
        value = os.environ.get(name, "").strip()
        if value:
            return name, value
    return None


def _otlp_endpoint() -> str:
    """Return the URL to post metrics to: the metrics endpoint as written, or a base plus its path."""
    specific = os.environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "").strip()
    if specific:
        name, endpoint = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", specific
    else:
        name = "OTEL_EXPORTER_OTLP_ENDPOINT"
        base = urlsplit(os.environ.get(name, "").strip() or _DEFAULT_ENDPOINT)
        endpoint = urlunsplit(base._replace(path=f"{base.path.rstrip('/')}/{_METRICS_PATH}"))
    parts = urlsplit(endpoint)
    try:
        port_ok = parts.port is None or parts.port > 0
    except ValueError:
        port_ok = False
    if parts.scheme not in {"http", "https"} or not parts.hostname or not port_ok:
        raise ConfigError(
            f"{name} must be an http:// or https:// URL, such as {_DEFAULT_ENDPOINT}."
        )
    if parts.username is not None:
        # The standard library cannot send one, and its error would repeat it.
        raise ConfigError(
            f"{name} must hold no user name or password. Send credentials as headers, in "
            "OTEL_EXPORTER_OTLP_HEADERS."
        )
    return endpoint


def _otlp_headers() -> dict[str, str]:
    """Return the request headers the exporter variables set, never repeating a value."""
    setting = _otlp_variable("HEADERS")
    if setting is None:
        return {}
    name, text = setting
    message = (
        f"{name} must hold comma-separated key=value items, percent-encoded as UTF-8, "
        "with a valid HTTP header name in each key."
    )
    try:
        headers = _parsed_attributes(text)
    except ValueError:
        # A value is often an API key, so the error never quotes the variable.
        raise ConfigError(message) from None
    for key, value in headers.items():
        if _HEADER_NAME.fullmatch(key) is None or _HEADER_BREAK.search(value):
            raise ConfigError(message)
    return headers


def _otlp_timeout() -> float:
    """Return the seconds to wait for the endpoint, from a timeout set in milliseconds."""
    setting = _otlp_variable("TIMEOUT")
    if setting is None:
        return _DEFAULT_TIMEOUT_MS / 1000
    name, text = setting
    # `isdigit` alone accepts digits such as `²`, which `int` then refuses.
    if not (text.isascii() and text.isdigit()) or int(text) == 0:
        raise ConfigError(f"{name} must be a whole number of milliseconds above 0, such as 10000.")
    return int(text) / 1000


def _check_otlp_protocol() -> None:
    """Refuse an OTLP protocol other than the one Veridelta sends."""
    setting = _otlp_variable("PROTOCOL")
    if setting is not None and setting[1] != _JSON_PROTOCOL:
        name, protocol = setting
        raise ConfigError(
            f"{name} is '{protocol}', but Veridelta sends metrics only as {_JSON_PROTOCOL}. "
            f"Set it to {_JSON_PROTOCOL}, or unset it."
        )


def _on_this_machine(host: str) -> bool:
    """Return whether a host name is this machine: `localhost`, or a loopback address."""
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class _RefuseRedirects(urllib.request.HTTPRedirectHandler):
    """Refuse every redirect, so a header such as an API key never reaches a second host."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> urllib.request.Request | None:
        """Return no request to follow, which turns the redirect into an HTTP error."""
        _ = (req, fp, code, msg, headers, newurl)
        return None


def send_otlp_metrics(
    result: DiffResult,
    *,
    config_path: str | Path | None = None,
    source: SourceRef | None = None,
    target: SourceRef | None = None,
    time_unix_nano: int | None = None,
) -> str:
    """Send a comparison's OTLP/JSON metrics export to an OTLP/HTTP endpoint.

    The standard variables configure the send. `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`
    is the URL as written; otherwise `/v1/metrics` follows `OTEL_EXPORTER_OTLP_ENDPOINT`,
    which defaults to `http://localhost:4318`. The `HEADERS`, `TIMEOUT`, and
    `PROTOCOL` variables follow the same pattern, with the metrics variable
    winning over the general one. The send is one attempt, with no retry.

    Args:
        result (DiffResult): Completed comparison.
        config_path (str | Path | None): As for `render_otlp_metrics`.
        source (SourceRef | None): As for `render_otlp_metrics`.
        target (SourceRef | None): As for `render_otlp_metrics`.
        time_unix_nano (int | None): As for `render_otlp_metrics`.

    Returns:
        str: The endpoint the export reached, with no user part or query.

    Raises:
        ConfigError: If a variable holds an unusable value, or names a
            protocol other than `http/json`.
        ConnectorError: If the endpoint cannot be reached, redirects, or
            answers with an HTTP error.

    Examples:
        >>> import polars as pl
        >>> from veridelta.engine import DiffEngine
        >>> from veridelta.models import DiffConfig
        >>> source = pl.LazyFrame({"id": [1, 2], "amount": [10.0, 20.0]})
        >>> target = pl.LazyFrame({"id": [1, 2], "amount": [10.0, 21.5]})
        >>> result = DiffEngine(DiffConfig(primary_keys=["id"]), source, target).run()
        >>> send_otlp_metrics(result, config_path="veridelta.yaml")  # doctest: +SKIP
        'http://localhost:4318/v1/metrics'
    """
    _check_otlp_protocol()
    endpoint = _otlp_endpoint()
    headers = _otlp_headers()
    timeout = _otlp_timeout()
    export = render_otlp_metrics(
        result,
        config_path=config_path,
        source=source,
        target=target,
        time_unix_nano=time_unix_nano,
    )
    # A query can hold a credential too, so messages name the endpoint without one.
    where = cast("str", redacted_location(endpoint))
    parts = urlsplit(endpoint)
    if headers and parts.scheme == "http" and not _on_this_machine(parts.hostname or ""):
        # Headers often carry an API key, which plain http sends readable on the way.
        logger.warning(
            "Sending OTLP headers to %s over plain http, so anyone on the network path can "
            "read them. Use an https:// endpoint.",
            where,
        )
    request = urllib.request.Request(
        endpoint,
        data=export.encode("utf-8"),
        headers={**headers, "Content-Type": "application/json"},
        method="POST",
    )
    opener = urllib.request.build_opener(_RefuseRedirects())
    started = time.perf_counter()
    try:
        with opener.open(request, timeout=timeout) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        # The body is left out: a server can echo the request it refused.
        answer = (
            "a redirect, which Veridelta does not follow; set the final URL instead"
            if 300 <= exc.code < 400
            else f"HTTP {exc.code} {exc.reason}"
        )
        failure = f"the endpoint answered with {answer}"
        # The error holds the answer open, and Python 3.14 warns when one is never closed.
        exc.close()
    except TimeoutError:
        failure = f"no answer came within {timeout:g} seconds"
    except urllib.error.URLError as exc:
        reason = exc.reason
        failure = (
            f"no answer came within {timeout:g} seconds"
            if isinstance(reason, TimeoutError)
            else str(reason)
        )
    except (OSError, http.client.HTTPException) as exc:
        # Raised while reading the answer, such as a connection closed without one.
        failure = str(exc) or type(exc).__name__
    else:
        logger.info("Sent metrics to %s in %.3fs", where, time.perf_counter() - started)
        return where
    logger.warning("Sending metrics to %s failed after %.3fs", where, time.perf_counter() - started)
    raise ConnectorError(f"Sending metrics to {where} failed: {failure}.")


__all__: Final[list[str]] = [
    "SERVICE_NAME",
    "render_otlp_metrics",
    "send_otlp_metrics",
    "write_otlp_metrics",
]
