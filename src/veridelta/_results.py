# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Shape what a comparison returns: its summary, its artifact files, and its accepted drift.

A local run and a pushed-down run both end here, so their summaries and files
agree. A baseline moves the rows it names out of the counts and into `accepted`.
"""

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Final, NamedTuple

import polars as pl

from veridelta.exceptions import ConfigError
from veridelta.models import (
    AcceptedChange,
    ArtifactFormat,
    Baseline,
    DiffConfig,
    DiffSummary,
    mismatch_ratio_of,
)


def local_column_mismatches(changed: pl.DataFrame, compared_columns: list[str]) -> dict[str, int]:
    """Count, per compared column, how many changed rows failed its match flag."""
    if not compared_columns or changed.is_empty():
        return {}
    tally = changed.select(
        [(~pl.col(f"{col}_is_match")).sum().alias(col) for col in compared_columns]
    ).row(0, named=True)
    return {column: count for column, count in tally.items() if count > 0}


_ARTIFACT_WRITERS: Final[dict[ArtifactFormat, Callable[[pl.DataFrame, Path], None]]] = {
    "csv": lambda frame, path: frame.write_csv(path),
    "parquet": lambda frame, path: frame.write_parquet(path),
    "json": lambda frame, path: frame.write_json(path),
    "ndjson": lambda frame, path: frame.write_ndjson(path),
    "arrow": lambda frame, path: frame.write_ipc(path),
}
"""Artifact format to writer."""


_TEXT_FORMATS: Final[frozenset[ArtifactFormat]] = frozenset({"csv", "json", "ndjson"})
"""Artifact formats with no type for bytes."""


def _holds_binary(dtype: object) -> bool:
    """Return whether a type is binary, or nests a binary type anywhere inside it.

    A nested type names its members as instances or as bare classes, so both count.
    """
    if dtype == pl.Binary:
        return True
    if isinstance(dtype, (pl.List, pl.Array)):
        return _holds_binary(dtype.inner)
    if isinstance(dtype, pl.Struct):
        return any(_holds_binary(field.dtype) for field in dtype.fields)
    return False


def _writable(frame: pl.DataFrame, output_format: ArtifactFormat) -> pl.DataFrame:
    """Write each binary column as hexadecimal text in a format that has no bytes type.

    Polars refuses a binary column in CSV and panics on one in JSON, a panic no
    `except Exception` catches, so the bytes become text first.

    Raises:
        ConfigError: If a column nests binary values in a list or a struct,
            which a text format cannot hold.
    """
    if output_format not in _TEXT_FORMATS:
        return frame
    binary: list[str] = []
    for name, dtype in frame.schema.items():
        if isinstance(dtype, pl.Binary):
            binary.append(name)
        elif _holds_binary(dtype):
            raise ConfigError(
                f"Column '{name}' nests binary values, which output_format '{output_format}' "
                "cannot hold. Set output_format to parquet or arrow."
            )
    return frame.with_columns(pl.col(binary).bin.encode("hex")) if binary else frame


def export_artifacts(
    frames: dict[str, pl.DataFrame], output_path: str | None, output_format: ArtifactFormat
) -> bool:
    """Persist non-empty discrepancy frames to the configured directory."""
    if output_path is None:
        return False
    # Checked before the loop so a misconfigured format fails the same way on a
    # clean run as on a drifted one, rather than only when a frame reaches disk.
    if output_format not in _ARTIFACT_WRITERS:
        supported = ", ".join(sorted(_ARTIFACT_WRITERS))
        raise ConfigError(
            f"Artifact format '{output_format}' has no writer. Supported formats: {supported}."
        )

    out_dir = Path(output_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    written = False
    for name, frame in frames.items():
        if frame.height == 0:
            continue
        _ARTIFACT_WRITERS[output_format](
            _writable(frame, output_format), out_dir / f"{name}.{output_format}"
        )
        written = True
    return written


def build_summary(
    diff: DiffConfig,
    changed: pl.DataFrame,
    added: pl.DataFrame,
    removed: pl.DataFrame,
    source_total: int,
    target_total: int,
    column_mismatches: dict[str, int],
    artifacts_written: bool,
    accepted_count: int = 0,
) -> DiffSummary:
    """Count a comparison's discrepancies and apply the threshold, for either engine."""
    changed_count = changed.height
    added_count = added.height
    removed_count = removed.height
    total_mismatches = added_count + removed_count + changed_count
    return DiffSummary(
        total_rows_source=source_total,
        total_rows_target=target_total,
        added_count=added_count,
        removed_count=removed_count,
        changed_count=changed_count,
        column_mismatches=column_mismatches,
        is_match=mismatch_ratio_of(total_mismatches, source_total) <= diff.threshold,
        accepted_count=accepted_count,
        report_limit=diff.report_top_columns_limit,
        artifacts_written=artifacts_written,
    )


def _baseline_keys(
    keys: Sequence[Mapping[str, Any]], names: list[str], schema: pl.Schema
) -> pl.DataFrame:
    """Build a baseline's keys as a frame, typed as the result's key columns.

    JSON has no type for a date or a timestamp, so a key read as text casts to the
    column's type, and one that cannot cast matches no row.
    """
    frame = pl.DataFrame({name: [key[name] for key in keys] for name in names}, strict=False)
    return frame.select(pl.col(name).cast(schema[name], strict=False) for name in names)


def _unaccepted_rows(
    frame: pl.DataFrame, keys: Sequence[Mapping[str, Any]], names: list[str]
) -> pl.DataFrame:
    """Drop the rows of an added or removed frame whose key a baseline lists."""
    if not keys or frame.is_empty():
        return frame
    accepted = _baseline_keys(keys, names, frame.schema)
    return frame.join(accepted, on=names, how="anti", nulls_equal=True)


def _unaccepted_changes(
    changed: pl.DataFrame,
    entries: Sequence[AcceptedChange],
    names: list[str],
    compared_columns: list[str],
) -> pl.DataFrame:
    """Mark accepted columns as matching on their rows, and drop rows left matching."""
    if not entries or changed.is_empty():
        return changed
    accepted = (
        _baseline_keys([entry.key for entry in entries], names, changed.schema)
        .with_columns(
            pl.Series(
                _ACCEPTED_COLUMNS, [entry.columns for entry in entries], dtype=pl.List(pl.String)
            )
        )
        # A key listed twice keeps one row, with every column either entry names.
        .group_by(names)
        .agg(pl.col(_ACCEPTED_COLUMNS).list.explode(keep_nulls=False, empty_as_null=False))
    )
    flags = [f"{column}_is_match" for column in compared_columns]
    return (
        changed.join(accepted, on=names, how="left", nulls_equal=True)
        .with_columns(
            (
                pl.col(flag)
                | pl.col(_ACCEPTED_COLUMNS).list.contains(column).fill_null(value=False)
            ).alias(flag)
            for column, flag in zip(compared_columns, flags, strict=True)
        )
        .drop(_ACCEPTED_COLUMNS)
        .filter(~pl.all_horizontal(flags))
    )


_ACCEPTED_COLUMNS: Final = "__veridelta_accepted_columns"
"""A working column for the columns a baseline accepts on each changed row."""


def _listed(
    frame: pl.DataFrame, keys: Sequence[Mapping[str, Any]], names: list[str]
) -> pl.DataFrame:
    """Keep the rows of a frame whose key a baseline lists."""
    if not keys or frame.is_empty():
        return frame.clear()
    return frame.join(
        _baseline_keys(keys, names, frame.schema), on=names, how="semi", nulls_equal=True
    )


class _Acceptance(NamedTuple):
    """The drift a baseline left in a run, and what it accepted."""

    added: pl.DataFrame
    removed: pl.DataFrame
    changed: pl.DataFrame
    rows: int
    accepted: Baseline


def accept_baseline(
    baseline: Baseline,
    names: list[str],
    frames: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame],
    compared_columns: list[str],
) -> _Acceptance:
    """Leave the drift a baseline lists out of the added, removed, and changed rows.

    Returns:
        _Acceptance: The rows left, how many rows the baseline accepted, and what
            it accepted, as the rows it matched, so a saved baseline keeps it.

    Raises:
        ConfigError: If the baseline names other primary keys than the run.
    """
    if baseline.primary_keys != names:
        raise ConfigError(
            f"The baseline lists rows by the primary keys {baseline.primary_keys}, and "
            f"this configuration compares on {names}. Save the baseline again from a run "
            "of this configuration."
        )
    added, removed, changed = frames
    kept = (
        _unaccepted_rows(added, baseline.added, names),
        _unaccepted_rows(removed, baseline.removed, names),
        _unaccepted_changes(changed, baseline.changed, names, compared_columns),
    )
    rows = sum(before.height - after.height for before, after in zip(frames, kept, strict=True))
    listed = [
        [*baseline.added],
        [*baseline.removed],
        [entry.key for entry in baseline.changed],
    ]
    before, after = (
        Baseline.of_rows(
            names,
            (
                _listed(side[0], listed[0], names),
                _listed(side[1], listed[1], names),
                _listed(side[2], listed[2], names),
            ),
            compared_columns,
        )
        for side in (frames, kept)
    )
    return _Acceptance(*kept, rows, before.without(after))
