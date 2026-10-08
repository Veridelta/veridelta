# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Seed known drift into a clean table, and keep a ledger of what a run must report.

The parity suite proves that the local engine and pushdown agree. It cannot
tell when both are wrong. Here each case starts from one clean table and
applies seeds whose effect is known by construction. A `Ledger` records that
effect: the keys only the target has, the keys only the source has, and the
columns that differ on each changed key. `grade` then holds a result to the
ledger, field by field.

A seed a rule should forgive records nothing, so a rule that fails to forgive
it shows up as an unexpected change. A seed no rule covers records its keys,
so an engine that misses it shows up as a missing change. Each seed takes rows
no other seed touches, unless a case asks for the same rows on purpose.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Final, Literal

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from veridelta.models import DiffConfig, DiffResult

KEY: Final = "id"
"""The primary key of every seeded table."""

ROWS: Final = 60
"""Rows in the clean table."""

LEGACY_FORMAT: Final = "%d/%m/%Y %H:%M"
"""How a legacy system writes `placed_at` as text."""

_SCHEMA: Final = {
    KEY: pl.Int64(),
    "amount": pl.Float64(),
    "quantity": pl.Int64(),
    "status": pl.String(),
    "region": pl.String(),
    "note": pl.String(),
    "placed_at": pl.Datetime("us"),
}

_STATUSES: Final = ("open", "shipped", "closed")
_REGIONS: Final = ("north", "south", "east", "west")

Effect = Literal["added", "removed", "changed", "forgiven"]
"""What a seed's rows should look like in the result."""


def clean_table(rows: int = ROWS, seed: int = 7) -> pl.DataFrame:
    """Return the table every case starts from, the same on every call.

    Args:
        rows (int): Rows to generate, keyed `0` to `rows - 1`.
        seed (int): Seed for the values.

    Returns:
        pl.DataFrame: An order table with no NULL anywhere.
    """
    rng = random.Random(seed)
    start = datetime(2026, 1, 1)
    return pl.DataFrame(
        {
            KEY: list(range(rows)),
            "amount": [round(rng.uniform(1, 500), 2) for _ in range(rows)],
            "quantity": [rng.randint(1, 50) for _ in range(rows)],
            "status": [rng.choice(_STATUSES) for _ in range(rows)],
            "region": [rng.choice(_REGIONS) for _ in range(rows)],
            "note": [f"order {index}" for index in range(rows)],
            "placed_at": [start + timedelta(minutes=rng.randint(0, 129_600)) for _ in range(rows)],
        },
        schema=_SCHEMA,
    )


def _set(frame: pl.DataFrame, keys: Sequence[int], column: str, value: pl.Expr) -> pl.DataFrame:
    """Replace `column` with `value` on the rows whose key is in `keys`."""
    return frame.with_columns(
        pl.when(pl.col(KEY).is_in(list(keys))).then(value).otherwise(pl.col(column)).alias(column)
    )


@dataclass(frozen=True)
class Seed:
    """One kind of drift, applied to rows the case gives it.

    Attributes:
        name (str): What the seed does, for failure messages.
        count (int): Rows it takes.
        effect (Effect): What its rows should look like in the result.
        column (str | None): The column it changes, for a `changed` seed.
        apply (Callable): Takes the source, the target, and its keys, and
            returns both sides with the drift in place.
        new_keys (bool): Whether it adds keys instead of taking existing ones.
        same_rows_as (int | None): Index of an earlier seed whose rows it
            reuses, to change two columns on one row.
    """

    name: str
    count: int
    effect: Effect
    apply: Callable[[pl.DataFrame, pl.DataFrame, list[int]], tuple[pl.DataFrame, pl.DataFrame]]
    column: str | None = None
    new_keys: bool = False
    same_rows_as: int | None = None


def delete(count: int) -> Seed:
    """Remove rows from the target, so they are only in the source."""
    return Seed(
        f"delete {count} rows",
        count,
        "removed",
        lambda source, target, keys: (source, target.filter(~pl.col(KEY).is_in(keys))),
    )


def insert(count: int) -> Seed:
    """Add rows only the target has, under keys the source never used."""

    def apply(
        source: pl.DataFrame, target: pl.DataFrame, keys: list[int]
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        extra = source.head(len(keys)).with_columns(pl.Series(KEY, keys, dtype=pl.Int64()))
        return source, pl.concat([target, extra])

    return Seed(f"insert {count} rows", count, "added", apply, new_keys=True)


def change(
    column: str,
    count: int,
    value: Callable[[], pl.Expr],
    name: str,
    *,
    forgiven: bool = False,
    same_rows_as: int | None = None,
) -> Seed:
    """Change one target column on `count` rows.

    Args:
        column (str): The column to change.
        count (int): Rows to change.
        value (Callable[[], pl.Expr]): Builds the new target value.
        name (str): What the change is, for failure messages.
        forgiven (bool): Whether the case's rules should forgive it.
        same_rows_as (int | None): Index of an earlier seed whose rows to reuse.

    Returns:
        Seed: The change.
    """
    return Seed(
        name,
        count,
        "forgiven" if forgiven else "changed",
        lambda source, target, keys: (source, _set(target, keys, column, value())),
        column=column,
        same_rows_as=same_rows_as,
    )


def null_both(column: str, count: int, *, forgiven: bool) -> Seed:
    """Make `column` NULL on both sides, which matches only under `treat_null_as_equal`."""
    return Seed(
        f"NULL {column} on both sides",
        count,
        "forgiven" if forgiven else "changed",
        lambda source, target, keys: (
            _set(source, keys, column, pl.lit(None, dtype=source.schema[column])),
            _set(target, keys, column, pl.lit(None, dtype=target.schema[column])),
        ),
        column=column,
    )


def sentinel(column: str, count: int, token: str, *, forgiven: bool) -> Seed:
    """Make `column` NULL in the source and `token` in the target, as a legacy export writes it."""
    return Seed(
        f"{token!r} in the target for a source NULL in {column}",
        count,
        "forgiven" if forgiven else "changed",
        lambda source, target, keys: (
            _set(source, keys, column, pl.lit(None, dtype=pl.String())),
            _set(target, keys, column, pl.lit(token)),
        ),
        column=column,
    )


def duplicate_key() -> Seed:
    """Repeat one source key, which no comparison may pair."""
    return Seed(
        "repeat one source key",
        1,
        "forgiven",
        lambda source, target, keys: (
            pl.concat([source, source.filter(pl.col(KEY).is_in(keys))]),
            target,
        ),
    )


@dataclass(frozen=True)
class Reshape:
    """A change to a whole column, made after the seeds.

    Attributes:
        name (str): What it does, for failure messages.
        apply (Callable): Takes both sides and returns them reshaped.
        changes (str | None): A column that now differs on every paired row,
            when no rule covers the reshape.
        compared (Callable): Maps the columns a run would compare without the
            reshape to those it compares with it.
        renamed (tuple[str, str] | None): A source column and the target name
            it is now compared and reported under.
    """

    name: str
    apply: Callable[[pl.DataFrame, pl.DataFrame], tuple[pl.DataFrame, pl.DataFrame]]
    changes: str | None = None
    compared: Callable[[frozenset[str]], frozenset[str]] = lambda columns: columns
    renamed: tuple[str, str] | None = None


def legacy_dates(*, forgiven: bool) -> Reshape:
    """Write the source's `placed_at` as legacy text, as a system before the migration did."""
    return Reshape(
        f"source placed_at as {LEGACY_FORMAT!r} text",
        lambda source, target: (
            source.with_columns(pl.col("placed_at").dt.strftime(LEGACY_FORMAT)),
            target.with_columns(pl.col("placed_at").dt.truncate("1m")),
        ),
        changes=None if forgiven else "placed_at",
    )


def rename_target(old: str, new: str) -> Reshape:
    """Store a target column under a new name."""
    return Reshape(
        f"target {old} renamed to {new}",
        lambda source, target: (source, target.rename({old: new})),
        compared=lambda columns: (columns - {old}) | {new},
        renamed=(old, new),
    )


def drop_target(column: str) -> Reshape:
    """Leave a column out of the target."""
    return Reshape(
        f"target without {column}",
        lambda source, target: (source, target.drop(column)),
        compared=lambda columns: columns - {column},
    )


@dataclass(frozen=True)
class Ledger:
    """What a comparison of the seeded pair must report.

    Attributes:
        added (frozenset[int]): Keys only the target has.
        removed (frozenset[int]): Keys only the source has.
        changed (Mapping[int, frozenset[str]]): For each changed key, the
            columns that differ on it.
        compared (frozenset[str]): Columns the run compares.
    """

    added: frozenset[int]
    removed: frozenset[int]
    changed: Mapping[int, frozenset[str]]
    compared: frozenset[str]

    @property
    def column_mismatches(self) -> dict[str, int]:
        """Return the changed rows per column."""
        counts: dict[str, int] = {}
        for columns in self.changed.values():
            for column in columns:
                counts[column] = counts.get(column, 0) + 1
        return counts

    @property
    def is_match(self) -> bool:
        """Return whether nothing should differ."""
        return not (self.added or self.removed or self.changed)

    def keys_changed_in(self, column: str) -> frozenset[int]:
        """Return the keys whose `column` should differ."""
        return frozenset(key for key, columns in self.changed.items() if column in columns)


@dataclass(frozen=True)
class SeededPair:
    """A clean table, its seeded copy, and the ledger of what was seeded.

    Attributes:
        source (pl.DataFrame): The source side.
        target (pl.DataFrame): The target side.
        ledger (Ledger): What a comparison must report.
    """

    source: pl.DataFrame
    target: pl.DataFrame
    ledger: Ledger


@dataclass(frozen=True)
class Case:
    """A configuration, the seeds applied to a clean table, and any whole-column reshapes.

    Attributes:
        name (str): The test id.
        config (DiffConfig): What the run declares.
        seeds (tuple[Seed, ...]): Drift applied to disjoint rows, in order.
        reshapes (tuple[Reshape, ...]): Whole-column changes after the seeds.
        ignored (frozenset[str]): Columns the configuration leaves out.
        pushdown_refuses (str | None): The column pushdown refuses to compare,
            when the case holds a pair of types only a local run converts.
    """

    name: str
    config: DiffConfig
    seeds: tuple[Seed, ...] = ()
    reshapes: tuple[Reshape, ...] = ()
    ignored: frozenset[str] = field(default_factory=frozenset)
    pushdown_refuses: str | None = None

    def build(self, seed: int = 7) -> SeededPair:
        """Seed a clean table and record what the seeds should report.

        Args:
            seed (int): Seed for the table and for which rows each seed takes.

        Returns:
            SeededPair: Both sides and their ledger.
        """
        source = clean_table(seed=seed)
        target = source.clone()
        keys = _Keys(seed)
        draft = _Draft()
        for entry in self.seeds:
            taken = keys.take(entry)
            source, target = entry.apply(source, target, taken)
            draft.record(entry, taken)

        compared = frozenset(_SCHEMA) - {KEY} - self.ignored
        for reshape in self.reshapes:
            source, target = reshape.apply(source, target)
            compared = reshape.compared(compared)
            draft.reshape(reshape, set(source[KEY].to_list()) & set(target[KEY].to_list()))
        return SeededPair(source, target, draft.freeze(compared))


class _Keys:
    """Hands each seed rows no earlier seed took, or new keys, or an earlier seed's rows."""

    def __init__(self, seed: int) -> None:
        self._free = list(range(ROWS))
        random.Random(seed).shuffle(self._free)
        self._next_new = ROWS
        self._taken: list[list[int]] = []

    def take(self, entry: Seed) -> list[int]:
        """Return the keys `entry` applies to."""
        if entry.same_rows_as is not None:
            keys = self._taken[entry.same_rows_as][: entry.count]
        elif entry.new_keys:
            keys = list(range(self._next_new, self._next_new + entry.count))
            self._next_new += entry.count
        else:
            keys, self._free = self._free[: entry.count], self._free[entry.count :]
        self._taken.append(keys)
        return keys


@dataclass
class _Draft:
    """The ledger while seeds and reshapes apply, before `freeze` fixes it."""

    added: set[int] = field(default_factory=set[int])
    removed: set[int] = field(default_factory=set[int])
    changed: dict[int, set[str]] = field(default_factory=dict[int, set[str]])

    def record(self, entry: Seed, keys: Sequence[int]) -> None:
        """Record what one seed did to its keys."""
        if entry.effect == "added":
            self.added.update(keys)
        elif entry.effect == "removed":
            self.removed.update(keys)
        elif entry.effect == "changed" and entry.column is not None:
            for key in keys:
                self.changed.setdefault(key, set()).add(entry.column)

    def reshape(self, reshape: Reshape, paired: set[int]) -> None:
        """Record a whole-column change on every paired key, and follow a rename."""
        if reshape.renamed is not None:
            old, new = reshape.renamed
            for columns in self.changed.values():
                if old in columns:
                    columns.discard(old)
                    columns.add(new)
        if reshape.changes is not None:
            for key in paired:
                self.changed.setdefault(key, set()).add(reshape.changes)

    def freeze(self, compared: frozenset[str]) -> Ledger:
        """Return the finished ledger."""
        return Ledger(
            added=frozenset(self.added),
            removed=frozenset(self.removed),
            changed={key: frozenset(columns) for key, columns in self.changed.items()},
            compared=compared,
        )


def grade(ledger: Ledger, result: DiffResult, *, every_row: bool) -> list[str]:
    """Hold a result to the ledger, and return each way it disagrees.

    Args:
        ledger (Ledger): What the run must report.
        result (DiffResult): What it reported.
        every_row (bool): Whether the result carries every differing row, as
            a local run does. Pushdown returns counts and keys only, and
            cannot say which column differs on a row.

    Returns:
        list[str]: One line per disagreement, empty when the result is right.
    """
    summary = result.summary
    problems = [
        f"{name}: expected {expected}, got {actual}"
        for name, expected, actual in (
            ("added_count", len(ledger.added), summary.added_count),
            ("removed_count", len(ledger.removed), summary.removed_count),
            ("changed_count", len(ledger.changed), summary.changed_count),
            ("is_match", ledger.is_match, summary.is_match),
        )
        if expected != actual
    ]
    reported = {column: count for column, count in summary.column_mismatches.items() if count}
    if reported != ledger.column_mismatches:
        problems.append(f"column_mismatches: expected {ledger.column_mismatches}, got {reported}")
    if set(result.compared_columns) != ledger.compared:
        problems.append(
            f"compared_columns: expected {sorted(ledger.compared)}, "
            f"got {sorted(result.compared_columns)}"
        )
    if not every_row:
        return problems

    for name, expected_keys, frame in (
        ("added keys", ledger.added, result.added),
        ("removed keys", ledger.removed, result.removed),
        ("changed keys", frozenset(ledger.changed), result.changed),
    ):
        actual_keys = frozenset(frame[KEY].to_list())
        if actual_keys != expected_keys:
            problems.append(_key_problem(name, expected_keys, actual_keys))
    for column in sorted(ledger.compared):
        expected_keys = ledger.keys_changed_in(column)
        actual_keys = frozenset(result.get_mismatches(column)[KEY].to_list())
        if actual_keys != expected_keys:
            problems.append(_key_problem(f"keys changed in {column}", expected_keys, actual_keys))
    return problems


def _key_problem(name: str, expected: frozenset[int], actual: frozenset[int]) -> str:
    """Describe a key set that differs from the ledger's."""
    return f"{name}: missing {sorted(expected - actual)}, unexpected {sorted(actual - expected)}"
