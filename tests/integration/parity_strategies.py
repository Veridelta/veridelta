# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

r"""Hypothesis strategies for comparisons both engines are expected to agree on.

Each case is a source frame, a target derived from it by dropping, adding,
mutating, and nulling rows, and a configuration whose rules are drawn from a
menu that fits each column's type. Inputs where the two engines are documented
to differ are left out on purpose, so a failure is a real divergence:

- text is ASCII: DuckDB folds case differently on other scripts, and its
  `levenshtein` counts bytes rather than characters;
- regular expressions avoid `\d`, `\w`, and look-around, whose meaning varies
  by engine;
- no column is cast between float and text, or cast leniently, since each
  engine spells those conversions its own way;
- no Categorical, Enum, Duration, Null, nanosecond, or non-UTC timezone dtypes,
  which the DuckDB harness cannot round-trip faithfully;
- no rule field the backend's pushdown refuses, such as `datetime_format` on
  Postgres, and no `pad_zeros` on a dtype the backend writes as different text
  on its two sides, such as a `UInt64` Postgres stores as `numeric`, both of
  which `comparison_cases` drops when told to.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import polars as pl
from hypothesis import strategies as st

from veridelta.models import DiffConfig, DiffRule

_TEXT_ALPHABET = "ABab01 \t_$.-"
"""ASCII letters in both cases, digits, a space and a tab, and the characters the regex menu
targets."""

_INT8_RANGE = (-128, 127)


@dataclass(frozen=True)
class _Kind:
    """One column type: its dtype, values, how a target drifts, and the rules that fit it.

    Attributes:
        dtype (pl.DataType): Polars dtype of the column on both sides.
        values (st.SearchStrategy[Any]): Non-null values.
        drift (Callable[[st.DrawFn, Any], Any]): A changed counterpart of a value.
        rules (st.SearchStrategy[dict[str, Any]]): `DiffRule` fields that suit the type.
        sibling (pl.DataType | None): Another dtype every value fits, which the
            target may store the column as when `strict_types` is on.
    """

    dtype: pl.DataType
    values: st.SearchStrategy[Any]
    drift: Callable[[st.DrawFn, Any], Any]
    rules: st.SearchStrategy[dict[str, Any]]
    sibling: pl.DataType | None = None


def _clamped(low: int, high: int) -> Callable[[st.DrawFn, Any], Any]:
    """Shift an integer by a small step, kept inside the type's range."""

    def drift(draw: st.DrawFn, value: Any) -> Any:
        step = draw(st.sampled_from([-10, -2, -1, 1, 2, 10]))
        return min(high, max(low, int(value) + step))

    return drift


def _float_drift(draw: st.DrawFn, value: Any) -> Any:
    """Shift a float across and along the tolerance boundaries, or make it non-finite."""
    choice = draw(st.sampled_from(["step", "nan", "inf"]))
    if choice == "nan":
        return float("nan")
    if choice == "inf":
        return float("inf")
    return float(value) + draw(st.sampled_from([-1.0, -0.5, 0.25, 0.5, 1.0, 2.0]))


def _decimal_drift(draw: st.DrawFn, value: Any) -> Any:
    """Shift a decimal by a cent or more."""
    return Decimal(value) + draw(
        st.sampled_from([Decimal("-1.00"), Decimal("0.01"), Decimal("0.50")])
    )


def _text_drift(draw: st.DrawFn, value: Any) -> Any:
    """Change text the ways the text rules forgive, and ways they do not."""
    text = str(value)
    edits: list[str] = [
        text.upper(),
        text.lower(),
        f" {text}",
        f"{text} ",
        f"\t{text}",
        f"{text}\r\n",
        f"{text}-",
        f"{text}x",
        text[1:],
        text.replace("a", "b"),
    ]
    return draw(st.sampled_from(edits))


def _date_drift(draw: st.DrawFn, value: Any) -> Any:
    """Move a date by a day or a year."""
    return value + timedelta(days=draw(st.sampled_from([-1, 1, 365])))


def _datetime_drift(draw: st.DrawFn, value: Any) -> Any:
    """Move a timestamp by a microsecond, a second, or a day."""
    return value + draw(
        st.sampled_from([timedelta(microseconds=1), timedelta(seconds=-1), timedelta(days=1)])
    )


def _numeric_rules(sentinel: object) -> st.SearchStrategy[dict[str, Any]]:
    """Rules for numbers: tolerances, a typed null sentinel, and null equality."""
    return st.one_of(
        st.just({}),
        st.fixed_dictionaries({"absolute_tolerance": st.sampled_from([0.5, 1.0, 2.0])}),
        st.fixed_dictionaries({"relative_tolerance": st.sampled_from([0.1, 0.5])}),
        st.just({"null_values": [sentinel]}),
        st.just({"treat_null_as_equal": True}),
    )


def _integer_rules(sentinel: int) -> st.SearchStrategy[dict[str, Any]]:
    """Integer rules: the numeric menu plus zero padding."""
    return st.one_of(
        _numeric_rules(sentinel),
        st.fixed_dictionaries({"pad_zeros": st.sampled_from([0, 3, 6])}),
    )


@st.composite
def _text_rules(draw: st.DrawFn) -> dict[str, Any]:
    """Text rules, combined: regex, whitespace, case, a value map, and a similarity limit."""
    fields: dict[str, Any] = {}
    if draw(st.booleans()):
        fields["regex_replace"] = draw(
            st.sampled_from(
                [
                    {"-": ""},
                    {"[^A-Za-z0-9]": ""},
                    {"\\$": ""},
                    {"a+": "a"},
                    {"(A)(b)": "$2$1"},
                    {"([01])": "${1}$$"},
                ]
            )
        )
    if draw(st.booleans()):
        fields["whitespace_mode"] = draw(st.sampled_from(["left", "right", "both"]))
    if draw(st.booleans()):
        fields["case_insensitive"] = True
    if draw(st.booleans()):
        fields["value_map"] = draw(
            st.dictionaries(
                st.text(_TEXT_ALPHABET, max_size=3), st.text(_TEXT_ALPHABET, max_size=3), max_size=2
            )
        )
    if draw(st.booleans()):
        fields["max_levenshtein_distance"] = draw(st.integers(1, 2))
    if draw(st.booleans()):
        fields["null_values"] = draw(st.sampled_from([["N/A"], [""]]))
    if draw(st.booleans()):
        fields["treat_null_as_equal"] = True
    return fields


_TEMPORAL_RULES: st.SearchStrategy[dict[str, Any]] = st.one_of(
    st.just({}), st.just({"treat_null_as_equal": True})
)

_STAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


@st.composite
def _stamp_text(draw: st.DrawFn) -> str:
    """Draw a timestamp written as text with a one- to six-digit fraction."""
    moment = draw(st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2030, 12, 31)))
    digits = draw(st.integers(1, 6))
    return f"{moment:%Y-%m-%d %H:%M:%S}.{moment.microsecond:06d}"[: 20 + digits]


def _stamp_drift(draw: st.DrawFn, value: Any) -> Any:
    """Change a written timestamp: lengthen its fraction, move it, or garble it.

    A fraction stays within six digits. A local run also reads seven to nine,
    where a warehouse reads NULL; that documented difference is pinned by
    `TestDatetimeFormatParity` instead.
    """
    text = str(value)
    longer = f"{text}0" if len(text) < 26 else text[:-1]
    return draw(st.sampled_from([longer, f"{text[:-1]}9", "not a time", draw(_stamp_text())]))


KINDS: dict[str, _Kind] = {
    "int64": _Kind(
        pl.Int64(),
        st.integers(-1000, 1000),
        _clamped(-(2**31), 2**31 - 1),
        _integer_rules(-1),
        sibling=pl.Int32(),
    ),
    "int8": _Kind(pl.Int8(), st.integers(*_INT8_RANGE), _clamped(*_INT8_RANGE), _integer_rules(0)),
    "uint32": _Kind(
        pl.UInt32(),
        st.integers(0, 1000),
        _clamped(0, 2**32 - 1),
        _integer_rules(0),
        sibling=pl.UInt64(),
    ),
    "float64": _Kind(
        pl.Float64(),
        st.one_of(
            st.floats(-1e6, 1e6, allow_nan=False, allow_infinity=False),
            st.sampled_from([float("nan"), float("inf"), float("-inf"), -0.0]),
        ),
        _float_drift,
        _numeric_rules(-1.0),
    ),
    "decimal": _Kind(
        pl.Decimal(10, 2),
        st.decimals(min_value=-1000, max_value=1000, places=2),
        _decimal_drift,
        st.one_of(
            st.just({}),
            st.fixed_dictionaries({"absolute_tolerance": st.sampled_from([0.5, 1.0])}),
            st.just({"treat_null_as_equal": True}),
        ),
        sibling=pl.Decimal(12, 4),
    ),
    "boolean": _Kind(
        pl.Boolean(),
        st.booleans(),
        lambda _draw, value: not value,
        _TEMPORAL_RULES,
    ),
    "text": _Kind(pl.String(), st.text(_TEXT_ALPHABET, max_size=6), _text_drift, _text_rules()),
    "date": _Kind(
        pl.Date(),
        st.dates(min_value=date(2000, 1, 1), max_value=date(2030, 12, 31)),
        _date_drift,
        _TEMPORAL_RULES,
    ),
    "stamp_text": _Kind(
        pl.String(),
        _stamp_text(),
        _stamp_drift,
        st.fixed_dictionaries(
            {"datetime_format": st.just(_STAMP_FORMAT), "treat_null_as_equal": st.booleans()}
        ),
    ),
    "datetime": _Kind(
        pl.Datetime("us"),
        st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2030, 12, 31)),
        _datetime_drift,
        _TEMPORAL_RULES,
    ),
}
"""Every column type the fuzzer generates, by name."""


@st.composite
def _cell(draw: st.DrawFn, kind: _Kind) -> Any:
    """Draw one value of a kind, NULL about one time in four."""
    if draw(st.integers(0, 3)) == 0:
        return None
    return draw(kind.values)


@st.composite
def _target_cell(draw: st.DrawFn, kind: _Kind, value: Any) -> Any:
    """Derive a target value: kept, drifted, nulled, or redrawn."""
    action = draw(st.sampled_from(["keep", "keep", "drift", "null", "redraw"]))
    if action == "keep":
        return value
    if action == "null":
        return None
    if action == "drift" and value is not None:
        return kind.drift(draw, value)
    return draw(_cell(kind))


@st.composite
def comparison_cases(
    draw: st.DrawFn, refused: frozenset[str] = frozenset()
) -> tuple[DiffConfig, pl.DataFrame, pl.DataFrame]:
    """Draw a configuration and a source and target both engines should agree on.

    Args:
        draw (st.DrawFn): Hypothesis' draw function.
        refused (frozenset[str]): Rule fields to leave out of every rule,
            because the pushdown side would refuse them.

    Returns:
        tuple[DiffConfig, pl.DataFrame, pl.DataFrame]: Keys on a unique `id`,
            one to three typed columns, and a rule for some of them.
    """
    names = draw(st.lists(st.sampled_from(sorted(KINDS)), min_size=1, max_size=3))
    columns = {f"c{index}_{name}": KINDS[name] for index, name in enumerate(names)}
    strict = draw(st.booleans())
    rows = draw(st.integers(0, 8))

    source: dict[str, list[Any]] = {"id": list(range(rows))}
    for column, kind in columns.items():
        source[column] = [draw(_cell(kind)) for _ in range(rows)]

    kept = [index for index in range(rows) if draw(st.integers(0, 4)) != 0]
    added = draw(st.integers(0, 2))
    target: dict[str, list[Any]] = {"id": [*kept, *range(rows, rows + added)]}
    for column, kind in columns.items():
        drifted = [draw(_target_cell(kind, source[column][index])) for index in kept]
        target[column] = [*drifted, *(draw(_cell(kind)) for _ in range(added))]

    schema = {"id": pl.Int64(), **{column: kind.dtype for column, kind in columns.items()}}
    # Under strict_types a sibling type must fail the column; otherwise it
    # would compare by value, a case the hand-written parity suite covers.
    target_schema = {
        "id": pl.Int64(),
        **{
            column: kind.sibling
            if strict and kind.sibling is not None and draw(st.booleans())
            else kind.dtype
            for column, kind in columns.items()
        },
    }
    rules: list[DiffRule] = []
    for column, kind in columns.items():
        if not draw(st.booleans()):
            continue
        fields = {field: value for field, value in draw(kind.rules).items() if field not in refused}
        rules.append(DiffRule(column_names=[column], **fields))
    config = DiffConfig(
        primary_keys=["id"],
        rules=rules,
        strict_types=strict,
        default_absolute_tolerance=draw(st.sampled_from([0.0, 0.0, 0.5])),
        default_treat_null_as_equal=draw(st.booleans()),
    )
    return (
        config,
        pl.DataFrame(source, schema=schema),
        pl.DataFrame(target, schema=target_schema),
    )
