# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Differential parity tests between the local engine and compiled pushdown SQL."""

from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import polars as pl
import pytest

from tests.integration.duckdb_harness import (
    assert_parity,
    run_local,
    run_pushdown,
    run_value_map_pushdown,
)
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError, DataIntegrityError
from veridelta.models import DiffConfig, DiffRule, ValueMapProposal

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_REFUSED_PARSE = pytest.mark.duckdb_only(
    reason="Postgres refuses datetime_format pushdown: it has no parse that yields NULL."
)
_REFUSED_EDIT_DISTANCE = pytest.mark.duckdb_only(
    reason="Postgres refuses max_levenshtein_distance pushdown: levenshtein needs fuzzystrmatch."
)


class TestBaselineParity:
    """Validate the harness itself on comparisons with no transform rules."""

    @pytest.mark.parametrize(
        ("src", "tgt", "config", "expected"),
        [
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3], "amount": [10.5, 20.0, 30.0]}),
                pl.DataFrame({"id": [1, 2, 3], "amount": [10.5, 20.0, 30.0]}),
                DiffConfig(primary_keys=["id"]),
                {"is_perfect_match": True},
                id="clean-comparison",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3], "val": ["A", "B", "C"]}),
                pl.DataFrame({"id": [1, 2, 4], "val": ["A", "CHANGED", "D"]}),
                DiffConfig(primary_keys=["id"]),
                {
                    "added_count": 1,
                    "removed_count": 1,
                    "changed_count": 1,
                    "column_mismatches": {"val": 1},
                    "is_match": False,
                },
                id="added-removed-and-changed-rows",
            ),
            # A bare `=` against NULL yields NULL, so pushdown needs `COALESCE(..., FALSE)`.
            pytest.param(
                pl.DataFrame({"id": [1, 2], "val": ["A", "B"]}),
                pl.DataFrame({"id": [1, 2], "val": ["A", None]}),
                DiffConfig(primary_keys=["id"]),
                {"changed_count": 1},
                id="null-meets-a-value",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "val": [None, "B"]}),
                pl.DataFrame({"id": [1, 2], "val": [None, "B"]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["val"], treat_null_as_equal=True)],
                ),
                {"is_perfect_match": True},
                id="nulls-treated-as-equal",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "val": [None, "B"]}),
                pl.DataFrame({"id": [1, 2], "val": ["A", "B"]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["val"], treat_null_as_equal=True)],
                ),
                {"changed_count": 1},
                id="unmatched-null-is-a-mismatch",
            ),
        ],
    )
    def test_it_agrees_without_transform_rules(
        self, src: pl.DataFrame, tgt: pl.DataFrame, config: DiffConfig, expected: dict[str, object]
    ) -> None:
        """Ensure comparisons without transforms report the same tallies on both paths."""
        summary = assert_parity(config, src, tgt)

        assert {key: getattr(summary, key) for key in expected} == expected


class TestShippedStageParity:
    """Validate the transform stages that shipped before complete parity."""

    @pytest.mark.parametrize(
        ("src", "tgt", "config", "expected"),
        [
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3], "cost": [10.00, 20.00, 30.00]}),
                pl.DataFrame({"id": [1, 2, 3], "cost": [10.02, 20.04, 30.20]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["cost"], absolute_tolerance=0.05)],
                ),
                {"changed_count": 1},
                id="numeric-tolerance",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "status": ["Active", "N/A"]}),
                pl.DataFrame({"id": [1, 2], "status": ["Active", None]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(
                            column_names=["status"], null_values=["N/A"], treat_null_as_equal=True
                        )
                    ],
                ),
                {"is_perfect_match": True},
                id="typed-null-sentinel",
            ),
            # Pushdown skips the stage as local does, rather than emit `amount IN ('N/A')`.
            pytest.param(
                pl.DataFrame({"id": [1, 2], "amount": [10, 20]}),
                pl.DataFrame({"id": [1, 2], "amount": [10, 99]}),
                DiffConfig(primary_keys=["id"], default_null_values=["N/A"]),
                {"changed_count": 1},
                id="sentinel-cannot-apply-to-the-dtype",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "phone": ["1-800-555", "1-800-556"]}),
                pl.DataFrame({"id": [1, 2], "phone": ["1800555", "1800555"]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["phone"], regex_replace={"-": ""})],
                ),
                {"changed_count": 1},
                id="regex-replaces-every-match",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "cost": ["$10.00", "$20.50"]}),
                pl.DataFrame({"id": [1, 2], "cost": ["10.00", "20.50"]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["cost"], regex_replace={"\\$": ""})],
                ),
                {"is_perfect_match": True},
                id="regex-replace",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3], "name": ["  Ada ", "grace", "LINUS "]}),
                pl.DataFrame({"id": [1, 2, 3], "name": ["ADA", "Grace  ", " torvalds"]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(
                            column_names=["name"], whitespace_mode="both", case_insensitive=True
                        )
                    ],
                ),
                {"changed_count": 1},
                id="whitespace-and-case",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3], "tier": ["Enterprise", "Premium", "Standard"]}),
                pl.DataFrame({"id": [1, 2, 3], "tier": ["ENT", "PRM", "BASIC"]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(
                            column_names=["tier"],
                            value_map={"Enterprise": "ENT", "Premium": "PRM", "Standard": "STD"},
                        )
                    ],
                ),
                {"changed_count": 1},
                id="source-side-value-map",
            ),
            # Polars skips the regex on the float side, so pushdown skips `REGEXP_REPLACE` too.
            pytest.param(
                pl.DataFrame({"id": [1, 2], "cost": ["$10.00", "$99.99"]}),
                pl.DataFrame({"id": [1, 2], "cost": [10.0, 99.98]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(
                            column_names=["cost"],
                            regex_replace={"\\$": ""},
                            whitespace_mode="both",
                            case_insensitive=True,
                            cast_to="Float64",
                        )
                    ],
                ),
                {"changed_count": 1},
                id="text-stage-meets-a-numeric-side",
            ),
        ],
    )
    def test_it_agrees_on_each_shipped_stage(
        self, src: pl.DataFrame, tgt: pl.DataFrame, config: DiffConfig, expected: dict[str, object]
    ) -> None:
        """Ensure each shipped stage reaches the same verdict on both paths."""
        summary = assert_parity(config, src, tgt)

        assert {key: getattr(summary, key) for key in expected} == expected

    def test_it_agrees_on_ignored_columns(self) -> None:
        """Ensure a dropped column contributes to neither tally."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"], "audit": ["x", "y"]})
        tgt = pl.DataFrame({"id": [1, 2], "val": ["A", "B"], "audit": ["p", "q"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["audit"], ignore=True)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True
        assert "audit" not in summary.column_mismatches


class TestPaddingParity:
    """Validate stage 5 against Python's `str.zfill`, sign and overflow included."""

    @pytest.mark.parametrize(
        ("src", "tgt", "rule", "expected"),
        [
            pytest.param(
                pl.DataFrame({"id": [1, 2], "code": ["7", "42"]}),
                pl.DataFrame({"id": [1, 2], "code": ["00007", "00042"]}),
                DiffRule(column_names=["code"], pad_zeros=5),
                {"is_perfect_match": True},
                id="unsigned",
            ),
            # A bare `LPAD('-12', 4, '0')` yields `0-12`, while Polars yields `-012`.
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3], "code": ["-12", "-7", "-1234"]}),
                pl.DataFrame({"id": [1, 2, 3], "code": ["-012", "-007", "-1234"]}),
                DiffRule(column_names=["code"], pad_zeros=4),
                {"is_perfect_match": True},
                id="negative-numbers",
            ),
            pytest.param(
                pl.DataFrame({"id": [1], "code": ["+7"]}),
                pl.DataFrame({"id": [1], "code": ["+007"]}),
                DiffRule(column_names=["code"], pad_zeros=4),
                {"is_perfect_match": True},
                id="explicitly-positive",
            ),
            # A bare `LPAD` truncates input longer than the width, so distinct long codes collide.
            pytest.param(
                pl.DataFrame({"id": [1, 2], "code": ["1234567", "1234599"]}),
                pl.DataFrame({"id": [1, 2], "code": ["1234567", "1234500"]}),
                DiffRule(column_names=["code"], pad_zeros=3),
                {"changed_count": 1},
                id="never-truncates",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "code": [7, -42]}),
                pl.DataFrame({"id": [1, 2], "code": ["00007", "-0042"]}),
                DiffRule(column_names=["code"], pad_zeros=5),
                {"is_perfect_match": True},
                id="padded-integers",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "code": [None, "7"]}),
                pl.DataFrame({"id": [1, 2], "code": [None, "007"]}),
                DiffRule(column_names=["code"], pad_zeros=3, treat_null_as_equal=True),
                {"is_perfect_match": True},
                id="propagates-nulls",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "code": [7, 42]}),
                pl.DataFrame({"id": [1, 2], "code": ["7", "42"]}),
                DiffRule(column_names=["code"], pad_zeros=0),
                {"is_perfect_match": True},
                id="zero-width",
            ),
        ],
    )
    def test_it_agrees_on_zero_padding(
        self, src: pl.DataFrame, tgt: pl.DataFrame, rule: DiffRule, expected: dict[str, object]
    ) -> None:
        """Ensure zero padding lands on the same text on both paths."""
        summary = assert_parity(DiffConfig(primary_keys=["id"], rules=[rule]), src, tgt)

        assert {key: getattr(summary, key) for key in expected} == expected


class TestCastParity:
    """Validate stage 7 against Polars cast semantics."""

    @pytest.mark.parametrize(
        ("src", "tgt", "rule", "expected"),
        [
            pytest.param(
                pl.DataFrame({"id": [1, 2], "cost": ["$10.00", "$99.99"]}),
                pl.DataFrame({"id": [1, 2], "cost": [10.0, 99.98]}),
                DiffRule(column_names=["cost"], regex_replace={"\\$": ""}, cast_to="Float64"),
                {"changed_count": 1},
                id="string-to-float",
            ),
            # Polars truncates toward zero, while Snowflake and DuckDB round.
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3, 4], "n": [10.7, -10.7, 10.5, 11.5]}),
                pl.DataFrame({"id": [1, 2, 3, 4], "n": [10, -10, 10, 11]}),
                DiffRule(column_names=["n"], cast_to="Int64"),
                {"is_perfect_match": True},
                id="float-to-integer-truncates",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "code": [7, -42]}),
                pl.DataFrame({"id": [1, 2], "code": ["7", "-42"]}),
                DiffRule(column_names=["code"], cast_to="String"),
                {"is_perfect_match": True},
                id="integer-to-string",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "day": ["2026-01-02", "2026-03-04"]}),
                pl.DataFrame({"id": [1, 2], "day": [date(2026, 1, 2), date(2026, 3, 5)]}),
                DiffRule(column_names=["day"], cast_to="Date"),
                {"changed_count": 1},
                id="string-to-date",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "code": [7, 42]}),
                pl.DataFrame({"id": [1, 2], "code": ["00007", "00042"]}),
                DiffRule(column_names=["code"], pad_zeros=5, cast_to="String"),
                {"is_perfect_match": True},
                id="padding-feeds-a-cast",
            ),
        ],
    )
    def test_it_agrees_on_casts(
        self, src: pl.DataFrame, tgt: pl.DataFrame, rule: DiffRule, expected: dict[str, object]
    ) -> None:
        """Ensure a cast yields the same values on both paths."""
        summary = assert_parity(DiffConfig(primary_keys=["id"], rules=[rule]), src, tgt)

        assert {key: getattr(summary, key) for key in expected} == expected


@_REFUSED_PARSE
class TestDatetimeFormatParity:
    """Validate stage 6a.

    DuckDB reads Python directives natively, so these tests prove the parse is
    wired into the right stage with the right null behavior. They say nothing
    about the Snowflake and Databricks translation tables, which are pinned by
    string assertions in `tests/unit/test_sql_compiler.py` instead.
    """

    @pytest.mark.parametrize(
        ("src", "tgt", "rule", "expected"),
        [
            pytest.param(
                pl.DataFrame({"id": [1, 2], "ts": ["2026-01-02 15:30:45", "2026-03-04 01:02:03"]}),
                pl.DataFrame(
                    {
                        "id": [1, 2],
                        "ts": [datetime(2026, 1, 2, 15, 30, 45), datetime(2026, 3, 4, 1, 2, 4)],
                    }
                ),
                DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d %H:%M:%S"),
                {"changed_count": 1},
                id="parsed-timestamp",
            ),
            # Polars parses non-strictly, while a strict SQL parse fails the whole query.
            pytest.param(
                pl.DataFrame({"id": [1, 2], "ts": ["2026-01-02", "not a date"]}),
                pl.DataFrame({"id": [1, 2], "ts": ["2026-01-02", "also not a date"]}),
                DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d", treat_null_as_equal=True),
                {"is_perfect_match": True},
                id="unparseable-text-becomes-null",
            ),
            pytest.param(
                pl.DataFrame({"id": [1], "ts": [20260102]}),
                pl.DataFrame({"id": [1], "ts": ["20260102"]}),
                DiffRule(column_names=["ts"], pad_zeros=8, datetime_format="%Y%m%d"),
                {"is_perfect_match": True},
                id="padding-precedes-parsing",
            ),
            pytest.param(
                pl.DataFrame(
                    {
                        "id": [1, 2, 3, 4],
                        "ts": [
                            "2026-01-02 01:02:03.5",
                            "2026-01-02 01:02:03.123",
                            "2026-01-02 01:02:03.123456",
                            "2026-01-02 01:02:03.25",
                        ],
                    }
                ),
                pl.DataFrame(
                    {
                        "id": [1, 2, 3, 4],
                        "ts": [
                            datetime(2026, 1, 2, 1, 2, 3, 500000),
                            datetime(2026, 1, 2, 1, 2, 3, 123000),
                            datetime(2026, 1, 2, 1, 2, 3, 123456),
                            datetime(2026, 1, 2, 1, 2, 3, 520000),
                        ],
                    }
                ),
                DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d %H:%M:%S.%f"),
                {"changed_count": 1},
                id="fractional-seconds",
            ),
            pytest.param(
                pl.DataFrame({"id": [1], "ts": ["2026-01-02 15:30:45+0200"]}),
                pl.DataFrame({"id": [1], "ts": ["2026-01-02 13:30:45+0000"]}),
                DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d %H:%M:%S%z"),
                {"is_perfect_match": True},
                id="parsed-offset",
            ),
        ],
    )
    def test_it_agrees_on_parsed_datetimes(
        self, src: pl.DataFrame, tgt: pl.DataFrame, rule: DiffRule, expected: dict[str, object]
    ) -> None:
        """Ensure text parsed by `datetime_format` compares the same on both paths."""
        summary = assert_parity(DiffConfig(primary_keys=["id"], rules=[rule]), src, tgt)

        assert {key: getattr(summary, key) for key in expected} == expected

    def test_it_differs_on_fractions_longer_than_six_digits(self) -> None:
        """Pin the documented difference: only a local run reads seven to nine digits.

        Polars' `%.f` keeps the microseconds of a longer fraction, while DuckDB's
        `%f`, like Python's, reads at most six digits and yields NULL.
        """
        src = pl.DataFrame({"id": [1], "ts": ["2026-01-02 01:02:03.1234567"]})
        tgt = pl.DataFrame({"id": [1], "ts": [datetime(2026, 1, 2, 1, 2, 3, 123456)]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d %H:%M:%S.%f")],
        )

        local = run_local(config, src, tgt).summary
        pushdown, _ = run_pushdown(config, src, tgt)

        assert local.changed_count == 0
        assert pushdown.summary.changed_count == 1


class TestStrictTypesParity:
    """Validate that `strict_types` fails a type mismatch on both paths."""

    @pytest.mark.parametrize(
        ("source", "target", "treat_null", "expected_changed"),
        [
            pytest.param(
                pl.Series("val", [10.0, None]),
                pl.Series("val", [10, None]),
                False,
                2,
                id="float-vs-int",
            ),
            pytest.param(
                pl.Series("val", [10, 11]),
                pl.Series("val", ["10", "11"]),
                False,
                2,
                id="int-vs-text",
            ),
            pytest.param(
                pl.Series("val", [Decimal("1.50"), Decimal("2.00")], dtype=pl.Decimal(10, 2)),
                pl.Series("val", [Decimal("1.5000"), Decimal("2.0000")], dtype=pl.Decimal(12, 4)),
                False,
                2,
                id="decimal-scales",
            ),
            pytest.param(
                pl.Series("val", ["abc", "10"]),
                pl.Series("val", [10, 10]),
                False,
                2,
                id="unparseable-text-vs-int",
            ),
            pytest.param(
                pl.Series("val", [None, 1.0]),
                pl.Series("val", [None, 1]),
                True,
                1,
                id="nulls-still-meet",
            ),
        ],
    )
    def test_it_agrees_that_differing_types_never_match(
        self, source: pl.Series, target: pl.Series, treat_null: bool, expected_changed: int
    ) -> None:
        """Ensure a warehouse no longer compares across types it was told to keep apart.

        Before, the warehouse compared `10.0` with `10` by value, and DuckDB
        failed the whole statement casting `'abc'` to an integer.
        """
        src = pl.DataFrame({"id": [1, 2]}).with_columns(source)
        tgt = pl.DataFrame({"id": [1, 2]}).with_columns(target)
        config = DiffConfig(
            primary_keys=["id"],
            strict_types=True,
            rules=[DiffRule(column_names=["val"], treat_null_as_equal=treat_null)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == expected_changed


class TestTimezoneParity:
    """Validate stage 6b, where the local conversion is metadata-only."""

    @pytest.mark.parametrize(
        ("src", "tgt", "rule", "expected"),
        [
            pytest.param(
                pl.DataFrame(
                    {
                        "id": [1, 2],
                        "ts": [
                            datetime(2026, 1, 2, 2, 30, tzinfo=UTC),
                            datetime(2026, 7, 2, 15, 30, tzinfo=UTC),
                        ],
                    }
                ),
                pl.DataFrame(
                    {
                        "id": [1, 2],
                        "ts": [
                            datetime(2026, 1, 2, 2, 30, tzinfo=UTC),
                            datetime(2026, 7, 2, 16, 30, tzinfo=UTC),
                        ],
                    }
                ),
                DiffRule(column_names=["ts"], timezone="America/New_York"),
                {"changed_count": 1},
                id="converted-timestamp",
            ),
            # Polars casts the UTC instant to 2026-01-02, not to the New York day of 2026-01-01.
            pytest.param(
                pl.DataFrame({"id": [1], "ts": [datetime(2026, 1, 2, 2, 30, tzinfo=UTC)]}),
                pl.DataFrame({"id": [1], "ts": [datetime(2026, 1, 2, 2, 30, tzinfo=UTC)]}),
                DiffRule(column_names=["ts"], timezone="America/New_York", cast_to="Date"),
                {"is_perfect_match": True},
                id="converted-timestamp-cast-to-a-date",
            ),
            # A `%z` parse makes the text timezone-aware before the zone rule converts it.
            pytest.param(
                pl.DataFrame(
                    {"id": [1, 2], "ts": ["2026-01-02T02:30:00+0000", "2026-07-02T15:30:00+0000"]}
                ),
                pl.DataFrame(
                    {"id": [1, 2], "ts": ["2026-01-01T21:30:00-0500", "2026-07-02T12:30:00-0400"]}
                ),
                DiffRule(
                    column_names=["ts"],
                    datetime_format="%Y-%m-%dT%H:%M:%S%z",
                    timezone="America/New_York",
                ),
                {"changed_count": 1},
                id="text-parsed-with-an-offset",
                marks=_REFUSED_PARSE,
            ),
        ],
    )
    def test_it_agrees_under_a_zone_rule(
        self, src: pl.DataFrame, tgt: pl.DataFrame, rule: DiffRule, expected: dict[str, object]
    ) -> None:
        """Ensure a zone rule leaves both paths comparing the same instants."""
        summary = assert_parity(DiffConfig(primary_keys=["id"], rules=[rule]), src, tgt)

        assert {key: getattr(summary, key) for key in expected} == expected

    @pytest.mark.parametrize(
        ("frame", "rule", "match"),
        [
            # Pushdown refuses to guess an origin zone, as local does.
            pytest.param(
                pl.DataFrame({"id": [1], "ts": [datetime(2026, 1, 2, 2, 30)]}),
                DiffRule(column_names=["ts"], timezone="America/New_York"),
                "timezone-naive",
                id="naive-timestamp",
            ),
            pytest.param(
                pl.DataFrame({"id": [1], "ts": ["2026-01-02"]}),
                DiffRule(column_names=["ts"], timezone="America/New_York"),
                "not a",
                id="non-temporal-column",
            ),
            # A parse without `%z` yields naive timestamps, which meet the same refusal.
            pytest.param(
                pl.DataFrame({"id": [1], "ts": ["2026-01-02 02:30:00"]}),
                DiffRule(
                    column_names=["ts"],
                    datetime_format="%Y-%m-%d %H:%M:%S",
                    timezone="America/New_York",
                ),
                "timezone-naive",
                id="text-parsed-without-an-offset",
                marks=_REFUSED_PARSE,
            ),
        ],
    )
    def test_it_rejects_a_zone_rule_on_both_paths(
        self, frame: pl.DataFrame, rule: DiffRule, match: str
    ) -> None:
        """Ensure a zone rule on anything but an aware timestamp fails before any comparison."""
        config = DiffConfig(primary_keys=["id"], rules=[rule])

        with pytest.raises(ConfigError, match=match):
            run_local(config, frame, frame)
        with pytest.raises(ConfigError, match=match):
            run_pushdown(config, frame, frame)


class TestEdgeCaseParity:
    """Validate join shapes, literal escaping, and rule precedence at the edges.

    Every stage above is proven in isolation. These cases cover the seams
    between stages and the join: composite keys, renamed columns, empty inner
    joins, quote characters inside data literals, and what happens when two
    rules claim the same column.
    """

    @pytest.mark.parametrize(
        ("src", "tgt", "config", "expected"),
        [
            # 0.5 and 5.0 sit inside 1% of their source; 2.0 does not.
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3], "cost": [100.0, 100.0, 1000.0]}),
                pl.DataFrame({"id": [1, 2, 3], "cost": [100.5, 102.0, 1005.0]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["cost"], relative_tolerance=0.01)],
                ),
                {"changed_count": 1},
                id="relative-tolerance-alone",
            ),
            # A 1.0 drift fails 0.6 absolute or 0.5% relative alone, but passes their sum.
            pytest.param(
                pl.DataFrame({"id": [1, 2], "cost": [100.0, 100.0]}),
                pl.DataFrame({"id": [1, 2], "cost": [101.0, 101.2]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(
                            column_names=["cost"], absolute_tolerance=0.6, relative_tolerance=0.005
                        )
                    ],
                ),
                {"changed_count": 1},
                id="absolute-and-relative-tolerances-add",
            ),
            # Row 2 is a null pair, row 3 exceeds the tolerance, row 4 is one-sided.
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3, 4], "cost": [10.0, None, 30.0, None]}),
                pl.DataFrame({"id": [1, 2, 3, 4], "cost": [10.04, None, 30.5, 5.0]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(
                            column_names=["cost"], absolute_tolerance=0.05, treat_null_as_equal=True
                        )
                    ],
                ),
                {"changed_count": 2},
                id="tolerance-meets-null-safe-equality",
            ),
            pytest.param(
                pl.DataFrame({"tenant": [1, 1, 2], "id": [1, 2, 1], "val": ["A", "B", "C"]}),
                pl.DataFrame({"tenant": [1, 1, 2], "id": [1, 2, 2], "val": ["A", "X", "D"]}),
                DiffConfig(primary_keys=["tenant", "id"]),
                {
                    "added_count": 1,
                    "removed_count": 1,
                    "changed_count": 1,
                    "column_mismatches": {"val": 1},
                },
                id="composite-primary-keys",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3], "legacy_amt": [10.0, 20.0, 30.0]}),
                pl.DataFrame({"id": [1, 2, 3], "amount": [10.0, 25.0, 30.0]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["legacy_amt"], rename_to="amount")],
                ),
                {"changed_count": 1, "column_mismatches": {"amount": 1}},
                id="renamed-column",
            ),
            # `_literal` doubles each quote, so a literal never ends the SQL string early.
            pytest.param(
                pl.DataFrame(
                    {
                        "id": [1, 2, 3],
                        "owner": ["O'Brien", "Smith", "D'Angelo"],
                        "phrase": ["it's", "that's", "ok"],
                    }
                ),
                pl.DataFrame(
                    {
                        "id": [1, 2, 3],
                        "owner": [None, "Smith", "DAngelo"],
                        "phrase": ["its", "thats", "ok"],
                    }
                ),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(
                            column_names=["owner"],
                            null_values=["O'Brien"],
                            regex_replace={"'": ""},
                            treat_null_as_equal=True,
                        ),
                        DiffRule(
                            column_names=["phrase"], value_map={"it's": "its", "that's": "thats"}
                        ),
                    ],
                ),
                {"is_perfect_match": True},
                id="apostrophes-inside-literals",
            ),
            # `SUM` over zero rows is NULL in SQL, which the reducer reads as nothing to report.
            pytest.param(
                pl.DataFrame({"id": [1, 2], "val": ["A", "B"]}),
                pl.DataFrame({"id": [3, 4], "val": ["C", "D"]}),
                DiffConfig(primary_keys=["id"]),
                {"added_count": 2, "removed_count": 2, "changed_count": 0, "column_mismatches": {}},
                id="no-primary-key-overlaps",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "audit": ["x", "y"]}),
                pl.DataFrame({"id": [1, 3], "audit": ["p", "q"]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["audit"], ignore=True)],
                ),
                {"added_count": 1, "removed_count": 1, "changed_count": 0, "column_mismatches": {}},
                id="every-non-key-column-ignored",
            ),
            # Integers are the portable input, since Polars refuses to cast text such as `'true'`.
            pytest.param(
                pl.DataFrame({"id": [1, 2, 3], "flag": [1, 0, 1]}),
                pl.DataFrame({"id": [1, 2, 3], "flag": [True, False, False]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["flag"], cast_to="Boolean")],
                ),
                {"changed_count": 1},
                id="boolean-cast",
            ),
            # The tolerance survives the rename, though the rule lists only the source spelling.
            pytest.param(
                pl.DataFrame({"id": [1, 2], "legacy_amt": [10.0, 20.0]}),
                pl.DataFrame({"id": [1, 2], "amount": [10.0, 20.04]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(
                            column_names=["legacy_amt"], rename_to="amount", absolute_tolerance=0.05
                        )
                    ],
                ),
                {"is_perfect_match": True},
                id="renamed-column-carries-a-tolerance",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "legacy_name": ["ada", "grace"]}),
                pl.DataFrame({"id": [1, 2], "name": [" ada ", "Grace"]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(
                            column_names=["legacy_name"],
                            rename_to="name",
                            whitespace_mode="both",
                            case_insensitive=True,
                        )
                    ],
                ),
                {"is_perfect_match": True},
                id="renamed-column-normalized-on-both-sides",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "legacy_amt": [10.0, 20.0]}),
                pl.DataFrame({"id": [1, 2], "amount": [10.0, 20.04]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(column_names=["legacy_amt"], rename_to="amount"),
                        DiffRule(column_names=["amount"], absolute_tolerance=0.05),
                    ],
                ),
                {"is_perfect_match": True},
                id="rule-names-the-target-spelling",
            ),
            # Exact names outrank patterns, so the ignore pattern cannot drop `_etl_batch_id`.
            pytest.param(
                pl.DataFrame({"id": [1, 2], "_etl_batch_id": [1, 2], "_etl_loaded_at": ["a", "b"]}),
                pl.DataFrame({"id": [1, 2], "_etl_batch_id": [1, 3], "_etl_loaded_at": ["x", "y"]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(pattern="^_etl_", ignore=True),
                        DiffRule(column_names=["_etl_batch_id"]),
                    ],
                ),
                {"changed_count": 1, "column_mismatches": {"_etl_batch_id": 1}},
                id="exact-rule-outranks-a-pattern-ignore",
            ),
            # Pushdown reads the key under its stored name and projects it under the join name.
            pytest.param(
                pl.DataFrame({"legacy_id": [1, 2, 3], "val": ["A", "B", "C"]}),
                pl.DataFrame({"user_id": [1, 2, 4], "val": ["A", "X", "D"]}),
                DiffConfig(
                    primary_keys=["user_id"],
                    rules=[DiffRule(column_names=["legacy_id"], rename_to="user_id")],
                ),
                {"changed_count": 1, "added_count": 1, "removed_count": 1},
                id="renamed-primary-key",
            ),
            pytest.param(
                pl.DataFrame({"id": [1, 2], "val": ["A", "B"]}),
                pl.DataFrame({"id": [1, 2], "val": ["A", "C"]}),
                DiffConfig(primary_keys=["id"], normalize_column_names=True),
                {"changed_count": 1},
                id="header-normalization-changes-nothing",
            ),
            # A null key never joins, yet its row counts toward the totals, as `COUNT(*)` does.
            pytest.param(
                pl.DataFrame({"id": [1, 2, None], "val": ["A", "B", "C"]}),
                pl.DataFrame({"id": [1, 2, 3], "val": ["A", "B", "C"]}),
                DiffConfig(primary_keys=["id"], threshold=0.7),
                {"total_rows_source": 3, "removed_count": 1, "added_count": 1, "is_match": True},
                id="row-totals-with-a-null-key",
            ),
            # `cost` takes the strict exact-name rule; `count` falls through to the pattern.
            pytest.param(
                pl.DataFrame({"id": [1, 2], "cost": [10.0, 20.0], "count": [1, 2]}),
                pl.DataFrame({"id": [1, 2], "cost": [10.04, 21.0], "count": [1, 5]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[
                        DiffRule(pattern="^co", absolute_tolerance=100.0),
                        DiffRule(column_names=["cost"], absolute_tolerance=0.05),
                        DiffRule(column_names=["cost"], absolute_tolerance=10.0),
                    ],
                ),
                {"changed_count": 1, "column_mismatches": {"cost": 1}},
                id="first-matching-rule-wins",
            ),
        ],
    )
    def test_it_agrees_on_edge_cases(
        self, src: pl.DataFrame, tgt: pl.DataFrame, config: DiffConfig, expected: dict[str, object]
    ) -> None:
        """Ensure both paths agree at the seams between stages and the join."""
        summary = assert_parity(config, src, tgt)

        assert {key: getattr(summary, key) for key in expected} == expected

    @pytest.mark.parametrize(
        ("mode", "expected_changed"),
        [
            pytest.param("left", 1, id="left-keeps-trailing"),
            pytest.param("right", 1, id="right-keeps-leading"),
            pytest.param("both", 0, id="both-strips-all"),
        ],
    )
    def test_it_agrees_on_one_sided_whitespace_stripping(
        self, mode: str, expected_changed: int
    ) -> None:
        """Ensure `LTRIM` and `RTRIM` are wired to the matching Polars strip."""
        src = pl.DataFrame({"id": [1, 2], "name": ["  ada", "grace  "]})
        tgt = pl.DataFrame({"id": [1, 2], "name": ["ada", "grace"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], whitespace_mode=mode)],  # type: ignore[arg-type]
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == expected_changed

    @pytest.mark.parametrize(
        ("pattern", "replacement", "source", "target", "expected_changed"),
        [
            pytest.param(r"(\d{3})-(\d{4})", "$1$2", "555-1234", "5551234", 0, id="numbered"),
            pytest.param(
                r"([a-z]+)@([a-z]+)", "$2.$1", "ada@lovelace", "lovelace.ada", 0, id="swap"
            ),
            pytest.param("-([0-9])", "${1}0", "x-1", "x10", 0, id="braced-before-a-digit"),
            pytest.param("[0-9]+", "<$0>", "a12b", "a34b", 1, id="whole-match"),
            pytest.param("-", "$$", "1-2", "1$2", 0, id="escaped-dollar"),
            pytest.param("x", "\\", "axb", "a\\b", 0, id="backslash"),
        ],
    )
    def test_it_agrees_on_group_references_in_replacements(
        self, pattern: str, replacement: str, source: str, target: str, expected_changed: int
    ) -> None:
        """Ensure a replacement written for Polars means the same in a warehouse.

        The rule rewrites both sides, so each case only comes out as expected
        when the reference is read as Polars reads it. Pushdown used to pass the
        replacement through as written, so DuckDB, Snowflake, and BigQuery read
        `$1` and `$0` as text, and a lone backslash as the start of a reference.
        """
        src = pl.DataFrame({"id": [1], "value": [source]})
        tgt = pl.DataFrame({"id": [1], "value": [target]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["value"], regex_replace={pattern: replacement})],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == expected_changed

    @pytest.mark.parametrize(
        ("mode", "expected_changed"),
        [
            pytest.param("left", 3, id="left"),
            pytest.param("right", 3, id="right"),
            pytest.param("both", 0, id="both"),
        ],
    )
    def test_it_strips_tabs_line_breaks_and_unicode_spaces_as_polars_does(
        self, mode: str, expected_changed: int
    ) -> None:
        """Ensure a warehouse trims every character Polars' `strip_chars` removes.

        A bare SQL `TRIM` removes only spaces on Snowflake, Databricks, and
        DuckDB, so a tab, a line break, or a no-break space used to survive
        pushdown and count as drift that a local run stripped away.
        """
        src = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["\tada", "grace\r\n", "\u00a0linus\u3000", "\u2028guido\x85"],
            }
        )
        tgt = pl.DataFrame({"id": [1, 2, 3, 4], "name": ["ada", "grace", "linus", "guido"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], whitespace_mode=mode)],  # type: ignore[arg-type]
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == expected_changed

    def test_it_refuses_header_normalization_that_would_rename_a_warehouse_column(self) -> None:
        """Ensure pushdown fails loudly where normalizing would change a stored name.

        The compiler quotes identifiers exactly as they are stored, so it
        cannot refer to a column by the lowercase name a local run gives it.
        """
        frame = pl.DataFrame({"ID": [1], "val": ["A"]})
        config = DiffConfig(primary_keys=["ID"], normalize_column_names=True)

        assert run_local(config, frame, frame).summary.is_perfect_match is True
        with pytest.raises(ConfigError, match="normalize_column_names"):
            run_pushdown(config, frame, frame)


class TestKeyNormalizationParity:
    """Validate that primary keys pass through stages 1-7 on both paths.

    The local engine normalizes keys before it joins. Pushdown used to join
    keys as stored, so these cases reported added and removed rows in the
    warehouse for keys a local run matched.
    """

    @pytest.mark.parametrize(
        ("src", "tgt", "config", "expected"),
        [
            pytest.param(
                pl.DataFrame({"id": ["  A", "B "], "val": [1, 2]}),
                pl.DataFrame({"id": ["A", "B"], "val": [1, 3]}),
                DiffConfig(primary_keys=["id"], default_whitespace_mode="both"),
                {"added_count": 0, "removed_count": 0, "changed_count": 1},
                id="global-whitespace-mode",
            ),
            pytest.param(
                pl.DataFrame({"id": ["\tA", "B\u00a0", "\x0bC\r\n"], "val": [1, 2, 3]}),
                pl.DataFrame({"id": ["A", "B", "C"], "val": [1, 2, 4]}),
                DiffConfig(primary_keys=["id"], default_whitespace_mode="both"),
                {"added_count": 0, "removed_count": 0, "changed_count": 1},
                id="tabs-and-unicode-spaces",
            ),
            pytest.param(
                pl.DataFrame({"email": ["Ada@Example.com", "grace@example.com"], "val": [1, 2]}),
                pl.DataFrame({"email": ["ada@example.com", "GRACE@EXAMPLE.COM"], "val": [1, 2]}),
                DiffConfig(
                    primary_keys=["email"],
                    rules=[DiffRule(column_names=["email"], case_insensitive=True)],
                ),
                {"is_perfect_match": True},
                id="case-insensitive-key",
            ),
            pytest.param(
                pl.DataFrame({"acct": ["7", "42"], "val": ["A", "B"]}),
                pl.DataFrame({"acct": ["00007", "00042"], "val": ["A", "B"]}),
                DiffConfig(
                    primary_keys=["acct"],
                    rules=[DiffRule(column_names=["acct"], pad_zeros=5)],
                ),
                {"is_perfect_match": True},
                id="zero-padded-text-key",
            ),
            # DuckDB coerces the raw join anyway, so this guards the padded cross-type path.
            pytest.param(
                pl.DataFrame({"acct": [7, 42], "val": ["A", "B"]}),
                pl.DataFrame({"acct": ["00007", "00042"], "val": ["A", "B"]}),
                DiffConfig(
                    primary_keys=["acct"],
                    rules=[DiffRule(column_names=["acct"], pad_zeros=5)],
                ),
                {"is_perfect_match": True},
                id="padded-key-of-different-types",
            ),
            # DuckDB coerces the raw join too, so this guards the cast path.
            pytest.param(
                pl.DataFrame({"id": ["1", "2"], "val": ["A", "B"]}),
                pl.DataFrame({"id": [1, 2], "val": ["A", "C"]}),
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["id"], cast_to="Int64")],
                ),
                {"changed_count": 1, "added_count": 0},
                id="cast-key-of-different-types",
            ),
            pytest.param(
                pl.DataFrame({"code": ["M", "F"], "val": [1, 2]}),
                pl.DataFrame({"code": ["Male", "Female"], "val": [1, 2]}),
                DiffConfig(
                    primary_keys=["code"],
                    rules=[DiffRule(column_names=["code"], value_map={"M": "Male", "F": "Female"})],
                ),
                {"is_perfect_match": True},
                id="crosswalked-key",
            ),
            pytest.param(
                pl.DataFrame({"legacy_id": ["ab-1", "CD-2"], "val": [1, 2]}),
                pl.DataFrame({"user_id": ["AB-1", "cd-2"], "val": [1, 2]}),
                DiffConfig(
                    primary_keys=["user_id"],
                    rules=[
                        DiffRule(
                            column_names=["legacy_id"], rename_to="user_id", case_insensitive=True
                        )
                    ],
                ),
                {"is_perfect_match": True},
                id="renamed-key-its-rule-normalizes",
            ),
            # A NULL key never joins on either path, yet its row counts toward the totals.
            pytest.param(
                pl.DataFrame({"id": ["N/A", "B"], "val": [1, 2]}),
                pl.DataFrame({"id": ["N/A", "B"], "val": [1, 2]}),
                DiffConfig(primary_keys=["id"], default_null_values=["N/A"]),
                {"removed_count": 1, "added_count": 1, "total_rows_source": 2},
                id="sentinel-key-never-joins",
            ),
            pytest.param(
                pl.DataFrame({"tenant": [1, 1, 2], "code": ["a", "B", "c"], "val": [1, 2, 3]}),
                pl.DataFrame({"tenant": [1, 1, 3], "code": ["A", "b", "c"], "val": [1, 9, 3]}),
                DiffConfig(
                    primary_keys=["tenant", "code"],
                    rules=[DiffRule(column_names=["code"], case_insensitive=True)],
                ),
                {"changed_count": 1, "added_count": 1, "removed_count": 1},
                id="composite-key-with-one-normalized-part",
            ),
        ],
    )
    def test_it_agrees_on_normalized_keys(
        self, src: pl.DataFrame, tgt: pl.DataFrame, config: DiffConfig, expected: dict[str, object]
    ) -> None:
        """Ensure keys normalized before the join pair the same rows on both paths."""
        summary = assert_parity(config, src, tgt)

        assert {key: getattr(summary, key) for key in expected} == expected


def _rejection_on_both_paths(config: DiffConfig, src: pl.DataFrame, tgt: pl.DataFrame) -> str:
    """Assert both engines refuse duplicate keys with the same message, and return it."""
    with pytest.raises(DataIntegrityError) as local:
        run_local(config, src, tgt)
    with pytest.raises(DataIntegrityError) as pushdown:
        run_pushdown(config, src, tgt)

    assert str(pushdown.value) == str(local.value)
    return str(local.value)


class TestDuplicateKeyParity:
    """Validate that duplicate primary keys fail both paths identically.

    A local run refuses to join keys that repeat within a dataset, since the
    join would fan out. Pushdown used to skip the check, so a table that
    repeated a key reported whatever the fanned-out joins happened to produce,
    including a perfect match.
    """

    def test_it_rejects_duplicate_source_keys_on_both_paths(self) -> None:
        """Ensure identical duplicated rows fail instead of matching each other."""
        src = pl.DataFrame({"id": [1, 1, 2], "val": ["A", "A", "B"]})
        tgt = pl.DataFrame({"id": [1, 1, 2], "val": ["A", "A", "B"]})

        message = _rejection_on_both_paths(DiffConfig(primary_keys=["id"]), src, tgt)

        assert "not unique in SOURCE dataset. Found 2 duplicate rows" in message

    def test_it_rejects_duplicate_target_keys_on_both_paths(self) -> None:
        """Ensure the count covers every copy of every repeated key."""
        src = pl.DataFrame({"id": [1, 2, 3], "val": ["A", "B", "C"]})
        tgt = pl.DataFrame({"id": [1, 2, 2, 3, 3, 3], "val": ["A", "B", "B", "C", "C", "C"]})

        message = _rejection_on_both_paths(DiffConfig(primary_keys=["id"]), src, tgt)

        assert "not unique in TARGET dataset. Found 5 duplicate rows" in message

    def test_it_reports_the_source_first_when_both_sides_repeat_keys(self) -> None:
        """Ensure both paths check the datasets in the same order."""
        src = pl.DataFrame({"id": [1, 1], "val": ["A", "A"]})
        tgt = pl.DataFrame({"id": [2, 2, 2], "val": ["B", "B", "B"]})

        message = _rejection_on_both_paths(DiffConfig(primary_keys=["id"]), src, tgt)

        assert "SOURCE" in message

    def test_it_rejects_keys_that_collide_once_normalized(self) -> None:
        """Ensure uniqueness is checked on normalized keys, not stored ones.

        `'A'` and `'a'` are distinct as stored but the same key once a
        case-insensitive rule folds them, so the join would pair both rows.
        """
        src = pl.DataFrame({"code": ["A", "a", "b"], "val": [1, 2, 3]})
        tgt = pl.DataFrame({"code": ["a", "b"], "val": [1, 3]})

        config = DiffConfig(
            primary_keys=["code"],
            rules=[DiffRule(column_names=["code"], case_insensitive=True)],
        )

        message = _rejection_on_both_paths(config, src, tgt)

        assert "not unique in SOURCE dataset. Found 2 duplicate rows" in message

    def test_it_rejects_repeated_null_keys_including_sentinels(self) -> None:
        """Ensure NULL keys count as repeats of each other, as they do locally.

        Polars and SQL `GROUP BY` both place NULL keys in one group, so a
        stored NULL and a sentinel nulled by `null_values` collide.
        """
        src = pl.DataFrame({"id": ["N/A", None, "B"], "val": [1, 2, 3]})
        tgt = pl.DataFrame({"id": ["B"], "val": [3]})

        config = DiffConfig(primary_keys=["id"], default_null_values=["N/A"])

        message = _rejection_on_both_paths(config, src, tgt)

        assert "not unique in SOURCE dataset. Found 2 duplicate rows" in message

    def test_it_checks_composite_keys_as_a_whole(self) -> None:
        """Ensure only a repeated combination fails, not a repeated part."""
        unique = pl.DataFrame({"tenant": [1, 1, 2], "id": [1, 2, 1], "val": [1, 2, 3]})
        repeated = pl.DataFrame({"tenant": [1, 1, 2], "id": [1, 1, 1], "val": [1, 2, 3]})
        config = DiffConfig(primary_keys=["tenant", "id"])

        summary = assert_parity(config, unique, unique)
        message = _rejection_on_both_paths(config, unique, repeated)

        assert summary.is_perfect_match is True
        assert "not unique in TARGET dataset. Found 2 duplicate rows" in message

    def test_it_checks_a_renamed_key_under_its_stored_name(self) -> None:
        """Ensure the check reads a renamed key where the source stores it."""
        src = pl.DataFrame({"legacy_id": [1, 1], "val": ["A", "A"]})
        tgt = pl.DataFrame({"user_id": [1], "val": ["A"]})

        config = DiffConfig(
            primary_keys=["user_id"],
            rules=[DiffRule(column_names=["legacy_id"], rename_to="user_id")],
        )

        message = _rejection_on_both_paths(config, src, tgt)

        assert "Primary keys ['user_id'] are not unique in SOURCE dataset" in message


class TestToleranceScopeParity:
    """Validate that a tolerance only ever loosens a numeric comparison.

    The local engine applies tolerances to columns that are numeric once
    normalized and compares everything else exactly. A global
    `default_absolute_tolerance` reaches every column, so the warehouse has to
    draw the same line instead of subtracting text, booleans, or dates.
    """

    @pytest.mark.parametrize(
        ("source", "target", "rules", "expected_changed"),
        [
            pytest.param(
                pl.Series("val", ["a", "b"]),
                pl.Series("val", ["a", "B"]),
                [],
                1,
                id="text",
            ),
            pytest.param(
                pl.Series("val", [True, False]),
                pl.Series("val", [True, True]),
                [],
                1,
                id="boolean",
            ),
            pytest.param(
                pl.Series("val", [date(2024, 1, 2), date(2024, 1, 2)]),
                pl.Series("val", [date(2024, 1, 2), date(2024, 1, 3)]),
                [],
                1,
                id="date-a-day-apart",
            ),
            pytest.param(
                pl.Series("val", [7, 42]),
                pl.Series("val", ["00007", "00042"]),
                [DiffRule(column_names=["val"], pad_zeros=5)],
                0,
                id="padded-to-text",
            ),
            pytest.param(
                pl.Series("val", ["2024-01-01 00:00:00", "2024-01-01 00:00:00"]),
                pl.Series("val", [datetime(2024, 1, 1), datetime(2024, 1, 1, 0, 0, 1)]),
                [DiffRule(column_names=["val"], datetime_format="%Y-%m-%d %H:%M:%S")],
                1,
                id="parsed-timestamp",
                marks=_REFUSED_PARSE,
            ),
            pytest.param(
                pl.Series("val", ["10.00", "20.00"]),
                pl.Series("val", [10.4, 20.5]),
                [DiffRule(column_names=["val"], cast_to="Float64")],
                0,
                id="cast-to-float-keeps-the-tolerance",
            ),
        ],
    )
    def test_it_agrees_that_tolerances_only_reach_numeric_columns(
        self,
        source: pl.Series,
        target: pl.Series,
        rules: list[DiffRule],
        expected_changed: int,
    ) -> None:
        """Ensure a global tolerance leaves non-numeric columns compared exactly."""
        src = pl.DataFrame({"id": [1, 2]}).with_columns(source)
        tgt = pl.DataFrame({"id": [1, 2]}).with_columns(target)
        config = DiffConfig(primary_keys=["id"], default_absolute_tolerance=1.0, rules=rules)

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == expected_changed


class TestNumericComparisonParity:
    """Validate that both paths compare numbers by value, whatever their storage."""

    @pytest.mark.parametrize(
        ("source", "target", "tolerance", "expected_changed"),
        [
            pytest.param(
                pl.Series("val", [5, 3], dtype=pl.UInt32),
                pl.Series("val", [3, 3], dtype=pl.UInt32),
                1.0,
                1,
                id="unsigned-below-zero",
            ),
            pytest.param(
                pl.Series("val", [100, 1], dtype=pl.Int8),
                pl.Series("val", [-100, 1], dtype=pl.Int8),
                1.0,
                1,
                id="int8-wider-than-its-type",
            ),
            pytest.param(
                pl.Series("val", [-(2**63), 0], dtype=pl.Int64),
                pl.Series("val", [2**63 - 1, 0], dtype=pl.Int64),
                1.0,
                1,
                id="int64-extremes",
            ),
        ],
    )
    def test_it_agrees_on_integer_differences_wider_than_their_type(
        self, source: pl.Series, target: pl.Series, tolerance: float, expected_changed: int
    ) -> None:
        """Ensure a tolerance measures an integer difference exactly on both paths.

        Without widening, DuckDB raises an out-of-range error on each of these,
        and Databricks overflows `ABS` of the smallest BIGINT.
        """
        src = pl.DataFrame({"id": [1, 2]}).with_columns(source)
        tgt = pl.DataFrame({"id": [1, 2]}).with_columns(target)
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["val"], absolute_tolerance=tolerance)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == expected_changed

    @pytest.mark.parametrize(
        ("source", "target", "expected_changed"),
        [
            pytest.param(
                pl.Series("val", [10, 10]), pl.Series("val", [10.7, 10.0]), 1, id="int-vs-float"
            ),
            pytest.param(
                pl.Series("val", [10.7, 10.0]), pl.Series("val", [10, 10]), 1, id="float-vs-int"
            ),
            pytest.param(
                pl.Series("val", [10, 10]),
                pl.Series("val", [Decimal("10.50"), Decimal("10.00")]),
                1,
                id="int-vs-decimal",
            ),
            pytest.param(
                pl.Series("val", [Decimal("10.70"), Decimal("10.70")]),
                pl.Series("val", [10.704, 10.7]),
                1,
                id="decimal-vs-float",
            ),
            pytest.param(
                pl.Series("val", [Decimal("10.50"), Decimal("10.50")], dtype=pl.Decimal(10, 2)),
                pl.Series("val", [Decimal("10.5049"), Decimal("10.5000")], dtype=pl.Decimal(12, 4)),
                1,
                id="decimal-scales",
            ),
        ],
    )
    def test_it_agrees_on_mixed_numeric_types(
        self, source: pl.Series, target: pl.Series, expected_changed: int
    ) -> None:
        """Ensure a local run no longer truncates the target to the source's type.

        A warehouse promotes both sides before comparing, so it always saw
        `10` and `10.7` as different while the local engine cast `10.7` to `10`.
        """
        src = pl.DataFrame({"id": [1, 2]}).with_columns(source)
        tgt = pl.DataFrame({"id": [1, 2]}).with_columns(target)

        summary = assert_parity(DiffConfig(primary_keys=["id"]), src, tgt)

        assert summary.changed_count == expected_changed

    def test_it_agrees_on_a_decimal_source_under_a_tolerance(self) -> None:
        """Ensure the finiteness guard accepts a decimal source on both paths.

        Decimals are always finite. Polars refuses `is_finite` on them, so the
        local engine skips the guard, while the SQL compares the decimal with a
        floating-point infinity.
        """
        src = pl.DataFrame({"id": [1, 2]}).with_columns(
            pl.Series("val", [Decimal("10.00"), Decimal("10.00")], dtype=pl.Decimal(10, 2))
        )
        tgt = pl.DataFrame({"id": [1, 2]}).with_columns(
            pl.Series("val", [Decimal("10.40"), Decimal("11.00")], dtype=pl.Decimal(10, 2))
        )
        config = DiffConfig(primary_keys=["id"], default_absolute_tolerance=0.5)

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    @pytest.mark.parametrize(
        ("source", "target", "tolerance", "matches"),
        [
            pytest.param(float("nan"), 1e6, (0.5, 0.0), False, id="nan-source"),
            pytest.param(float("nan"), 1e6, (0.0, 0.1), False, id="nan-source-relative"),
            pytest.param(float("inf"), 1e308, (0.5, 0.0), False, id="inf-source"),
            pytest.param(float("inf"), 1e308, (0.0, 0.1), False, id="inf-source-relative"),
            pytest.param(float("inf"), float("-inf"), (0.5, 0.0), False, id="inf-vs-neg"),
            pytest.param(
                float("inf"),
                float("-inf"),
                (0.0, 0.1),
                False,
                id="inf-vs-neg-relative",
            ),
            pytest.param(1.0, float("nan"), (0.5, 0.0), False, id="nan-target"),
            pytest.param(1.0, float("inf"), (0.5, 0.0), False, id="inf-target"),
            pytest.param(float("nan"), float("nan"), (0.5, 0.0), True, id="nan-pair"),
            pytest.param(float("inf"), float("inf"), (0.5, 0.0), True, id="inf-pair"),
            pytest.param(float("-inf"), float("-inf"), (0.0, 0.1), True, id="neg-inf-pair"),
            pytest.param(1.0, 1.4, (0.5, 0.0), True, id="finite-within"),
            pytest.param(100.0, 109.0, (0.0, 0.1), True, id="finite-relative"),
        ],
    )
    def test_it_agrees_that_a_tolerance_never_forgives_a_non_finite_value(
        self, source: float, target: float, tolerance: tuple[float, float], matches: bool
    ) -> None:
        """Ensure NaN matches only NaN and an infinity only itself, on both paths.

        The allowance `abs + rel * ABS(src)` is NaN for an infinite source when
        `rel` is 0, and infinite when it is not. Polars, DuckDB, Snowflake, and
        Spark all sort NaN above every number, so both paths used to accept
        any target once the source was not finite.
        """
        src = pl.DataFrame({"id": [1], "val": [source]})
        tgt = pl.DataFrame({"id": [1], "val": [target]})
        absolute, relative = tolerance
        rule = DiffRule(
            column_names=["val"], absolute_tolerance=absolute, relative_tolerance=relative
        )

        summary = assert_parity(DiffConfig(primary_keys=["id"], rules=[rule]), src, tgt)

        assert summary.changed_count == (0 if matches else 1)


class TestSimilarityParity:
    """Validate text similarity limits on both engines."""

    def test_it_runs_jaro_winkler_locally_and_refuses_it_in_a_warehouse(self) -> None:
        """Ensure a limit SQL cannot reproduce is refused rather than approximated."""
        src = pl.DataFrame({"id": [1], "name": ["MARTHA"]})
        tgt = pl.DataFrame({"id": [1], "name": ["MARHTA"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], min_jaro_winkler_similarity=0.96)],
        )

        assert run_local(config, src, tgt).summary.changed_count == 0
        with pytest.raises(ConfigError, match="Column 'name' sets min_jaro_winkler_similarity"):
            run_pushdown(config, src, tgt)

    @_REFUSED_EDIT_DISTANCE
    @pytest.mark.parametrize(("treat_null", "changed"), [(True, 2), (False, 3)])
    def test_it_agrees_on_typos_within_and_beyond_the_limit(
        self, treat_null: bool, changed: int
    ) -> None:
        """Ensure one-letter slips match while other names and NULLs keep their verdicts.

        The data is ASCII: DuckDB counts bytes where every warehouse counts
        characters, which only differ once a character needs two bytes.
        """
        src = pl.DataFrame(
            {"id": [1, 2, 3, 4, 5], "name": ["Jon", "Smith", "Jonathan", None, None]}
        )
        tgt = pl.DataFrame({"id": [1, 2, 3, 4, 5], "name": ["John", "Smyth", "John", "x", None]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["name"],
                    max_levenshtein_distance=1,
                    treat_null_as_equal=treat_null,
                )
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == changed

    @_REFUSED_EDIT_DISTANCE
    @pytest.mark.parametrize(("case_insensitive", "changed"), [(False, 1), (True, 0)])
    def test_it_folds_case_before_measuring_the_distance(
        self, case_insensitive: bool, changed: int
    ) -> None:
        """Ensure stage 3 lowercases both sides before stage 8 counts edits."""
        src = pl.DataFrame({"id": [1], "code": ["ABD"]})
        tgt = pl.DataFrame({"id": [1], "code": ["abc"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["code"],
                    max_levenshtein_distance=1,
                    case_insensitive=case_insensitive,
                )
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == changed

    @pytest.mark.parametrize(
        ("source", "target", "stages", "expected_changed"),
        [
            pytest.param(
                pl.Series("val", ["Jon", "Ann"]),
                pl.Series("val", ["John", "Bob"]),
                {},
                1,
                id="text",
                marks=_REFUSED_EDIT_DISTANCE,
            ),
            pytest.param(pl.Series("val", [12, 20]), pl.Series("val", [13, 20]), {}, 1, id="int"),
            pytest.param(
                pl.Series("val", [True, False]),
                pl.Series("val", [True, True]),
                {},
                1,
                id="boolean",
            ),
            pytest.param(
                pl.Series("val", [date(2024, 1, 2), date(2024, 1, 2)]),
                pl.Series("val", [date(2024, 1, 2), date(2024, 1, 3)]),
                {},
                1,
                id="date-a-day-apart",
            ),
            pytest.param(
                pl.Series("val", [7, 42]),
                pl.Series("val", [8, 42]),
                {"pad_zeros": 5},
                0,
                id="padded-to-text",
                marks=_REFUSED_EDIT_DISTANCE,
            ),
            pytest.param(
                pl.Series("val", ["2024-01-01 00:00:00", "2024-01-01 00:00:00"]),
                pl.Series("val", [datetime(2024, 1, 1), datetime(2024, 1, 1, 0, 0, 1)]),
                {"datetime_format": "%Y-%m-%d %H:%M:%S"},
                1,
                id="parsed-timestamp",
                marks=_REFUSED_PARSE,
            ),
            pytest.param(
                pl.Series("val", [12, 20]),
                pl.Series("val", [13, 20]),
                {"cast_to": "String"},
                0,
                id="cast-to-text",
                marks=_REFUSED_EDIT_DISTANCE,
            ),
            pytest.param(
                pl.Series("val", ["12", "20"]),
                pl.Series("val", ["13", "20"]),
                {"cast_to": "Int64"},
                1,
                id="cast-to-integer",
            ),
        ],
    )
    def test_it_agrees_that_edit_distances_only_reach_text_columns(
        self,
        source: pl.Series,
        target: pl.Series,
        stages: dict[str, object],
        expected_changed: int,
    ) -> None:
        """Ensure numbers, booleans, and dates one step apart still differ in both engines."""
        src = pl.DataFrame({"id": [1, 2]}).with_columns(source)
        tgt = pl.DataFrame({"id": [1, 2]}).with_columns(target)
        rule = DiffRule.model_validate(
            {"column_names": ["val"], "max_levenshtein_distance": 1, **stages}
        )

        summary = assert_parity(DiffConfig(primary_keys=["id"], rules=[rule]), src, tgt)

        assert summary.changed_count == expected_changed

    @pytest.mark.duckdb_only(reason="It pins how DuckDB itself counts edits.")
    def test_it_counts_bytes_on_the_duckdb_stand_in(self) -> None:
        """Pin the harness's one known divergence, which no supported warehouse shares.

        DuckDB's `levenshtein` counts UTF-8 bytes, so `café` and `cafe` are two
        edits apart there and one apart locally, as in Snowflake and Databricks,
        which count characters. That is why the parity data above is ASCII. If
        this starts failing, DuckDB counts characters and the restriction can go.
        """
        src = pl.DataFrame({"id": [1], "name": ["café"]})
        tgt = pl.DataFrame({"id": [1], "name": ["cafe"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], max_levenshtein_distance=1)],
        )

        pushdown, _ = run_pushdown(config, src, tgt)

        assert run_local(config, src, tgt).summary.changed_count == 0
        assert pushdown.summary.changed_count == 1


class TestRowSampleParity:
    """Validate that a pushdown sample shows the rows and values a local run reports.

    Keys are integers, so both engines and every backend order them alike.
    """

    _SOURCE = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": [" ada", "bob", "cy", "dee", "eve"],
            "amt": [10, 20, 30, 40, 50],
        }
    )
    _TARGET = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 6],
            "name": ["ada", "Bob", "cy", "dee", "fay"],
            "amount": [10, 20, 31, None, 60],
        }
    )
    _RULES = [  # noqa: RUF012
        DiffRule(column_names=["name"], whitespace_mode="both"),
        DiffRule(column_names=["amt"], rename_to="amount"),
    ]

    def test_it_samples_every_changed_row_with_the_local_values(self) -> None:
        """Ensure each sampled row carries the normalized values and flags of a local run."""
        config = DiffConfig(primary_keys=["id"], rules=self._RULES, pushdown_sample_rows=100)

        local = run_local(config, self._SOURCE, self._TARGET)
        pushdown, statements = run_pushdown(config, self._SOURCE, self._TARGET)

        sample = pushdown.changed_sample
        assert sample is not None, "\n".join(statements)
        assert sample.columns == [
            "id",
            "name_source",
            "name_target",
            "name_is_match",
            "amount_source",
            "amount_target",
            "amount_is_match",
        ]
        expected = local.changed.select(sample.columns).sort("id")
        assert sample.sort("id").to_dicts() == expected.to_dicts()
        assert sample["id"].to_list() == [2, 3, 4]

    def test_it_takes_the_first_changed_rows_in_key_order(self) -> None:
        """Ensure a small sample is the lowest changed keys, so it repeats run to run."""
        config = DiffConfig(primary_keys=["id"], rules=self._RULES, pushdown_sample_rows=2)

        local = run_local(config, self._SOURCE, self._TARGET)
        pushdown, _ = run_pushdown(config, self._SOURCE, self._TARGET)

        assert pushdown.changed_sample is not None
        assert pushdown.changed_sample["id"].to_list() == sorted(local.changed["id"])[:2]
        assert pushdown.summary.changed_count == local.summary.changed_count == 3


class TestHarnessSensitivity:
    """Prove the harness can actually observe divergence before it is trusted."""

    def test_it_detects_a_deliberately_divergent_rule(self) -> None:
        """Ensure differing configurations produce differing summaries.

        Guards against a harness that agrees with itself no matter what, which
        would make every parity assertion above vacuous.
        """
        src = pl.DataFrame({"id": [1, 2], "status": ["Active", "N/A"]})
        tgt = pl.DataFrame({"id": [1, 2], "status": ["Active", None]})

        strict = DiffConfig(primary_keys=["id"])
        lenient = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["status"],
                    null_values=["N/A"],
                    treat_null_as_equal=True,
                )
            ],
        )

        assert run_local(strict, src, tgt).summary.changed_count == 1
        assert run_pushdown(lenient, src, tgt)[0].summary.changed_count == 0


class TestPushdownRowAccess:
    """Validate what a pushdown result can and cannot hand back."""

    def test_it_returns_primary_keys_only(self) -> None:
        """Ensure a pushdown result is flagged and carries keys rather than values.

        The comparison SQL never projects values, so the frames hold keys and
        the flag is what tells a caller not to expect more.
        """
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [1, 2], "val": ["A", "CHANGED"]})

        result, _ = run_pushdown(DiffConfig(primary_keys=["id"]), src, tgt)

        assert result.keys_only is True
        assert result.changed.columns == ["id"]
        assert result.get_mismatches("val").to_dicts() == [{"id": 2}]

    def test_it_still_rejects_a_column_that_was_not_compared(self) -> None:
        """Ensure the uncompared-column guard survives the keys-only path.

        Pushdown frames cannot reveal which columns were compared, so the
        result records them explicitly. Without that, a mistyped name would
        silently return every changed key.
        """
        src = pl.DataFrame({"id": [1], "val": ["A"]})
        tgt = pl.DataFrame({"id": [1], "val": ["B"]})

        result, _ = run_pushdown(DiffConfig(primary_keys=["id"]), src, tgt)

        with pytest.raises(ConfigError, match="was not compared"):
            result.get_mismatches("vla")


def _proposals_agree(
    config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame, **thresholds: Any
) -> list[ValueMapProposal]:
    """Assert local and warehouse value map proposals are identical, and return them."""
    local = DiffEngine(config, source.lazy(), target.lazy()).propose_value_maps(**thresholds)
    pushdown, statements = run_value_map_pushdown(config, source, target, **thresholds)

    assert pushdown == local, "\n".join(statements)
    return local


def _codes(pairs: Sequence[tuple[str | None, str | None]]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build a keyed source and target from (source, target) value pairs."""
    ids = list(range(len(pairs)))
    return (
        pl.DataFrame(
            {"id": ids, "code": [pair[0] for pair in pairs]}, schema_overrides={"code": pl.String}
        ),
        pl.DataFrame(
            {"id": ids, "code": [pair[1] for pair in pairs]}, schema_overrides={"code": pl.String}
        ),
    )


class TestValueMapProposalParity:
    """Validate that a warehouse proposes exactly the value maps a local run does."""

    def test_it_agrees_at_the_confidence_boundary(self) -> None:
        """Ensure 19 of 20 rows is exactly 0.95 on both engines, identity rows included."""
        source, target = _codes(
            [("M", "Male")] * 19 + [("M", "Man")] + [("F", "Female")] * 10 + [("X", "X")] * 6
        )

        [proposal] = _proposals_agree(DiffConfig(primary_keys=["id"]), source, target)

        assert proposal.value_map == {"M": "Male", "F": "Female"}

    def test_it_agrees_below_the_floor_and_on_support(self) -> None:
        """Ensure 9 of 10 falls short at 0.95, and five agreeing rows need a support of five."""
        source, target = _codes([("U", "Unknown")] * 9 + [("U", "Other")] + [("Q", "Queued")] * 5)
        config = DiffConfig(primary_keys=["id"])

        [proposal] = _proposals_agree(config, source, target, min_support=5)

        assert proposal.value_map == {"Q": "Queued"}
        assert _proposals_agree(config, source, target, min_support=6) == []

    def test_it_agrees_on_nulls(self) -> None:
        """Ensure a NULL target counts toward the total, and a NULL source is never proposed."""
        source, target = _codes(
            [("M", "Male")] * 19 + [("M", None)] + [(None, "Male")] * 5 + [("F", "Female")] * 4
        )

        [proposal] = _proposals_agree(
            DiffConfig(primary_keys=["id"]), source, target, min_support=4
        )

        assert [(entry.source_value, entry.rows) for entry in proposal.entries] == [
            ("M", 20),
            ("F", 4),
        ]

    def test_it_agrees_on_an_existing_map(self) -> None:
        """Ensure entries merge with the governing rule's map, and its outputs are left out."""
        source, target = _codes(
            [("E", "Enterprise")] * 5 + [("Enterprise", "Enterprise")] * 3 + [("P", "Premium")] * 5
        )
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(column_names=["other"], case_insensitive=True),
                DiffRule(column_names=["code"], value_map={"E": "Enterprise"}),
            ],
        )

        [proposal] = _proposals_agree(config, source, target)

        assert proposal.value_map == {"E": "Enterprise", "P": "Premium"}
        assert proposal.governing_rule_index == 1

    def test_it_agrees_on_normalized_and_renamed_columns(self) -> None:
        """Ensure entries are read after case folding and trimming, under the target's name."""
        ids = list(range(12))
        source = pl.DataFrame({"id": ids, "legacy_state": [" active "] * 7 + ["Closed "] * 5})
        target = pl.DataFrame({"id": ids, "state": ["Open"] * 7 + ["Shut"] * 5})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["legacy_state"],
                    rename_to="state",
                    case_insensitive=True,
                    whitespace_mode="both",
                )
            ],
        )

        [proposal] = _proposals_agree(config, source, target)

        assert proposal.column == "state"
        assert proposal.value_map == {"active": "open", "closed": "shut"}

    def test_it_agrees_on_keys_that_join_only_after_normalizing(self) -> None:
        """Ensure a composite key with its own rule joins the same rows on both engines."""
        source = pl.DataFrame(
            {"tenant": [1] * 6, "code": [f"K{i}" for i in range(6)], "tier": ["G"] * 6}
        )
        target = pl.DataFrame(
            {"tenant": [1] * 6, "code": [f"k{i}" for i in range(6)], "tier": ["Gold"] * 6}
        )
        config = DiffConfig(
            primary_keys=["tenant", "code"],
            rules=[DiffRule(column_names=["code"], case_insensitive=True)],
        )

        [proposal] = _proposals_agree(config, source, target)

        assert proposal.value_map == {"G": "Gold"}

    def test_it_agrees_on_the_order_of_tied_entries(self) -> None:
        """Ensure entries with equal support list in the same order on both engines."""
        values = ["c", "C", "a", "b", "d"]
        source, target = _codes([(value, f"{value}!") for value in values for _ in range(5)])

        [proposal] = _proposals_agree(DiffConfig(primary_keys=["id"]), source, target)

        assert [entry.source_value for entry in proposal.entries] == ["C", "a", "b", "c", "d"]

    def test_only_a_local_run_proposes_for_a_non_text_target(self) -> None:
        """Pin the documented difference: a warehouse proposes only for text on both sides."""
        ids = list(range(10))
        source = pl.DataFrame({"id": ids, "flag": ["Y"] * 5 + ["N"] * 5})
        target = pl.DataFrame({"id": ids, "flag": [1] * 5 + [0] * 5})
        config = DiffConfig(primary_keys=["id"])

        local = DiffEngine(config, source.lazy(), target.lazy()).propose_value_maps()
        pushdown, _ = run_value_map_pushdown(config, source, target)

        assert [proposal.value_map for proposal in local] == [{"Y": "1", "N": "0"}]
        assert pushdown == []

    def test_it_agrees_that_repeated_keys_stop_the_proposal(self) -> None:
        """Ensure duplicate keys fail both engines, even before any column is counted."""
        source = pl.DataFrame({"id": [1, 1], "code": ["M", "M"]})
        target = pl.DataFrame({"id": [1, 1], "code": ["Male", "Male"]})
        config = DiffConfig(primary_keys=["id"])

        with pytest.raises(DataIntegrityError):
            DiffEngine(config, source.lazy(), target.lazy()).propose_value_maps()
        with pytest.raises(DataIntegrityError):
            run_value_map_pushdown(config, source, target)

    def test_a_warehouse_sample_is_repeatable_and_partial(self) -> None:
        """Ensure a sampled warehouse proposal reads the same keys every time, and not all."""
        source, target = _codes([("M", "Male")] * 2000)
        config = DiffConfig(primary_keys=["id"])

        first, _ = run_value_map_pushdown(config, source, target, sample_fraction=0.5)
        second, _ = run_value_map_pushdown(config, source, target, sample_fraction=0.5)

        assert first == second
        [entry] = first[0].entries
        assert 0 < entry.rows < 2000
