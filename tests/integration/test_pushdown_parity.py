# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Differential parity tests between the local engine and compiled pushdown SQL."""

from datetime import date, datetime, timezone
from decimal import Decimal

import polars as pl
import pytest

from tests.integration.duckdb_harness import assert_parity, run_local, run_pushdown
from veridelta.exceptions import ConfigError, DataIntegrityError
from veridelta.models import DiffConfig, DiffRule


@pytest.mark.integration
@pytest.mark.slow
class TestBaselineParity:
    """Validate the harness itself on comparisons with no transform rules."""

    def test_it_agrees_on_a_clean_comparison(self) -> None:
        """Ensure identical relations report a perfect match on both paths."""
        frame = pl.DataFrame({"id": [1, 2, 3], "amount": [10.5, 20.0, 30.0]})

        summary = assert_parity(DiffConfig(primary_keys=["id"]), frame, frame)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_added_removed_and_changed_rows(self) -> None:
        """Ensure the three anti-join and inner-join tallies line up exactly."""
        src = pl.DataFrame({"id": [1, 2, 3], "val": ["A", "B", "C"]})
        tgt = pl.DataFrame({"id": [1, 2, 4], "val": ["A", "CHANGED", "D"]})

        summary = assert_parity(DiffConfig(primary_keys=["id"]), src, tgt)

        assert summary.added_count == 1
        assert summary.removed_count == 1
        assert summary.changed_count == 1
        assert summary.column_mismatches == {"val": 1}

    def test_it_agrees_when_a_null_meets_a_value(self) -> None:
        """Ensure SQL's three-valued logic is folded to False like `fill_null`.

        A bare `=` against NULL yields NULL, not False. Without the compiler's
        `COALESCE(..., FALSE)` the row would be dropped from the mismatch join
        and the pushdown count would understate the drift.
        """
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [1, 2], "val": ["A", None]})

        summary = assert_parity(DiffConfig(primary_keys=["id"]), src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_when_nulls_are_treated_as_equal(self) -> None:
        """Ensure `IS NOT DISTINCT FROM` matches `fill_null` null-safe equality."""
        src = pl.DataFrame({"id": [1, 2], "val": [None, "B"]})
        tgt = pl.DataFrame({"id": [1, 2], "val": [None, "B"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["val"], treat_null_as_equal=True)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_that_an_unmatched_null_is_a_mismatch(self) -> None:
        """Ensure null-safe equality still fails when only one side is null."""
        src = pl.DataFrame({"id": [1, 2], "val": [None, "B"]})
        tgt = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["val"], treat_null_as_equal=True)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1


@pytest.mark.integration
@pytest.mark.slow
class TestShippedStageParity:
    """Validate the transform stages that shipped before complete parity."""

    def test_it_agrees_on_numeric_tolerances(self) -> None:
        """Ensure `ABS(src - tgt) <= tol` matches the local tolerance check."""
        src = pl.DataFrame({"id": [1, 2, 3], "cost": [10.00, 20.00, 30.00]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "cost": [10.02, 20.04, 30.20]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["cost"], absolute_tolerance=0.05)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_on_typed_null_sentinels(self) -> None:
        """Ensure sentinel coercion collapses to NULL on both paths."""
        src = pl.DataFrame({"id": [1, 2], "status": ["Active", "N/A"]})
        tgt = pl.DataFrame({"id": [1, 2], "status": ["Active", None]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["status"],
                    null_values=["N/A"],
                    treat_null_as_equal=True,
                )
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_when_a_sentinel_cannot_apply_to_the_dtype(self) -> None:
        """Ensure a numeric column never receives a string sentinel comparison.

        The local engine skips the stage and the compiler must skip it too,
        rather than emitting `amount IN ('N/A')` for the warehouse to choke on.
        """
        src = pl.DataFrame({"id": [1, 2], "amount": [10, 20]})
        tgt = pl.DataFrame({"id": [1, 2], "amount": [10, 99]})

        config = DiffConfig(primary_keys=["id"], default_null_values=["N/A"])

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_on_regex_replace(self) -> None:
        """Ensure `REGEXP_REPLACE` strips the same characters Polars does."""
        src = pl.DataFrame({"id": [1, 2], "cost": ["$10.00", "$20.50"]})
        tgt = pl.DataFrame({"id": [1, 2], "cost": ["10.00", "20.50"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["cost"], regex_replace={"\\$": ""})],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_whitespace_and_case_normalization(self) -> None:
        """Ensure trimming and case folding land on the same normalized strings."""
        src = pl.DataFrame({"id": [1, 2, 3], "name": ["  Ada ", "grace", "LINUS "]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "name": ["ADA", "Grace  ", " torvalds"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["name"],
                    whitespace_mode="both",
                    case_insensitive=True,
                )
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_on_source_side_value_mapping(self) -> None:
        """Ensure the CASE chain maps the same keys as the Polars replace."""
        src = pl.DataFrame({"id": [1, 2, 3], "tier": ["Enterprise", "Premium", "Standard"]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "tier": ["ENT", "PRM", "BASIC"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["tier"],
                    value_map={"Enterprise": "ENT", "Premium": "PRM", "Standard": "STD"},
                )
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_when_a_text_stage_meets_a_numeric_side(self) -> None:
        """Ensure text stages skip a non-text side the way the local engine does.

        A migration that cleans a currency string on the left and compares it
        to a native float on the right hits this on every row: Polars skips the
        regex on the float, and the compiler has to skip it too rather than ask
        the warehouse to run `REGEXP_REPLACE` over a number.
        """
        src = pl.DataFrame({"id": [1, 2], "cost": ["$10.00", "$99.99"]})
        tgt = pl.DataFrame({"id": [1, 2], "cost": [10.0, 99.98]})

        config = DiffConfig(
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
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

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


@pytest.mark.integration
@pytest.mark.slow
class TestPaddingParity:
    """Validate stage 5 against Python's `str.zfill`, sign and overflow included."""

    def test_it_agrees_on_unsigned_padding(self) -> None:
        """Ensure short values gain leading zeros identically."""
        src = pl.DataFrame({"id": [1, 2], "code": ["7", "42"]})
        tgt = pl.DataFrame({"id": [1, 2], "code": ["00007", "00042"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], pad_zeros=5)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_negative_numbers(self) -> None:
        """Ensure padding lands after the sign, not before it.

        A bare `LPAD('-12', 4, '0')` yields `0-12`. Polars yields `-012`, so an
        unguarded implementation reports drift on every negative value.
        """
        src = pl.DataFrame({"id": [1, 2, 3], "code": ["-12", "-7", "-1234"]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "code": ["-012", "-007", "-1234"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], pad_zeros=4)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_explicitly_positive_numbers(self) -> None:
        """Ensure a leading `+` is preserved the way `str.zfill` preserves it."""
        src = pl.DataFrame({"id": [1], "code": ["+7"]})
        tgt = pl.DataFrame({"id": [1], "code": ["+007"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], pad_zeros=4)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_that_padding_never_truncates(self) -> None:
        """Ensure over-long input survives intact.

        `LPAD` truncates from the right when the input exceeds the width, which
        would make two distinct long codes collide.
        """
        src = pl.DataFrame({"id": [1, 2], "code": ["1234567", "1234599"]})
        tgt = pl.DataFrame({"id": [1, 2], "code": ["1234567", "1234500"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], pad_zeros=3)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_on_padded_integers(self) -> None:
        """Ensure a numeric column and a zero-padded text column converge."""
        src = pl.DataFrame({"id": [1, 2], "code": [7, -42]})
        tgt = pl.DataFrame({"id": [1, 2], "code": ["00007", "-0042"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], pad_zeros=5)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_that_padding_propagates_nulls(self) -> None:
        """Ensure a NULL stays NULL through every branch of the pad expression."""
        src = pl.DataFrame({"id": [1, 2], "code": [None, "7"]})
        tgt = pl.DataFrame({"id": [1, 2], "code": [None, "007"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], pad_zeros=3, treat_null_as_equal=True)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_a_zero_width_pad(self) -> None:
        """Ensure width zero still stringifies without padding anything."""
        src = pl.DataFrame({"id": [1, 2], "code": [7, 42]})
        tgt = pl.DataFrame({"id": [1, 2], "code": ["7", "42"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], pad_zeros=0)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True


@pytest.mark.integration
@pytest.mark.slow
class TestCastParity:
    """Validate stage 7 against Polars cast semantics."""

    def test_it_agrees_on_a_string_to_float_cast(self) -> None:
        """Ensure a cleaned currency string compares as a number."""
        src = pl.DataFrame({"id": [1, 2], "cost": ["$10.00", "$99.99"]})
        tgt = pl.DataFrame({"id": [1, 2], "cost": [10.0, 99.98]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["cost"],
                    regex_replace={"\\$": ""},
                    cast_to="Float64",
                )
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_that_a_float_to_integer_cast_truncates(self) -> None:
        """Ensure the cast truncates toward zero rather than rounding.

        Polars truncates; Snowflake and DuckDB round. Without the explicit
        guard, 10.7 compares as 11 under pushdown and 10 locally.
        """
        src = pl.DataFrame({"id": [1, 2, 3, 4], "n": [10.7, -10.7, 10.5, 11.5]})
        tgt = pl.DataFrame({"id": [1, 2, 3, 4], "n": [10, -10, 10, 11]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["n"], cast_to="Int64")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_an_integer_to_string_cast(self) -> None:
        """Ensure a numeric column and its text spelling converge."""
        src = pl.DataFrame({"id": [1, 2], "code": [7, -42]})
        tgt = pl.DataFrame({"id": [1, 2], "code": ["7", "-42"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], cast_to="String")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_a_string_to_date_cast(self) -> None:
        """Ensure ISO date text parses to the same calendar day."""
        src = pl.DataFrame({"id": [1, 2], "day": ["2026-01-02", "2026-03-04"]})
        tgt = pl.DataFrame({"id": [1, 2], "day": [date(2026, 1, 2), date(2026, 3, 5)]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["day"], cast_to="Date")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_when_padding_feeds_a_cast(self) -> None:
        """Ensure stage 5 output flows into stage 7 in the documented order."""
        src = pl.DataFrame({"id": [1, 2], "code": [7, 42]})
        tgt = pl.DataFrame({"id": [1, 2], "code": ["00007", "00042"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], pad_zeros=5, cast_to="String")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True


@pytest.mark.integration
@pytest.mark.slow
class TestDatetimeFormatParity:
    """Validate stage 6a.

    DuckDB reads Python directives natively, so these tests prove the parse is
    wired into the right stage with the right null behavior. They say nothing
    about the Snowflake and Databricks translation tables, which are pinned by
    string assertions in `tests/unit/test_sql_compiler.py` instead.
    """

    def test_it_agrees_on_a_parsed_timestamp(self) -> None:
        """Ensure legacy timestamp text compares against native timestamps."""
        src = pl.DataFrame({"id": [1, 2], "ts": ["2026-01-02 15:30:45", "2026-03-04 01:02:03"]})
        tgt = pl.DataFrame(
            {
                "id": [1, 2],
                "ts": [datetime(2026, 1, 2, 15, 30, 45), datetime(2026, 3, 4, 1, 2, 4)],
            }
        )

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d %H:%M:%S")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_that_unparseable_text_becomes_null(self) -> None:
        """Ensure a bad row yields NULL rather than aborting the statement.

        Polars parses non-strictly, so one malformed value nulls that row and
        the run continues. A strict SQL parser would fail the whole query.
        """
        src = pl.DataFrame({"id": [1, 2], "ts": ["2026-01-02", "not a date"]})
        tgt = pl.DataFrame({"id": [1, 2], "ts": ["2026-01-02", "also not a date"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["ts"],
                    datetime_format="%Y-%m-%d",
                    treat_null_as_equal=True,
                )
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_when_padding_precedes_parsing(self) -> None:
        """Ensure stage 5 output is what stage 6a parses."""
        src = pl.DataFrame({"id": [1], "ts": [20260102]})
        tgt = pl.DataFrame({"id": [1], "ts": ["20260102"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], pad_zeros=8, datetime_format="%Y%m%d")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_a_parsed_offset(self) -> None:
        """Ensure `%z` lands on the same instant on both paths."""
        src = pl.DataFrame({"id": [1], "ts": ["2026-01-02 15:30:45+0200"]})
        tgt = pl.DataFrame({"id": [1], "ts": ["2026-01-02 13:30:45+0000"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d %H:%M:%S%z")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True


@pytest.mark.integration
@pytest.mark.slow
class TestTimezoneParity:
    """Validate stage 6b, where the local conversion is metadata-only."""

    def test_it_agrees_on_a_converted_timestamp(self) -> None:
        """Ensure a zone rule leaves the compared instants untouched."""
        src = pl.DataFrame(
            {
                "id": [1, 2],
                "ts": [
                    datetime(2026, 1, 2, 2, 30, tzinfo=timezone.utc),
                    datetime(2026, 7, 2, 15, 30, tzinfo=timezone.utc),
                ],
            }
        )
        tgt = pl.DataFrame(
            {
                "id": [1, 2],
                "ts": [
                    datetime(2026, 1, 2, 2, 30, tzinfo=timezone.utc),
                    datetime(2026, 7, 2, 16, 30, tzinfo=timezone.utc),
                ],
            }
        )

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], timezone="America/New_York")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_when_a_converted_timestamp_is_cast_to_a_date(self) -> None:
        """Ensure the cast reads the UTC instant on both paths.

        The source instant is 2026-01-01 21:30 in New York but 2026-01-02 in
        UTC. Polars casts through the instant and yields the UTC day, so a
        warehouse that shifted to wall-clock time first would disagree.
        """
        src = pl.DataFrame({"id": [1], "ts": [datetime(2026, 1, 2, 2, 30, tzinfo=timezone.utc)]})
        tgt = pl.DataFrame({"id": [1], "ts": [datetime(2026, 1, 2, 2, 30, tzinfo=timezone.utc)]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["ts"],
                    timezone="America/New_York",
                    cast_to="Date",
                )
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_rejects_a_naive_timestamp_on_both_paths(self) -> None:
        """Ensure pushdown refuses to guess an origin zone, exactly as local does."""
        frame = pl.DataFrame({"id": [1], "ts": [datetime(2026, 1, 2, 2, 30)]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], timezone="America/New_York")],
        )

        with pytest.raises(ConfigError, match="timezone-naive"):
            run_local(config, frame, frame)
        with pytest.raises(ConfigError, match="timezone-naive"):
            run_pushdown(config, frame, frame)

    def test_it_rejects_a_non_temporal_column_on_both_paths(self) -> None:
        """Ensure a zone rule on a text column fails before any comparison."""
        frame = pl.DataFrame({"id": [1], "ts": ["2026-01-02"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], timezone="America/New_York")],
        )

        with pytest.raises(ConfigError, match="not a"):
            run_local(config, frame, frame)
        with pytest.raises(ConfigError, match="not a"):
            run_pushdown(config, frame, frame)


@pytest.mark.integration
@pytest.mark.slow
class TestEdgeCaseParity:
    """Validate join shapes, literal escaping, and rule precedence at the edges.

    Every stage above is proven in isolation. These cases cover the seams
    between stages and the join: composite keys, renamed columns, empty inner
    joins, quote characters inside data literals, and what happens when two
    rules claim the same column.
    """

    def test_it_agrees_on_relative_tolerance_alone(self) -> None:
        """Ensure `rel_tol * ABS(src)` scales the allowance with the source value."""
        src = pl.DataFrame({"id": [1, 2, 3], "cost": [100.0, 100.0, 1000.0]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "cost": [100.5, 102.0, 1005.0]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["cost"], relative_tolerance=0.01)],
        )

        summary = assert_parity(config, src, tgt)

        # 0.5 and 5.0 sit inside 1% of their source; 2.0 does not.
        assert summary.changed_count == 1

    def test_it_agrees_that_absolute_and_relative_tolerances_add(self) -> None:
        """Ensure the allowance is `abs_tol + rel_tol * ABS(src)`, not the larger of the two.

        A 1.0 drift on 100.0 fails 0.6 absolute alone and fails 0.5% relative
        alone, yet passes their sum. Whichever engine picked `max` instead of
        `+` would flag the row.
        """
        src = pl.DataFrame({"id": [1, 2], "cost": [100.0, 100.0]})
        tgt = pl.DataFrame({"id": [1, 2], "cost": [101.0, 101.2]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(column_names=["cost"], absolute_tolerance=0.6, relative_tolerance=0.005)
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_when_a_tolerance_meets_null_safe_equality(self) -> None:
        """Ensure the tolerance branch still folds NULLs the way stage 9 asks."""
        src = pl.DataFrame({"id": [1, 2, 3, 4], "cost": [10.0, None, 30.0, None]})
        tgt = pl.DataFrame({"id": [1, 2, 3, 4], "cost": [10.04, None, 30.5, 5.0]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(column_names=["cost"], absolute_tolerance=0.05, treat_null_as_equal=True)
            ],
        )

        summary = assert_parity(config, src, tgt)

        # Row 2 is a null pair, row 3 exceeds the tolerance, row 4 is one-sided.
        assert summary.changed_count == 2

    def test_it_agrees_on_composite_primary_keys(self) -> None:
        """Ensure every key column participates in both the anti-joins and the inner join."""
        src = pl.DataFrame({"tenant": [1, 1, 2], "id": [1, 2, 1], "val": ["A", "B", "C"]})
        tgt = pl.DataFrame({"tenant": [1, 1, 2], "id": [1, 2, 2], "val": ["A", "X", "D"]})

        summary = assert_parity(DiffConfig(primary_keys=["tenant", "id"]), src, tgt)

        assert summary.added_count == 1
        assert summary.removed_count == 1
        assert summary.changed_count == 1
        assert summary.column_mismatches == {"val": 1}

    def test_it_agrees_on_a_renamed_column_end_to_end(self) -> None:
        """Ensure `rename_to` pairs the two spellings and reports drift under the target name."""
        src = pl.DataFrame({"id": [1, 2, 3], "legacy_amt": [10.0, 20.0, 30.0]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "amount": [10.0, 25.0, 30.0]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["legacy_amt"], rename_to="amount")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1
        assert summary.column_mismatches == {"amount": 1}

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

    def test_it_agrees_on_apostrophes_inside_data_literals(self) -> None:
        """Ensure `'` inside sentinels, crosswalks, and regexes survives SQL quoting.

        Each literal reaches the statement through `_literal`, which doubles
        the quote. A raw interpolation would either break the statement or,
        worse, end the string early and compare against the wrong value.
        """
        src = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "owner": ["O'Brien", "Smith", "D'Angelo"],
                "phrase": ["it's", "that's", "ok"],
            }
        )
        tgt = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "owner": [None, "Smith", "DAngelo"],
                "phrase": ["its", "thats", "ok"],
            }
        )

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["owner"],
                    null_values=["O'Brien"],
                    regex_replace={"'": ""},
                    treat_null_as_equal=True,
                ),
                DiffRule(column_names=["phrase"], value_map={"it's": "its", "that's": "thats"}),
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_when_no_primary_key_overlaps(self) -> None:
        """Ensure an empty inner join yields no changed rows and an empty tally.

        `SUM` over zero rows is NULL in SQL; the reducer must read that as
        nothing to report, exactly like the local engine's empty frame.
        """
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [3, 4], "val": ["C", "D"]})

        summary = assert_parity(DiffConfig(primary_keys=["id"]), src, tgt)

        assert summary.added_count == 2
        assert summary.removed_count == 2
        assert summary.changed_count == 0
        assert summary.column_mismatches == {}

    def test_it_agrees_when_every_non_key_column_is_ignored(self) -> None:
        """Ensure a keys-only comparison still counts added and removed rows."""
        src = pl.DataFrame({"id": [1, 2], "audit": ["x", "y"]})
        tgt = pl.DataFrame({"id": [1, 3], "audit": ["p", "q"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["audit"], ignore=True)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.added_count == 1
        assert summary.removed_count == 1
        assert summary.changed_count == 0
        assert summary.column_mismatches == {}

    def test_it_agrees_on_a_boolean_cast(self) -> None:
        """Ensure 0/1 flags and native booleans converge through `cast_to='Boolean'`.

        Integers are the portable input here: Polars refuses to cast text such
        as `'true'` to Boolean, so a rule doing that fails locally before any
        parity question arises.
        """
        src = pl.DataFrame({"id": [1, 2, 3], "flag": [1, 0, 1]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "flag": [True, False, False]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["flag"], cast_to="Boolean")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_on_a_renamed_column_that_carries_a_tolerance(self) -> None:
        """Ensure a `rename_to` rule's other settings survive the rename.

        The local engine used to look rules up by the renamed spelling, which
        the rule does not list, so the tolerance silently fell away there
        while pushdown, resolving by the source spelling, still applied it.
        """
        src = pl.DataFrame({"id": [1, 2], "legacy_amt": [10.0, 20.0]})
        tgt = pl.DataFrame({"id": [1, 2], "amount": [10.0, 20.04]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(column_names=["legacy_amt"], rename_to="amount", absolute_tolerance=0.05)
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_a_renamed_column_normalized_on_both_sides(self) -> None:
        """Ensure a rename rule's transforms reach the target spelling too."""
        src = pl.DataFrame({"id": [1, 2], "legacy_name": ["ada", "grace"]})
        tgt = pl.DataFrame({"id": [1, 2], "name": [" ada ", "Grace"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["legacy_name"],
                    rename_to="name",
                    whitespace_mode="both",
                    case_insensitive=True,
                )
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_when_a_rule_names_the_target_spelling(self) -> None:
        """Ensure a rule written against the renamed spelling governs the pair on both paths."""
        src = pl.DataFrame({"id": [1, 2], "legacy_amt": [10.0, 20.0]})
        tgt = pl.DataFrame({"id": [1, 2], "amount": [10.0, 20.04]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(column_names=["legacy_amt"], rename_to="amount"),
                DiffRule(column_names=["amount"], absolute_tolerance=0.05),
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_that_an_exact_rule_outranks_a_pattern_ignore(self) -> None:
        """Ensure an ignore pattern cannot hide a column an exact-name rule claims.

        Precedence is exact names before patterns, for `ignore` as for every
        other field. The local engine used to drop any column an ignore rule
        matched, skipping a column the configuration asked it to compare.
        """
        src = pl.DataFrame({"id": [1, 2], "_etl_batch_id": [1, 2], "_etl_loaded_at": ["a", "b"]})
        tgt = pl.DataFrame({"id": [1, 2], "_etl_batch_id": [1, 3], "_etl_loaded_at": ["x", "y"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(pattern="^_etl_", ignore=True),
                DiffRule(column_names=["_etl_batch_id"]),
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1
        assert summary.column_mismatches == {"_etl_batch_id": 1}

    def test_it_agrees_on_a_renamed_primary_key(self) -> None:
        """Ensure a key renamed between systems joins on both paths.

        The source relation stores the key under its old name, so pushdown
        reads it there and projects it under the name the join uses.
        """
        src = pl.DataFrame({"legacy_id": [1, 2, 3], "val": ["A", "B", "C"]})
        tgt = pl.DataFrame({"user_id": [1, 2, 4], "val": ["A", "X", "D"]})

        config = DiffConfig(
            primary_keys=["user_id"],
            rules=[DiffRule(column_names=["legacy_id"], rename_to="user_id")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1
        assert summary.added_count == 1
        assert summary.removed_count == 1

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

    def test_it_agrees_when_header_normalization_changes_nothing(self) -> None:
        """Ensure names already in normalized form still compare with the flag on."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [1, 2], "val": ["A", "C"]})
        config = DiffConfig(primary_keys=["id"], normalize_column_names=True)

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1

    def test_it_agrees_on_row_totals_when_a_key_is_null(self) -> None:
        """Ensure a row with a null key still counts toward the totals.

        The totals are the threshold's denominator. The local engine used to
        count non-null values of the first key, so it disagreed with the
        warehouse's `COUNT(*)` and could flip the verdict near the threshold.
        """
        src = pl.DataFrame({"id": [1, 2, None], "val": ["A", "B", "C"]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "val": ["A", "B", "C"]})

        summary = assert_parity(DiffConfig(primary_keys=["id"], threshold=0.7), src, tgt)

        # A null key never joins, so that row is removed and key 3 is added.
        assert summary.total_rows_source == 3
        assert summary.removed_count == 1
        assert summary.added_count == 1
        assert summary.is_match is True

    def test_it_agrees_that_the_first_matching_rule_wins(self) -> None:
        """Ensure both engines resolve a doubly-ruled column to the same rule.

        Exact names beat patterns, and among exact names the first declared
        wins. A looser second rule must not widen the tolerance on either path.
        """
        src = pl.DataFrame({"id": [1, 2], "cost": [10.0, 20.0], "count": [1, 2]})
        tgt = pl.DataFrame({"id": [1, 2], "cost": [10.04, 21.0], "count": [1, 5]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(pattern="^co", absolute_tolerance=100.0),
                DiffRule(column_names=["cost"], absolute_tolerance=0.05),
                DiffRule(column_names=["cost"], absolute_tolerance=10.0),
            ],
        )

        summary = assert_parity(config, src, tgt)

        # `cost` takes the strict exact-name rule; `count` falls through to the pattern.
        assert summary.changed_count == 1
        assert summary.column_mismatches == {"cost": 1}


@pytest.mark.integration
@pytest.mark.slow
class TestKeyNormalizationParity:
    """Validate that primary keys pass through stages 1-7 on both paths.

    The local engine normalizes keys before it joins. Pushdown used to join
    keys as stored, so these cases reported added and removed rows in the
    warehouse for keys a local run matched.
    """

    def test_it_agrees_on_keys_under_a_global_whitespace_mode(self) -> None:
        """Ensure a global default reaches key columns, not only compared ones."""
        src = pl.DataFrame({"id": ["  A", "B "], "val": [1, 2]})
        tgt = pl.DataFrame({"id": ["A", "B"], "val": [1, 3]})

        summary = assert_parity(
            DiffConfig(primary_keys=["id"], default_whitespace_mode="both"), src, tgt
        )

        assert summary.added_count == 0
        assert summary.removed_count == 0
        assert summary.changed_count == 1

    def test_it_agrees_on_a_case_insensitive_key(self) -> None:
        """Ensure rows whose keys differ only by case are matched and compared."""
        src = pl.DataFrame({"email": ["Ada@Example.com", "grace@example.com"], "val": [1, 2]})
        tgt = pl.DataFrame({"email": ["ada@example.com", "GRACE@EXAMPLE.COM"], "val": [1, 2]})

        config = DiffConfig(
            primary_keys=["email"],
            rules=[DiffRule(column_names=["email"], case_insensitive=True)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_a_zero_padded_text_key(self) -> None:
        """Ensure account numbers stored with and without leading zeros join once padded."""
        src = pl.DataFrame({"acct": ["7", "42"], "val": ["A", "B"]})
        tgt = pl.DataFrame({"acct": ["00007", "00042"], "val": ["A", "B"]})

        config = DiffConfig(
            primary_keys=["acct"],
            rules=[DiffRule(column_names=["acct"], pad_zeros=5)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_a_padded_key_stored_as_different_types(self) -> None:
        """Ensure a numeric key and its zero-padded text form join once padded.

        DuckDB would coerce the raw join here anyway, so this guards the padded
        cross-type path rather than proving the key is normalized.
        """
        src = pl.DataFrame({"acct": [7, 42], "val": ["A", "B"]})
        tgt = pl.DataFrame({"acct": ["00007", "00042"], "val": ["A", "B"]})

        config = DiffConfig(
            primary_keys=["acct"],
            rules=[DiffRule(column_names=["acct"], pad_zeros=5)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_a_cast_key_stored_as_different_types(self) -> None:
        """Ensure a text key cast to an integer joins an integer key.

        Like the padded cross-type case, DuckDB would coerce the raw join too,
        so this guards the cast path rather than proving normalization.
        """
        src = pl.DataFrame({"id": ["1", "2"], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [1, 2], "val": ["A", "C"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["id"], cast_to="Int64")],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1
        assert summary.added_count == 0

    def test_it_agrees_on_a_crosswalked_key(self) -> None:
        """Ensure a source-side `value_map` rewrites legacy key codes before the join."""
        src = pl.DataFrame({"code": ["M", "F"], "val": [1, 2]})
        tgt = pl.DataFrame({"code": ["Male", "Female"], "val": [1, 2]})

        config = DiffConfig(
            primary_keys=["code"],
            rules=[DiffRule(column_names=["code"], value_map={"M": "Male", "F": "Female"})],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_on_a_renamed_key_its_rename_rule_normalizes(self) -> None:
        """Ensure the rule that renames a key also normalizes it on both paths."""
        src = pl.DataFrame({"legacy_id": ["ab-1", "CD-2"], "val": [1, 2]})
        tgt = pl.DataFrame({"user_id": ["AB-1", "cd-2"], "val": [1, 2]})

        config = DiffConfig(
            primary_keys=["user_id"],
            rules=[
                DiffRule(column_names=["legacy_id"], rename_to="user_id", case_insensitive=True)
            ],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.is_perfect_match is True

    def test_it_agrees_that_a_sentinel_key_never_joins(self) -> None:
        """Ensure a key nulled by a sentinel is reported as removed and added.

        NULL never equals anything in a join, locally or in a warehouse, so
        the placeholder rows stay unmatched on both paths while still counting
        toward the totals.
        """
        src = pl.DataFrame({"id": ["N/A", "B"], "val": [1, 2]})
        tgt = pl.DataFrame({"id": ["N/A", "B"], "val": [1, 2]})

        summary = assert_parity(
            DiffConfig(primary_keys=["id"], default_null_values=["N/A"]), src, tgt
        )

        assert summary.removed_count == 1
        assert summary.added_count == 1
        assert summary.total_rows_source == 2

    def test_it_agrees_on_a_composite_key_with_one_normalized_part(self) -> None:
        """Ensure a rule on one key column leaves the other key column as stored."""
        src = pl.DataFrame({"tenant": [1, 1, 2], "code": ["a", "B", "c"], "val": [1, 2, 3]})
        tgt = pl.DataFrame({"tenant": [1, 1, 3], "code": ["A", "b", "c"], "val": [1, 9, 3]})

        config = DiffConfig(
            primary_keys=["tenant", "code"],
            rules=[DiffRule(column_names=["code"], case_insensitive=True)],
        )

        summary = assert_parity(config, src, tgt)

        assert summary.changed_count == 1
        assert summary.added_count == 1
        assert summary.removed_count == 1


def _rejection_on_both_paths(config: DiffConfig, src: pl.DataFrame, tgt: pl.DataFrame) -> str:
    """Assert both engines refuse duplicate keys with the same message, and return it."""
    with pytest.raises(DataIntegrityError) as local:
        run_local(config, src, tgt)
    with pytest.raises(DataIntegrityError) as pushdown:
        run_pushdown(config, src, tgt)

    assert str(pushdown.value) == str(local.value)
    return str(local.value)


@pytest.mark.integration
@pytest.mark.slow
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


@pytest.mark.integration
@pytest.mark.slow
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


@pytest.mark.integration
@pytest.mark.slow
class TestNumericComparisonParity:
    """Validate that both paths compare numbers by value, whatever their storage."""

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


@pytest.mark.integration
@pytest.mark.slow
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


@pytest.mark.integration
@pytest.mark.slow
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
