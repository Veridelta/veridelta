# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Differential parity tests between the local engine and compiled pushdown SQL."""

from datetime import date, datetime, timezone

import polars as pl
import pytest

from tests.integration.duckdb_harness import assert_parity, run_local, run_pushdown
from veridelta.exceptions import ConfigError
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
