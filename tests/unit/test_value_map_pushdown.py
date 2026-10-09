# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for value map proposals: thresholds and the warehouse route."""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pytest
from pytest_mock import MockerFixture

from veridelta.connectors.sql import (
    COUNT_ALIAS,
    VALUE_MAP_AGREEING_ALIAS,
    VALUE_MAP_COLUMN_ALIAS,
    VALUE_MAP_ROWS_ALIAS,
    VALUE_MAP_SOURCE_ALIAS,
    VALUE_MAP_TARGET_ALIAS,
    SQLDialect,
    SQLPushdownCompiler,
)
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import (
    DiffConfig,
    DiffRule,
    SnowflakeConfig,
    ValueMapProposal,
)

pytestmark = [pytest.mark.unit, pytest.mark.fast]


def _engine() -> DiffEngine:
    """Build an engine over one matching row, enough to reach the threshold checks."""
    frame = pl.DataFrame({"id": [1], "code": ["M"]}).lazy()
    return DiffEngine(DiffConfig(primary_keys=["id"]), frame, frame)


class TestValueMapThresholdTypes:
    """Validate that proposal thresholds are real numbers before anything reads rows."""

    @pytest.mark.parametrize(
        ("thresholds", "match"),
        [
            pytest.param(
                {"min_confidence": True}, "min_confidence must be a number", id="bool-confidence"
            ),
            pytest.param(
                {"min_confidence": Decimal("0.95")},
                "min_confidence must be a number",
                id="decimal-confidence",
            ),
            pytest.param(
                {"sample_fraction": True}, "sample_fraction must be a number", id="bool-share"
            ),
            pytest.param(
                {"min_support": True}, "min_support must be a whole number", id="bool-support"
            ),
            pytest.param(
                {"min_support": 5.0}, "min_support must be a whole number", id="float-support"
            ),
            pytest.param(
                {"min_support": Decimal("5")},
                "min_support must be a whole number",
                id="decimal-support",
            ),
        ],
    )
    def test_it_rejects_thresholds_that_are_not_real_numbers(
        self, thresholds: dict[str, Any], match: str
    ) -> None:
        """Ensure `True` or a Decimal never passes as a threshold, since `min_support` reaches SQL."""
        with pytest.raises(ConfigError, match=match):
            _engine().propose_value_maps(**thresholds)

    def test_it_accepts_whole_numbers_where_a_share_is_expected(self) -> None:
        """Ensure `1` is as good as `1.0` for a confidence or a sample share."""
        assert (
            _engine().propose_value_maps(min_confidence=1, sample_fraction=1, min_support=1) == []
        )


_PROBE = pl.DataFrame(schema={"ID": pl.Int64(), "GENDER": pl.String(), "AGE": pl.Int64()})
"""What both Snowflake schema probes report."""


def _snowflake(table: str) -> SnowflakeConfig:
    """Build one side of a Snowflake pair on a shared connection."""
    return SnowflakeConfig(
        account="xy12345",
        user="analyst",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
        table=table,
    )


def _session(mocker: MockerFixture, evidence: pl.DataFrame) -> MagicMock:
    """Patch the Snowflake connector with a session that answers each round trip."""
    session: MagicMock = mocker.patch("veridelta._warehouses.SnowflakeConnector").return_value
    session.compiler = SQLPushdownCompiler(SQLDialect.SNOWFLAKE)

    def answer(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
        if query_type == "schema":
            return _PROBE.lazy()
        if query_type == "duplicates":
            return pl.DataFrame({COUNT_ALIAS: [0]}).lazy()
        return evidence.lazy()

    session.execute_pushdown.side_effect = answer
    return session


def _evidence(**columns: Any) -> pl.DataFrame:
    """Build a value map result, with defaults for the columns not given."""
    defaults: dict[str, Any] = {
        VALUE_MAP_COLUMN_ALIAS: [0, 0],
        VALUE_MAP_SOURCE_ALIAS: ["M", "F"],
        VALUE_MAP_TARGET_ALIAS: ["Male", "Female"],
        VALUE_MAP_ROWS_ALIAS: [Decimal(20), Decimal(10)],
        VALUE_MAP_AGREEING_ALIAS: [Decimal(19), Decimal(10)],
    }
    return pl.DataFrame({**defaults, **columns})


def _propose(**thresholds: Any) -> list[ValueMapProposal]:
    """Propose value maps for the Snowflake pair."""
    return DiffEngine.propose_value_maps_from_configs(
        DiffConfig(primary_keys=["ID"]), _snowflake("SRC"), _snowflake("TGT"), **thresholds
    )


class TestWarehouseValueMapRouting:
    """Validate how a warehouse pair's proposals are collected."""

    def test_it_counts_in_one_statement_after_the_key_checks(self, mocker: MockerFixture) -> None:
        """Ensure two probes, two key checks, and one count run, and the session closes."""
        session = _session(mocker, _evidence())

        [proposal] = _propose()

        assert [call.kwargs["query_type"] for call in session.execute_pushdown.call_args_list] == [
            "schema",
            "schema",
            "duplicates",
            "duplicates",
            "value_maps",
        ]
        session.close.assert_called_once()
        assert proposal.column == "GENDER"
        assert [(entry.source_value, entry.confidence) for entry in proposal.entries] == [
            ("M", 0.95),
            ("F", 1.0),
        ]

    def test_it_applies_the_confidence_floor_itself(self, mocker: MockerFixture) -> None:
        """Ensure a pair the SQL keeps as possible is dropped below `min_confidence`."""
        _session(mocker, _evidence())

        [proposal] = _propose(min_confidence=0.99)

        assert [entry.source_value for entry in proposal.entries] == ["F"]

    def test_it_reads_categorical_text(self, mocker: MockerFixture) -> None:
        """Ensure text a driver delivers as Categorical still forms entries."""
        _session(
            mocker,
            _evidence(**{VALUE_MAP_SOURCE_ALIAS: pl.Series(["M", "F"], dtype=pl.Categorical)}),
        )

        [proposal] = _propose()

        assert proposal.value_map == {"M": "Male", "F": "Female"}

    def test_it_skips_the_count_without_a_text_column(self, mocker: MockerFixture) -> None:
        """Ensure no count runs when no column is text on both sides, after the key checks."""
        session = _session(mocker, _evidence())
        config = DiffConfig(
            primary_keys=["ID"], rules=[DiffRule(column_names=["GENDER"], ignore=True)]
        )

        proposals = DiffEngine.propose_value_maps_from_configs(
            config, _snowflake("SRC"), _snowflake("TGT")
        )

        assert proposals == []
        assert session.execute_pushdown.call_count == 4

    @pytest.mark.parametrize(
        ("evidence", "match"),
        [
            pytest.param(
                _evidence().drop(VALUE_MAP_ROWS_ALIAS), "missing the columns", id="missing-column"
            ),
            pytest.param(
                _evidence(**{VALUE_MAP_AGREEING_ALIAS: ["many", "few"]}),
                "values of the wrong type",
                id="text-count",
            ),
        ],
    )
    def test_it_rejects_a_malformed_result_and_still_closes(
        self, mocker: MockerFixture, evidence: pl.DataFrame, match: str
    ) -> None:
        """Ensure a result the engine cannot read is a connector error, never a guess."""
        session = _session(mocker, evidence)

        with pytest.raises(ConnectorError, match=match):
            _propose()

        session.close.assert_called_once()

    def test_it_checks_thresholds_before_connecting(self, mocker: MockerFixture) -> None:
        """Ensure a bad threshold fails before a warehouse is reached."""
        connector = mocker.patch("veridelta._warehouses.SnowflakeConnector")

        with pytest.raises(ConfigError, match="min_support must be a whole number"):
            _propose(min_support=True)

        connector.assert_not_called()
