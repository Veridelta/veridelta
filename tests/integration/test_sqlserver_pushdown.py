# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Live checks of SQL Server pushdown that only a real server can answer.

The parity suite runs the compiled statements against SQL Server through the
harness. These tests go through `DiffEngine.run_from_configs` and the command
line, as `veridelta run` does, so the routing that opens a pushdown session is
exercised too. They run under `make sqlserver`, which sets
`VERIDELTA_PARITY_BACKEND=sqlserver`.
"""

import logging
from pathlib import Path

import polars as pl
import pytest
import yaml

from tests.integration.duckdb_harness import PARITY_BACKEND
from tests.integration.sqlserver_harness import database_source, loaded_tables
from veridelta.cli import EXIT_MISMATCH, build_parser, run
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError
from veridelta.models import DiffConfig, DiffResult, DiffRule

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        PARITY_BACKEND != "sqlserver", reason="Needs a live SQL Server; run make sqlserver."
    ),
]


def _both_settings(
    config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame
) -> tuple[DiffResult, DiffResult]:
    """Compare two loaded tables with `pushdown` on, then off.

    Returns:
        tuple[DiffResult, DiffResult]: The pushdown result, then the local one.
    """
    with loaded_tables(source, target) as (source_table, target_table):
        pushdown = DiffEngine.run_from_configs(
            config,
            database_source(source_table, pushdown=True),
            database_source(target_table, pushdown=True),
        )
        local = DiffEngine.run_from_configs(
            config, database_source(source_table), database_source(target_table)
        )
    return pushdown, local


class TestSqlServerPushdownRouting:
    """Validate that two opted-in SQL Server tables are compared inside SQL Server."""

    def test_it_agrees_with_reading_the_same_tables(self, caplog: pytest.LogCaptureFixture) -> None:
        """Ensure `pushdown: true` checks the server, runs in it, and reaches the local verdict."""
        source = pl.DataFrame({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})
        target = pl.DataFrame({"id": [2, 3, 4], "amount": [20.0, 31.0, 40.0]})

        with caplog.at_level(logging.INFO, logger="veridelta.connectors.database"):
            pushdown, local = _both_settings(DiffConfig(primary_keys=["id"]), source, target)

        assert "is engine edition" in caplog.text
        assert "Ran SQL Server mismatch statement" in caplog.text
        assert (pushdown.summary.added_count, local.summary.added_count) == (1, 1)
        assert (pushdown.summary.removed_count, local.summary.removed_count) == (1, 1)
        assert (pushdown.summary.changed_count, local.summary.changed_count) == (1, 1)
        assert pushdown.summary.column_mismatches == {"amount": 1}

    def test_it_joins_text_keys_character_for_character(self) -> None:
        """Ensure keys that differ only in case, accents, or a trailing space stay apart.

        The harness stores text under a collation that ignores case and
        accents, and `=` ignores trailing spaces under any collation, so a
        join on the stored values would pair every row here.
        """
        source = pl.DataFrame({"code": ["a", "e", "k "], "val": [1, 2, 3]})
        target = pl.DataFrame({"code": ["A", "é", "k"], "val": [1, 2, 3]})

        pushdown, local = _both_settings(DiffConfig(primary_keys=["code"]), source, target)

        assert (pushdown.summary.added_count, local.summary.added_count) == (3, 3)
        assert (pushdown.summary.removed_count, local.summary.removed_count) == (3, 3)

    def test_it_refuses_a_rule_it_has_no_spelling_for_before_comparing(self) -> None:
        """Ensure a `regex_replace` rule stops the run rather than being compared some other way."""
        frame = pl.DataFrame({"id": [1], "code": ["A-1"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], regex_replace={"-": ""})],
        )

        with pytest.raises(ConfigError, match="regex_replace cannot be pushed down to tsql"):
            _both_settings(config, frame, frame)

    def test_it_refuses_a_value_sql_server_would_change_in_a_literal(self) -> None:
        """Ensure a backslash before a line break, which SQL Server drops, is never sent."""
        frame = pl.DataFrame({"id": [1], "code": ["a"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], value_map={"a\\\nb": "c"})],
        )

        with pytest.raises(ConfigError, match="line continuation"):
            _both_settings(config, frame, frame)


class TestCommandLine:
    """Validate `veridelta run` on two SQL Server tables, as a configuration file names them."""

    def test_run_compares_inside_sql_server_and_exits_on_drift(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a YAML pair with `pushdown: true` runs and exits 1 on a case-only change."""
        source = pl.DataFrame({"id": [1, 2], "status": ["open", "closed"]})
        target = pl.DataFrame({"id": [1, 2], "status": ["open", "Closed"]})
        with loaded_tables(source, target) as (source_table, target_table):
            sides = {
                side: database_source(table, pushdown=True).model_dump(mode="json")
                for side, table in (("source", source_table), ("target", target_table))
            }
            config = tmp_path / "veridelta.yaml"
            config.write_text(yaml.safe_dump({**sides, "primary_keys": ["id"]}), encoding="utf-8")

            code = run(build_parser().parse_args(["run", "-c", str(config), "--quiet"]))

        capsys.readouterr()
        assert code == EXIT_MISMATCH
