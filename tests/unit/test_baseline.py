# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for accepted drift: `run --baseline` leaves out the drift a file lists."""

import json
from datetime import date
from pathlib import Path

import duckdb
import polars as pl
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from veridelta._results import accept_pushdown_baseline
from veridelta.cli import main
from veridelta.connectors.duckdb import DuckDBPushdownSession
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import (
    AcceptedChange,
    Baseline,
    DiffConfig,
    DiffResult,
    DiffRule,
    DuckDBConfig,
)
from veridelta.report import render_html, render_markdown

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_SOURCE = {"id": [1, 2, 3, 4], "fare": [10.0, 20.0, 30.0, 40.0], "zone": ["a", "b", "c", "d"]}
_TARGET = {"id": [2, 3, 4, 5], "fare": [20.0, 31.0, 41.0, 50.0], "zone": ["b", "c", "x", "e"]}
"""Row 1 removed, row 5 added, row 3 changed in fare, and row 4 in fare and zone."""


def _engine() -> DiffEngine:
    """Build an engine over the drifting pair, keyed by `id`."""
    return DiffEngine(DiffConfig(primary_keys=["id"]), pl.LazyFrame(_SOURCE), pl.LazyFrame(_TARGET))


def _baseline(**entries: object) -> Baseline:
    """Build a baseline keyed by `id`."""
    return Baseline.model_validate({"primary_keys": ["id"], **entries})


def _duckdb_pair(folder: Path) -> tuple[DuckDBConfig, DuckDBConfig]:
    """Store the drifting pair as two tables of one DuckDB file, compared in place."""
    database = folder / "pair.duckdb"
    with duckdb.connect(str(database)) as writer:
        writer.register("source_frame", pl.DataFrame(_SOURCE).to_arrow())
        writer.register("target_frame", pl.DataFrame(_TARGET).to_arrow())
        writer.execute("CREATE TABLE a AS SELECT * FROM source_frame")
        writer.execute("CREATE TABLE b AS SELECT * FROM target_frame")
    side = {"type": "duckdb", "database": str(database), "pushdown": True}
    return (
        DuckDBConfig.model_validate({**side, "table": "a"}),
        DuckDBConfig.model_validate({**side, "table": "b"}),
    )


class TestAcceptedDrift:
    """Validate what a baseline leaves out of a run, and what it still counts."""

    def test_without_a_baseline_every_row_of_drift_counts(self) -> None:
        """Ensure the pair drifts in four rows when nothing is accepted."""
        summary = _engine().run().summary

        assert (summary.added_count, summary.removed_count, summary.changed_count) == (1, 1, 2)
        assert summary.accepted_count == 0

    def test_it_accepts_every_row_it_lists(self) -> None:
        """Ensure listed rows leave the counts and the verdict, and are counted as accepted."""
        baseline = _baseline(
            added=[{"id": 5}],
            removed=[{"id": 1}],
            changed=[
                {"key": {"id": 3}, "columns": ["fare"]},
                {"key": {"id": 4}, "columns": ["fare", "zone"]},
            ],
        )

        result = _engine().run(baseline=baseline)

        assert result.summary.is_match
        assert result.summary.total_mismatches == 0
        assert result.summary.accepted_count == 4
        assert result.changed.is_empty()

    def test_drift_in_a_column_it_does_not_name_still_counts(self) -> None:
        """Ensure a changed row accepted in one column still drifts in another."""
        baseline = _baseline(changed=[{"key": {"id": 4}, "columns": ["fare"]}])

        summary = _engine().run(baseline=baseline).summary

        assert summary.changed_count == 2
        assert summary.column_mismatches == {"fare": 1, "zone": 1}
        assert summary.accepted_count == 0

    def test_an_entry_for_a_column_that_matches_accepts_nothing(self) -> None:
        """Ensure a changed row's entry accepts only the columns it names that differ."""
        baseline = _baseline(changed=[{"key": {"id": 3}, "columns": ["zone"]}])

        result = _engine().run(baseline=baseline)

        assert (result.summary.changed_count, result.summary.accepted_count) == (2, 0)
        assert result.accepted is not None
        assert result.accepted.changed == []

    def test_a_row_listed_twice_counts_once(self) -> None:
        """Ensure two entries for one row join their columns, and add no row."""
        baseline = _baseline(
            changed=[
                {"key": {"id": 4}, "columns": ["fare"]},
                {"key": {"id": 4}, "columns": ["zone"]},
            ]
        )

        summary = _engine().run(baseline=baseline).summary

        assert (summary.changed_count, summary.accepted_count) == (1, 1)

    def test_it_reads_a_date_key_written_as_text(self) -> None:
        """Ensure a date key, which JSON writes as text, matches the row it names."""
        days = [date(2026, 10, 6), date(2026, 10, 7)]
        engine = DiffEngine(
            DiffConfig(primary_keys=["day"]),
            pl.LazyFrame({"day": days[:1], "n": [1]}),
            pl.LazyFrame({"day": days, "n": [1, 2]}),
        )
        baseline = Baseline.model_validate(
            {"primary_keys": ["day"], "added": [{"day": "2026-10-07"}]}
        )

        summary = engine.run(baseline=baseline).summary

        assert (summary.added_count, summary.accepted_count) == (0, 1)

    def test_a_baseline_for_other_keys_is_refused(self) -> None:
        """Ensure a baseline saved for other primary keys fails before it accepts anything."""
        baseline = Baseline.model_validate({"primary_keys": ["order_id"]})

        with pytest.raises(ConfigError, match="primary keys \\['order_id'\\]"):
            _engine().run(baseline=baseline)

    def test_an_entry_must_name_the_keys(self) -> None:
        """Ensure an entry that names other columns than the keys is not a baseline."""
        with pytest.raises(ValidationError, match="every entry names the primary keys"):
            _baseline(added=[{"order_id": 5}])

    def test_a_pair_compared_where_it_is_stored_accepts_the_same_drift(
        self, tmp_path: Path
    ) -> None:
        """Ensure pushdown accepts what a local run accepts, reading flags for the changed rows alone.

        Row 3 differs in `fare` only, so accepting `fare` settles it. Row 4
        differs in `fare` and `zone`, so it stays changed with `zone` alone
        counted. The flags statement names the two changed rows and no other.
        """
        source, target = _duckdb_pair(tmp_path)
        baseline = _baseline(
            added=[{"id": 5}],
            removed=[{"id": 1}],
            changed=[
                {"key": {"id": 3}, "columns": ["fare"]},
                {"key": {"id": 4}, "columns": ["fare"]},
            ],
        )

        result = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]), source, target, baseline=baseline
        )
        local = _engine().run(baseline=baseline)

        assert result.keys_only
        assert result.summary.model_dump(exclude={"artifacts_written"}) == local.summary.model_dump(
            exclude={"artifacts_written"}
        )
        assert (result.summary.changed_count, result.summary.accepted_count) == (1, 3)
        assert result.summary.column_mismatches == {"zone": 1}
        assert result.changed["id"].to_list() == [4]
        assert result.accepted == local.accepted
        assert result.accepted is not None
        assert [entry.model_dump() for entry in result.accepted.changed] == [
            {"key": {"id": 3}, "columns": ["fare"]},
            {"key": {"id": 4}, "columns": ["fare"]},
        ]

    def test_a_pushdown_baseline_for_other_keys_is_refused_before_any_statement(
        self, tmp_path: Path
    ) -> None:
        """Ensure the key check fails the run before the warehouse is asked anything."""
        source, target = _duckdb_pair(tmp_path)

        with pytest.raises(ConfigError, match="lists rows by the primary keys"):
            DiffEngine.run_from_configs(
                DiffConfig(primary_keys=["id"]),
                source,
                target,
                baseline=Baseline.model_validate(
                    {"primary_keys": ["zone"], "added": [{"zone": "a"}]}
                ),
            )

    def test_a_pushdown_sample_drops_the_rows_a_baseline_settled(self, tmp_path: Path) -> None:
        """Ensure a row sample read before the baseline applied shows no accepted row, and goes when empty."""
        source, target = _duckdb_pair(tmp_path)
        config = DiffConfig(primary_keys=["id"], pushdown_sample_rows=5)
        settling_row_3 = _baseline(changed=[{"key": {"id": 3}, "columns": ["fare"]}])
        settling_both = _baseline(
            changed=[
                {"key": {"id": 3}, "columns": ["fare"]},
                {"key": {"id": 4}, "columns": ["fare", "zone"]},
            ]
        )

        partial = DiffEngine.run_from_configs(config, source, target, baseline=settling_row_3)
        settled = DiffEngine.run_from_configs(config, source, target, baseline=settling_both)

        assert partial.changed_sample is not None
        assert partial.changed_sample["id"].to_list() == [4]
        assert settled.changed_sample is None
        assert settled.summary.changed_count == 0

    def test_a_pushdown_baseline_with_no_compared_column_fetches_no_flags(
        self, tmp_path: Path
    ) -> None:
        """Ensure a run that compares nothing accepts the keys it has and asks for no flags."""
        source, target = _duckdb_pair(tmp_path)
        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["fare", "zone"], ignore=True)]
        )
        baseline = _baseline(added=[{"id": 5}], changed=[{"key": {"id": 3}, "columns": ["fare"]}])

        result = DiffEngine.run_from_configs(config, source, target, baseline=baseline)

        assert (result.summary.added_count, result.summary.removed_count) == (0, 1)
        assert (result.summary.changed_count, result.summary.accepted_count) == (0, 1)

    def test_a_warehouse_that_drops_a_flag_alias_is_an_error(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a flags answer without its aliases fails loudly rather than accepting nothing."""
        source, target = _duckdb_pair(tmp_path)
        real = DuckDBPushdownSession.execute_pushdown

        def without_aliases(
            self: DuckDBPushdownSession, statement: str, query_type: str = "mismatch"
        ) -> pl.LazyFrame:
            if query_type == "flags":
                return pl.LazyFrame({"other": [1]})
            return real(self, statement, query_type)  # type: ignore[arg-type]

        mocker.patch.object(DuckDBPushdownSession, "execute_pushdown", without_aliases)

        with pytest.raises(ConnectorError, match="match flags without"):
            DiffEngine.run_from_configs(
                DiffConfig(primary_keys=["id"]),
                source,
                target,
                baseline=_baseline(changed=[{"key": {"id": 3}, "columns": ["fare"]}]),
            )

    def test_the_reports_say_how_much_it_accepted(self) -> None:
        """Ensure each report names the accepted rows, and only when there are any."""
        accepted = _engine().run(baseline=_baseline(added=[{"id": 5}]))
        plain = _engine().run()

        assert "Accepted:      1\n" in accepted.summary.report_summary
        assert "| Accepted by the baseline | 1 |" in render_markdown(accepted)
        assert ">Accepted<" in render_html(accepted)
        assert "Accepted" not in plain.summary.report_summary
        assert "Accepted by the baseline" not in render_markdown(plain)
        assert ">Accepted<" not in render_html(plain)


class TestBaselineFile:
    """Validate reading a baseline file, and `run --baseline` on the command line."""

    @staticmethod
    def _files(folder: Path) -> None:
        """Write the drifting pair as CSV files and a configuration that compares them."""
        pl.DataFrame(_SOURCE).write_csv(folder / "a.csv")
        pl.DataFrame(_TARGET).write_csv(folder / "b.csv")
        (folder / "veridelta.yaml").write_text(
            "primary_keys: [id]\nsource:\n  path: a.csv\ntarget:\n  path: b.csv\n"
        )

    def test_it_passes_once_the_file_accepts_the_drift(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `run --baseline` exits 0 and prints how many rows it accepted."""
        self._files(tmp_path)
        accepted = {
            "primary_keys": ["id"],
            "added": [{"id": 5}],
            "removed": [{"id": 1}],
            "changed": [
                {"key": {"id": 3}, "columns": ["fare"]},
                {"key": {"id": 4}, "columns": ["fare", "zone"]},
            ],
        }
        (tmp_path / "accepted.json").write_text(json.dumps(accepted))
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "sys.argv", ["veridelta", "run", "--baseline", "accepted.json", "--json"]
        )

        with pytest.raises(SystemExit) as exited:
            main()

        assert exited.value.code == 0
        assert json.loads(capsys.readouterr().out)["accepted_count"] == 4

    @pytest.mark.parametrize(
        ("text", "message"),
        [
            pytest.param(None, "Cannot read the baseline", id="missing"),
            pytest.param("{", "is not valid", id="not-json"),
            pytest.param('{"primary_keys": []}', "is not valid", id="no-keys"),
        ],
    )
    def test_a_file_it_cannot_use_exits_3(
        self,
        text: str | None,
        message: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure a missing or invalid baseline is an error, exit 3, that names the file."""
        self._files(tmp_path)
        if text is not None:
            (tmp_path / "accepted.json").write_text(text)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "sys.argv", ["veridelta", "run", "--baseline", "accepted.json", "--json"]
        )

        with pytest.raises(SystemExit) as exited:
            main()

        error = json.loads(capsys.readouterr().out)["error"]
        assert exited.value.code == 3
        assert error["type"] == "ConfigError"
        assert message in error["message"]
        assert "accepted.json" in error["message"]


class TestSavedBaseline:
    """Validate the baseline a run saves: the drift it found, ready to accept."""

    def test_it_lists_the_drift_with_each_changed_rows_columns(self) -> None:
        """Ensure added and removed rows go by key, and changed rows with their columns."""
        saved = Baseline.of(_engine().run())

        assert saved.added == [{"id": 5}]
        assert saved.removed == [{"id": 1}]
        assert saved.changed == [
            AcceptedChange(key={"id": 3}, columns=["fare"]),
            AcceptedChange(key={"id": 4}, columns=["fare", "zone"]),
        ]

    def test_read_back_it_makes_the_same_run_match(self, tmp_path: Path) -> None:
        """Ensure a saved and read baseline accepts every row of the run it came from."""
        written = Baseline.of(_engine().run()).write(str(tmp_path / "accepted.json"))

        summary = _engine().run(baseline=Baseline.read(str(written))).summary

        assert summary.is_match
        assert summary.accepted_count == 4

    def test_it_keeps_what_the_old_baseline_accepted(self) -> None:
        """Ensure saving under a baseline keeps its entries that still match, and adds the rest."""
        old = _baseline(
            removed=[{"id": 1}, {"id": 9}],
            changed=[{"key": {"id": 4}, "columns": ["zone"]}],
        )

        saved = Baseline.of(_engine().run(baseline=old))

        assert saved == Baseline.of(_engine().run())

    def test_a_date_key_round_trips(self, tmp_path: Path) -> None:
        """Ensure a date key saved as text accepts its row when read back."""
        days = [date(2026, 10, 6), date(2026, 10, 7)]

        def engine() -> DiffEngine:
            return DiffEngine(
                DiffConfig(primary_keys=["day"]),
                pl.LazyFrame({"day": days[:1], "n": [1]}),
                pl.LazyFrame({"day": days, "n": [1, 2]}),
            )

        path = Baseline.of(engine().run()).write(str(tmp_path / "accepted.json"))

        assert json.loads(path.read_text())["added"] == [{"day": "2026-10-07"}]
        assert engine().run(baseline=Baseline.read(str(path))).summary.is_match

    def test_a_pushdown_result_cannot_be_saved(self) -> None:
        """Ensure a result of keys alone, from pushdown, gives no baseline."""
        summary = _engine().run().summary
        result = DiffResult(
            summary=summary,
            added=pl.DataFrame(),
            removed=pl.DataFrame(),
            changed=pl.DataFrame(),
            keys_only=True,
        )

        with pytest.raises(ConfigError, match="compared where it is stored"):
            Baseline.of(result)

    def test_a_file_it_cannot_write_is_an_error(self, tmp_path: Path) -> None:
        """Ensure a baseline written into a folder that does not exist names the file."""
        with pytest.raises(ConfigError, match="Cannot write the baseline"):
            _baseline().write(str(tmp_path / "missing" / "accepted.json"))

    def test_the_command_saves_and_then_accepts_the_drift(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `--save-baseline` keeps the verdict, and `--baseline` then accepts it all."""
        TestBaselineFile._files(tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("sys.argv", ["veridelta", "run", "--save-baseline", "accepted.json"])

        with pytest.raises(SystemExit) as saved:
            main()

        assert saved.value.code == 1
        assert "Baseline saved to:" in capsys.readouterr().err
        monkeypatch.setattr("sys.argv", ["veridelta", "run", "--baseline", "accepted.json"])
        with pytest.raises(SystemExit) as accepted:
            main()
        assert accepted.value.code == 0


class TestPushdownAcceptance:
    """Validate the acceptance a pushdown run applies to its keys, from hand-built flags."""

    _KEYS = ["id"]  # noqa: RUF012
    _COLUMNS = ["fare", "zone"]  # noqa: RUF012

    def _frames(self) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        return (
            pl.DataFrame({"id": [5]}),
            pl.DataFrame({"id": [1]}),
            pl.DataFrame({"id": [3, 4]}),
        )

    def _flags(self) -> pl.DataFrame:
        return pl.DataFrame(
            {"id": [3, 4], "fare_is_match": [False, False], "zone_is_match": [True, False]}
        )

    def test_it_settles_a_row_whose_differing_columns_are_all_accepted(self) -> None:
        """Ensure a fully accepted row leaves, a partly accepted one stays, and the tally follows."""
        baseline = _baseline(
            added=[{"id": 5}],
            changed=[
                {"key": {"id": 3}, "columns": ["fare"]},
                {"key": {"id": 4}, "columns": ["fare"]},
            ],
        )

        kept = accept_pushdown_baseline(
            baseline,
            self._KEYS,
            self._frames(),
            self._COLUMNS,
            self._flags(),
            {"fare": 2, "zone": 1},
        )

        assert kept.added.is_empty()
        assert kept.removed["id"].to_list() == [1]
        assert kept.changed["id"].to_list() == [4]
        assert kept.rows == 2
        assert kept.column_mismatches == {"zone": 1}
        assert [entry.model_dump() for entry in kept.accepted.changed] == [
            {"key": {"id": 3}, "columns": ["fare"]},
            {"key": {"id": 4}, "columns": ["fare"]},
        ]
        assert kept.accepted.added == [{"id": 5}]

    def test_an_entry_for_a_column_that_matches_accepts_nothing(self) -> None:
        """Ensure a column that already matches on the row is not taken off the tally."""
        baseline = _baseline(changed=[{"key": {"id": 3}, "columns": ["zone"]}])

        kept = accept_pushdown_baseline(
            baseline,
            self._KEYS,
            self._frames(),
            self._COLUMNS,
            self._flags(),
            {"fare": 2, "zone": 1},
        )

        assert kept.changed["id"].to_list() == [3, 4]
        assert kept.rows == 0
        assert kept.column_mismatches == {"fare": 2, "zone": 1}
        assert kept.accepted.changed == []

    def test_without_flags_nothing_changed_is_accepted(self) -> None:
        """Ensure an empty flags frame, as a run with no changed entry has, leaves the keys alone."""
        empty = self._flags().clear()
        baseline = _baseline(removed=[{"id": 1}])

        kept = accept_pushdown_baseline(
            baseline, self._KEYS, self._frames(), self._COLUMNS, empty, {"fare": 2, "zone": 1}
        )

        assert kept.removed.is_empty()
        assert kept.changed["id"].to_list() == [3, 4]
        assert (kept.rows, kept.column_mismatches) == (1, {"fare": 2, "zone": 1})
