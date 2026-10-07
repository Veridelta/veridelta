# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for accepted drift: `run --baseline` leaves out the drift a file lists."""

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from pydantic import ValidationError

from veridelta.cli import main
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError
from veridelta.models import Baseline, DiffConfig, DuckDBConfig
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

    def test_a_pair_compared_where_it_is_stored_refuses_a_baseline(self, tmp_path: Path) -> None:
        """Ensure pushdown, whose rows stay where they are, refuses a baseline before connecting."""
        side = {"type": "duckdb", "database": str(tmp_path / "x.duckdb"), "pushdown": True}
        source = DuckDBConfig.model_validate({**side, "table": "a"})
        target = DuckDBConfig.model_validate({**side, "table": "b"})

        with pytest.raises(ConfigError, match="Leave out --baseline"):
            DiffEngine.run_from_configs(
                DiffConfig(primary_keys=["id"]), source, target, baseline=_baseline()
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
