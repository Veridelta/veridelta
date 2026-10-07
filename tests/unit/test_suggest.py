# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for `veridelta suggest`: rules that explain the differences, with their evidence."""

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import polars as pl
import pytest
import yaml

from veridelta.cli import main
from veridelta.engine import DiffEngine, _round_up
from veridelta.exceptions import ConfigError
from veridelta.models import DiffConfig, DiffRule, DuckDBConfig, RuleSuggestion

pytestmark = [pytest.mark.unit, pytest.mark.fast]


def _engine(
    source: Mapping[str, Sequence[object]],
    target: Mapping[str, Sequence[object]],
    **config: object,
) -> DiffEngine:
    """Build an engine over two frames keyed by `id`."""
    return DiffEngine(
        DiffConfig.model_validate({"primary_keys": ["id"], **config}),
        pl.LazyFrame(source),
        pl.LazyFrame(target),
    )


def _rounded(rows: int = 10) -> DiffEngine:
    """Compare fares the target rounds differently, by at most 0.004, beside zones that match."""
    ids = list(range(1, rows + 1))
    zones = [i % 3 for i in ids]
    return _engine(
        {"id": ids, "fare": [10.0 + i for i in ids], "zone": zones},
        {
            "id": ids,
            "fare": [10.0 + i + 0.004 * (i % 2) + 0.003 * (1 - i % 2) for i in ids],
            "zone": zones,
        },
    )


class TestRoundUp:
    """Hold the round value a tolerance gets to the first one above the gap."""

    @pytest.mark.parametrize(
        ("gap", "rounded"),
        [
            pytest.param(0.004, 0.005, id="to-five"),
            pytest.param(0.005, 0.01, id="above-not-equal"),
            pytest.param(0.3, 0.5, id="tenths"),
            pytest.param(0.15, 0.2, id="to-two"),
            pytest.param(7.0, 10.0, id="to-the-next-power"),
            pytest.param(1e-7, 2e-7, id="tiny"),
        ],
    )
    def test_it_picks_the_first_round_value_above_the_gap(self, gap: float, rounded: float) -> None:
        """Ensure 1, 2, or 5 times a power of ten, strictly above the gap, as written."""
        assert _round_up(gap) == rounded


class TestSuggestRules:
    """Validate what `DiffEngine.suggest_rules` suggests, and the evidence it gives."""

    def test_it_suggests_an_absolute_tolerance_for_rounding(self) -> None:
        """Ensure gaps of one size give an absolute tolerance that explains every row."""
        (suggestion,) = _rounded().suggest_rules()

        assert suggestion.column == "fare"
        assert suggestion.settings == {"absolute_tolerance": 0.005}
        assert suggestion.explained == suggestion.differing == 10
        assert suggestion.largest_gap == pytest.approx(0.004)
        assert suggestion.examples == ({"id": 1}, {"id": 2}, {"id": 3})
        assert suggestion.rule == DiffRule(column_names=["fare"], absolute_tolerance=0.005)
        assert suggestion.governing_rule_index is None

    def test_it_suggests_a_relative_tolerance_for_a_rate(self) -> None:
        """Ensure gaps that grow with the values give a relative tolerance."""
        ids = list(range(1, 11))
        engine = _engine(
            {"id": ids, "tip": [float(i * 3) for i in ids]},
            {"id": ids, "tip": [i * 3 * 1.004 for i in ids]},
        )

        (suggestion,) = engine.suggest_rules()

        assert suggestion.settings == {"relative_tolerance": 0.005}
        assert suggestion.explained == 10

    def test_a_zero_source_value_picks_an_absolute_tolerance(self) -> None:
        """Ensure a gap from 0, which no relative tolerance reaches, gets an absolute one."""
        engine = _engine({"id": [1, 2], "x": [0.0, 100.0]}, {"id": [1, 2], "x": [0.001, 100.9]})

        (suggestion,) = engine.suggest_rules(max_share=1)

        assert list(suggestion.settings) == ["absolute_tolerance"]
        assert suggestion.explained == 2

    def test_it_suggests_nothing_for_a_change_past_the_share(self) -> None:
        """Ensure a gap past `max_share` reads as a change, unless the share is raised."""
        engine = _engine({"id": [1, 2], "x": [10.0, 20.0]}, {"id": [1, 2], "x": [11.0, 20.0]})

        assert engine.suggest_rules() == []
        assert engine.suggest_rules(max_share=0.1)[0].settings == {"absolute_tolerance": 2.0}

    def test_it_counts_a_null_as_a_difference_it_cannot_explain(self) -> None:
        """Ensure a row with a null on one side counts as differing, and never as explained."""
        engine = _engine(
            {"id": [1, 2, 3], "x": [10.0, 20.0, 30.0]},
            {"id": [1, 2, 3], "x": [10.004, 20.004, None]},
        )

        (suggestion,) = engine.suggest_rules()

        assert (suggestion.explained, suggestion.differing) == (2, 3)

    def test_it_keeps_the_gap_between_large_integers(self) -> None:
        """Ensure integers subtract exactly, since Float64 rounds two nanosecond times to one."""
        base = 1_700_000_000_123_456_789
        engine = _engine(
            {"id": [1, 2], "ns": [base, base + 1000]},
            {"id": [1, 2], "ns": [base + 50, base + 1050]},
        )

        (suggestion,) = engine.suggest_rules()

        assert suggestion.settings == {"absolute_tolerance": 100.0}
        assert (suggestion.largest_gap, suggestion.explained) == (50.0, 2)

    @pytest.mark.parametrize("target", [[10.0, 20.0], [10.0, 20.004]], ids=["equal", "near"])
    def test_it_suggests_nothing_where_strict_types_fail_the_column(
        self, target: list[float]
    ) -> None:
        """Ensure a type that differs under `strict_types`, which no tolerance forgives, gets no rule."""
        engine = _engine(
            {"id": [1, 2], "x": [10, 20]}, {"id": [1, 2], "x": target}, strict_types=True
        )

        assert engine.suggest_rules() == []

    def test_it_suggests_nothing_for_a_flag(self) -> None:
        """Ensure a boolean column, which no tolerance or text setting loosens, gets no rule."""
        engine = _engine({"id": [1], "paid": [True]}, {"id": [1], "paid": [False]})

        assert engine.suggest_rules() == []

    def test_it_suggests_nothing_for_text_that_changed(self) -> None:
        """Ensure text that differs in more than whitespace and case gets no rule."""
        engine = _engine({"id": [1], "status": ["open"]}, {"id": [1], "status": ["shut"]})

        assert engine.suggest_rules() == []


class TestTextSuggestions:
    """Validate trimming and case folding, suggested for text that differs only in them."""

    @pytest.mark.parametrize(
        ("source", "target", "settings"),
        [
            pytest.param(
                ["open", "shut"], ["open ", " shut"], {"whitespace_mode": "both"}, id="trim"
            ),
            pytest.param(["Open", "SHUT"], ["open", "shut"], {"case_insensitive": True}, id="fold"),
            pytest.param(
                [" Open", "shut"],
                ["open", "SHUT"],
                {"whitespace_mode": "both", "case_insensitive": True},
                id="both",
            ),
        ],
    )
    def test_it_suggests_only_the_settings_the_rows_need(
        self, source: list[str], target: list[str], settings: dict[str, object]
    ) -> None:
        """Ensure trimming or case folding alone when it is enough, and both when not."""
        engine = _engine({"id": [1, 2], "status": source}, {"id": [1, 2], "status": target})

        (suggestion,) = engine.suggest_rules()

        assert suggestion.settings == settings
        assert suggestion.rule == DiffRule.model_validate({"column_names": ["status"], **settings})
        assert (suggestion.explained, suggestion.largest_gap) == (2, None)

    def test_it_counts_a_change_and_a_null_as_unexplained(self) -> None:
        """Ensure a real change, or a null on one side, differs still and is left out."""
        engine = _engine(
            {"id": [1, 2, 3], "status": ["open ", "open", "open"]},
            {"id": [1, 2, 3], "status": ["open", "shut", None]},
        )

        (suggestion,) = engine.suggest_rules()

        assert (suggestion.explained, suggestion.differing) == (1, 3)
        assert suggestion.examples == ({"id": 1},)

    def test_it_never_suggests_a_rule_that_makes_a_match_differ(self) -> None:
        """Ensure case folding, which would hide a value map's capital keys, is not suggested."""
        engine = _engine(
            {"id": [1, 2], "gender": ["M", "f"]},
            {"id": [1, 2], "gender": ["Male", "F"]},
            rules=[{"column_names": ["gender"], "value_map": {"M": "Male", "F": "Female"}}],
        )

        assert engine.suggest_rules() == []

    def test_it_keeps_the_settings_of_the_rule_that_governs_the_column(self) -> None:
        """Ensure a column a pattern governs keeps that rule's transforms, named on its own."""
        engine = _engine(
            {"id": [1, 2], "fare_amount": ["$10.00", "$20.00"]},
            {"id": [1, 2], "fare_amount": [10.004, 20.004]},
            rules=[{"pattern": ".*_amount$", "regex_replace": {r"\$": ""}, "cast_to": "Float64"}],
        )

        (suggestion,) = engine.suggest_rules()

        assert suggestion.governing_rule_index == 0
        assert suggestion.explained == 2
        assert suggestion.rule == DiffRule(
            column_names=["fare_amount"],
            regex_replace={r"\$": ""},
            cast_to="Float64",
            absolute_tolerance=0.005,
        )

    def test_it_leaves_the_engine_able_to_run(self) -> None:
        """Ensure suggesting works on a copy, so the engine still runs as configured."""
        engine = _rounded()

        engine.suggest_rules()

        assert engine.run().summary.column_mismatches == {"fare": 10}

    def test_it_writes_no_artifacts(self, tmp_path: Path) -> None:
        """Ensure the comparisons a suggestion runs never write discrepancy files."""
        engine = _engine(
            {"id": [1], "x": [1.0]}, {"id": [1], "x": [1.001]}, output_path=str(tmp_path / "out")
        )

        engine.suggest_rules(max_share=0.01)

        assert not (tmp_path / "out").exists()

    @pytest.mark.parametrize("share", [0, -0.1, 1.5, "0.1", True])
    def test_it_refuses_a_share_that_cannot_bound_a_tolerance(self, share: object) -> None:
        """Ensure `max_share` must be a number above 0 and at most 1."""
        with pytest.raises(ConfigError, match="max_share must be"):
            _rounded().suggest_rules(max_share=share)  # type: ignore[arg-type]

    def test_it_refuses_a_pair_compared_where_it_is_stored(self, tmp_path: Path) -> None:
        """Ensure a pushdown pair, whose rows stay where they are, is refused before connecting."""
        side = {"type": "duckdb", "database": str(tmp_path / "x.duckdb"), "pushdown": True}
        source = DuckDBConfig.model_validate({**side, "table": "a"})
        target = DuckDBConfig.model_validate({**side, "table": "b"})

        with pytest.raises(ConfigError, match="reads both sides locally"):
            DiffEngine.suggest_rules_from_configs(DiffConfig(primary_keys=["id"]), source, target)

    def test_its_suggestions_round_trip_as_json(self) -> None:
        """Ensure a suggestion, examples and rule included, reads back from its JSON."""
        (suggestion,) = _rounded().suggest_rules()

        assert RuleSuggestion.model_validate_json(suggestion.model_dump_json()) == suggestion


class TestSentinelSuggestions:
    """Validate null sentinels, suggested where one side spells NULL and the other holds it."""

    @pytest.mark.parametrize(
        ("source", "target", "sentinels"),
        [
            pytest.param(["N/A", "Oslo"], [None, "Oslo"], ["N/A"], id="text"),
            pytest.param(["Oslo", ""], ["Oslo", None], [""], id="empty-text"),
            pytest.param([None, 4.5], [-999.0, 4.5], [-999.0], id="number-on-the-target"),
        ],
    )
    def test_it_suggests_the_spelling_of_null_it_finds(
        self, source: list[object], target: list[object], sentinels: list[object]
    ) -> None:
        """Ensure a common spelling of NULL where the other side is NULL becomes a sentinel."""
        engine = _engine({"id": [1, 2], "x": source}, {"id": [1, 2], "x": target})

        (suggestion,) = engine.suggest_rules()

        assert suggestion.settings == {"null_values": sentinels}
        assert suggestion.explained == 1

    def test_it_leaves_a_real_value_beside_null_alone(self) -> None:
        """Ensure a value that is not a spelling of NULL, such as a city, is no sentinel."""
        engine = _engine({"id": [1], "city": ["Oslo"]}, {"id": [1], "city": [None]})

        assert engine.suggest_rules() == []

    def test_it_never_suggests_a_flag(self) -> None:
        """Ensure `false` where the other side is NULL, too often meant, is no sentinel."""
        engine = _engine({"id": [1], "paid": [False]}, {"id": [1], "paid": [None]})

        assert engine.suggest_rules() == []

    def test_it_keeps_the_sentinels_the_column_has_today(self) -> None:
        """Ensure a new sentinel joins the defaults, which a rule's list would replace."""
        engine = _engine(
            {"id": [1, 2, 3], "x": ["N/A", "-", "Oslo"]},
            {"id": [1, 2, 3], "x": [None, None, "Oslo"]},
            default_null_values=["-"],
        )

        (suggestion,) = engine.suggest_rules()

        assert suggestion.settings == {"null_values": ["N/A"]}
        assert suggestion.rule.null_values == ["-", "N/A"]

    def test_it_suggests_nothing_the_configuration_refuses(self) -> None:
        """Ensure a text sentinel for a column that is a number on the other side is left out."""
        engine = _engine({"id": [1, 2], "x": ["N/A", "5"]}, {"id": [1, 2], "x": [None, 5]})

        assert engine.suggest_rules() == []

    def test_it_suggests_nothing_when_null_never_equals_null(self) -> None:
        """Ensure a sentinel that would only turn a match into NULL against NULL is left out."""
        engine = _engine(
            {"id": [1, 2], "x": ["N/A", "N/A"]},
            {"id": [1, 2], "x": [None, "N/A"]},
            default_treat_null_as_equal=False,
        )

        assert engine.suggest_rules() == []

    def test_it_joins_a_tolerance_and_a_sentinel_in_one_rule(self) -> None:
        """Ensure a column with rounding and a sentinel gets one rule with both settings."""
        engine = _engine(
            {"id": [1, 2, 3], "fare": [10.0, 20.0, -999.0]},
            {"id": [1, 2, 3], "fare": [10.004, 20.003, None]},
        )

        (suggestion,) = engine.suggest_rules()

        assert suggestion.settings == {"absolute_tolerance": 0.005, "null_values": [-999.0]}
        assert suggestion.explained == 3
        assert suggestion.largest_gap == pytest.approx(0.004)


class TestSuggestCommand:
    """Validate `veridelta suggest` on the command line."""

    @staticmethod
    def _files(folder: Path, target_fare: str, rules: str = "") -> None:
        """Write two CSV files of three fares, and a configuration that compares them."""
        (folder / "a.csv").write_text("id,fare\n1,10.00\n2,20.00\n3,30.00\n")
        (folder / "b.csv").write_text(f"id,fare\n1,{target_fare}\n2,20.004\n3,30.004\n")
        (folder / "veridelta.yaml").write_text(
            f"primary_keys: [id]\nsource:\n  path: a.csv\ntarget:\n  path: b.csv\n{rules}"
        )

    def test_it_prints_the_rules_on_stdout_and_the_evidence_on_stderr(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure stdout holds only YAML rules to paste, and stderr says what each explains."""
        self._files(tmp_path, "10.004")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("sys.argv", ["veridelta", "suggest"])

        with pytest.raises(SystemExit) as exited:
            main()

        captured = capsys.readouterr()
        assert exited.value.code == 0
        assert yaml.safe_load(captured.out) == {
            "rules": [{"column_names": ["fare"], "absolute_tolerance": 0.005}]
        }
        assert "fare: absolute_tolerance 0.005 explains 3 of 3 differing rows" in captured.err
        assert "for example id=1; id=2; id=3" in captured.err

    def test_it_prints_a_text_rule_without_a_gap(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a trimming rule prints as YAML, and its evidence names no gap."""
        (tmp_path / "a.csv").write_text("id,city\n1,Oslo\n2,Lima\n")
        (tmp_path / "b.csv").write_text("id,city\n1,Oslo \n2,Lima\n")
        (tmp_path / "veridelta.yaml").write_text(
            "primary_keys: [id]\nsource:\n  path: a.csv\ntarget:\n  path: b.csv\n"
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("sys.argv", ["veridelta", "suggest"])

        with pytest.raises(SystemExit):
            main()

        captured = capsys.readouterr()
        assert yaml.safe_load(captured.out) == {
            "rules": [{"column_names": ["city"], "whitespace_mode": "both"}]
        }
        assert "city: whitespace_mode both explains 1 of 1 differing rows\n" in captured.err

    def test_it_says_when_no_rule_explains_the_differences(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a change past the share prints no rule, says so, and still exits 0."""
        self._files(tmp_path, "15.00")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("sys.argv", ["veridelta", "suggest"])

        with pytest.raises(SystemExit) as exited:
            main()

        captured = capsys.readouterr()
        assert exited.value.code == 0
        assert captured.out == ""
        assert "No rule explains the differences within --max-share." in captured.err

    def test_it_notes_the_rule_that_governs_a_column_even_when_quiet(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure the note on where the rule goes survives `--quiet`: pasted last, it does nothing."""
        self._files(tmp_path, "10.004", rules="rules:\n  - pattern: f.*\n    cast_to: Float64\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("sys.argv", ["veridelta", "suggest", "--quiet"])

        with pytest.raises(SystemExit):
            main()

        err = capsys.readouterr().err
        assert "Note: rules[0] governs 'fare' today." in err
        assert "put it first in rules" in err
        assert "explains" not in err

    def test_it_prints_json_and_exits_3_when_it_cannot_finish(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a missing configuration prints one error object under `--json`."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("sys.argv", ["veridelta", "suggest", "-c", "missing.yaml", "--json"])

        with pytest.raises(SystemExit) as exited:
            main()

        assert exited.value.code == 3
        assert json.loads(capsys.readouterr().out)["error"]["type"] == "ConfigError"

    @pytest.mark.parametrize("share", ["0", "1.5", "much"])
    def test_it_refuses_a_share_out_of_range(
        self, share: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `--max-share` outside (0, 1] is an invalid argument, exit 2."""
        monkeypatch.setattr("sys.argv", ["veridelta", "suggest", "--max-share", share])

        with pytest.raises(SystemExit) as exited:
            main()

        assert exited.value.code == 2
        assert "--max-share" in capsys.readouterr().err
