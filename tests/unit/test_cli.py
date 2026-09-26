# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Veridelta command-line interface."""

import argparse
import json
from unittest.mock import MagicMock

import pytest
import yaml
from pytest_mock import MockerFixture

from veridelta.cli import build_parser, crosswalk, main, run
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import DiffConfig, DiffRule, ValueMapEntry, ValueMapProposal


@pytest.mark.unit
@pytest.mark.fast
class TestCommandLineInterface:
    """Validate CLI argument parsing, workflow execution, and exit codes."""

    @pytest.fixture
    def default_args(self) -> argparse.Namespace:
        """Provide a default argparse namespace for testing the run function."""
        return argparse.Namespace(
            config="dummy.yaml", json=False, quiet=False, html=None, html_max_rows=1000
        )

    def test_it_returns_exit_code_zero_when_datasets_match(
        self, mocker: MockerFixture, default_args: argparse.Namespace
    ) -> None:
        """Ensure a successful comparison returns a 0 exit code for CI/CD pipelines."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")

        mock_diff_config = MagicMock(output_path=None)
        mock_source = MagicMock()
        mock_target = MagicMock()
        mock_load.return_value = (mock_diff_config, mock_source, mock_target)

        mock_summary = MagicMock(is_match=True, report_summary="Status: PASSED")
        mock_engine.run_from_configs.return_value = MagicMock(summary=mock_summary)

        exit_code = run(default_args)

        assert exit_code == 0
        mock_load.assert_called_once_with("dummy.yaml")
        mock_engine.run_from_configs.assert_called_once_with(
            mock_diff_config, mock_source, mock_target
        )

    def test_it_returns_exit_code_one_when_datasets_do_not_match(
        self, mocker: MockerFixture, default_args: argparse.Namespace
    ) -> None:
        """Ensure a failed comparison returns a 1 exit code to halt pipelines."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")

        mock_diff_config = MagicMock(output_path=None)
        mock_load.return_value = (mock_diff_config, MagicMock(), MagicMock())

        mock_summary = MagicMock(is_match=False, report_summary="Status: FAILED")
        mock_engine.run_from_configs.return_value = MagicMock(summary=mock_summary)

        exit_code = run(default_args)

        assert exit_code == 1

    def test_it_prints_artifact_paths_when_output_is_configured_and_mismatch_occurs(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure users are notified of where artifacts are saved upon failure."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")

        mock_diff_config = MagicMock(output_path="/tmp/diffs")
        mock_load.return_value = (mock_diff_config, MagicMock(), MagicMock())

        mock_summary = MagicMock(
            is_match=False, artifacts_written=True, report_summary="Status: FAILED"
        )
        mock_engine.run_from_configs.return_value = MagicMock(summary=mock_summary)

        run(default_args)
        captured = capsys.readouterr()

        assert "Artifacts saved to:" in captured.err
        assert "diffs" in captured.err

    def test_it_omits_artifact_paths_when_no_files_were_written(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure warehouse pushdown failures do not advertise nonexistent artifacts."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")

        mock_diff_config = MagicMock(output_path="/tmp/diffs")
        mock_load.return_value = (mock_diff_config, MagicMock(), MagicMock())

        mock_summary = MagicMock(
            is_match=False, artifacts_written=False, report_summary="Status: FAILED"
        )
        mock_engine.run_from_configs.return_value = MagicMock(summary=mock_summary)

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 1
        assert "Artifacts saved to:" not in captured.err

    def test_it_catches_config_errors_and_returns_exit_code_one_via_stderr(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure validation errors gracefully halt execution and print to standard error."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_load.side_effect = ConfigError("Invalid schema mode")

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 1
        assert "Configuration Error" in captured.err
        assert "Invalid schema mode" in captured.err

    def test_it_catches_connector_errors_and_returns_exit_code_one_via_stderr(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure warehouse routing failures print a named VerideltaError without a traceback."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")
        mock_load.return_value = (MagicMock(), MagicMock(), MagicMock())
        mock_engine.run_from_configs.side_effect = ConnectorError(
            "Cross-dialect warehouse pushdown"
        )

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 1
        assert "ConnectorError" in captured.err
        assert "Cross-dialect warehouse pushdown" in captured.err

    def test_it_catches_unexpected_exceptions_and_returns_exit_code_one_via_stderr(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure unhandled system errors do not crash the runner ungracefully."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_load.side_effect = RuntimeError("Disk full")

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 1
        assert "Unexpected System Error" in captured.err
        assert "Disk full" in captured.err

    def test_it_prints_the_summary_as_json_on_stdout(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure `--json` produces a parseable payload and no formatted report."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")
        mock_load.return_value = (MagicMock(output_path=None), MagicMock(), MagicMock())
        mock_summary = MagicMock(is_match=True, report_summary="Status: PASSED")
        mock_summary.model_dump_json.return_value = '{"is_match": true}'
        mock_engine.run_from_configs.return_value = MagicMock(summary=mock_summary)
        default_args.json = True

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 0
        assert captured.out.strip() == '{"is_match": true}'
        assert "Status: PASSED" not in captured.out
        mock_summary.model_dump_json.assert_called_once_with(indent=2)

    def test_it_keeps_progress_off_of_stdout_when_emitting_json(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure `veridelta run --json | jq` does not have to strip chatter."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")
        mock_load.return_value = (MagicMock(output_path=None), MagicMock(), MagicMock())
        mock_summary = MagicMock(is_match=True, report_summary="Status: PASSED")
        mock_summary.model_dump_json.return_value = "{}"
        mock_engine.run_from_configs.return_value = MagicMock(summary=mock_summary)
        default_args.json = True

        run(default_args)
        captured = capsys.readouterr()

        assert "Loading configuration" in captured.err
        assert "Loading configuration" not in captured.out

    def test_it_writes_an_html_report_when_asked(
        self, mocker: MockerFixture, default_args: argparse.Namespace
    ) -> None:
        """Ensure `--html` is the only thing that triggers a report write."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")
        mock_write = mocker.patch("veridelta.cli.write_html")
        mock_result = MagicMock(summary=MagicMock(is_match=True, report_summary="PASSED"))
        mock_load.return_value = (MagicMock(output_path=None), MagicMock(), MagicMock())
        mock_engine.run_from_configs.return_value = mock_result
        default_args.html = "report.html"

        run(default_args)

        mock_write.assert_called_once_with(mock_result, "report.html", max_rows=1000)

    def test_it_stays_silent_when_asked(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure `--quiet` suppresses progress without hiding the result."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")
        mock_load.return_value = (MagicMock(output_path=None), MagicMock(), MagicMock())
        mock_engine.run_from_configs.return_value = MagicMock(
            summary=MagicMock(is_match=True, report_summary="Status: PASSED")
        )
        default_args.quiet = True

        run(default_args)
        captured = capsys.readouterr()

        assert "Loading configuration" not in captured.err
        assert "Status: PASSED" in captured.out

    def test_it_prints_the_version(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Ensure `-V` reports the package version and does not start a run."""
        from veridelta import __version__

        with pytest.raises(SystemExit) as exc:
            build_parser().parse_args(["--version"])

        assert exc.value.code == 0
        assert __version__ in capsys.readouterr().out

    @pytest.mark.parametrize(
        ("value", "message"),
        [
            pytest.param("-5", "zero or more", id="negative"),
            pytest.param("many", "whole number", id="not-a-number"),
        ],
    )
    def test_it_rejects_an_unusable_html_row_cap(
        self, capsys: pytest.CaptureFixture[str], value: str, message: str
    ) -> None:
        """Ensure a bad `--html-max-rows` is a usage error, not a silently wrong report."""
        with pytest.raises(SystemExit) as exc:
            build_parser().parse_args(["run", "--html", "r.html", "--html-max-rows", value])

        assert exc.value.code == 2
        assert message in capsys.readouterr().err

    def test_it_accepts_a_zero_or_positive_html_row_cap(self) -> None:
        """Ensure a valid cap, zero included, parses to an integer."""
        parser = build_parser()

        assert parser.parse_args(["run", "--html-max-rows", "0"]).html_max_rows == 0
        assert parser.parse_args(["run", "--html-max-rows", "25"]).html_max_rows == 25

    def test_main_parses_arguments_and_delegates_to_run(self, mocker: MockerFixture) -> None:
        """Ensure the main entrypoint correctly routes the run command and exits."""
        mock_run = mocker.patch("veridelta.cli.run", return_value=0)
        mock_exit = mocker.patch("veridelta.cli.sys.exit")
        mocker.patch("veridelta.cli.sys.argv", ["veridelta", "run", "-c", "custom.yaml"])

        main()

        mock_run.assert_called_once()

        args_passed = mock_run.call_args[0][0]
        assert args_passed.command == "run"
        assert args_passed.config == "custom.yaml"

        mock_exit.assert_called_once_with(0)


_CROSSWALK_CONFIG = DiffConfig(
    primary_keys=["id"],
    rules=[
        DiffRule(column_names=["gender"]),
        DiffRule(column_names=["status", "state"]),
        DiffRule(pattern="^fl"),
    ],
)
"""Configuration the stubbed loader returns: one rule per way a rule can govern."""


def _proposal(
    column: str,
    entries: dict[str, tuple[str, int, int]],
    *,
    existing: dict[str, str] | None = None,
    governing_rule_index: int | None = None,
) -> ValueMapProposal:
    """Build a proposal from `{source: (target, rows, agreeing_rows)}` entries."""
    evidence = tuple(
        ValueMapEntry(source_value=source, target_value=target, rows=rows, agreeing_rows=agreeing)
        for source, (target, rows, agreeing) in entries.items()
    )
    return ValueMapProposal(
        column=column,
        value_map={
            **(existing or {}),
            **{entry.source_value: entry.target_value for entry in evidence},
        },
        entries=evidence,
        governing_rule_index=governing_rule_index,
    )


@pytest.mark.unit
@pytest.mark.fast
class TestCrosswalkCommand:
    """Validate `veridelta crosswalk`, which prints proposed value_map rules."""

    @pytest.fixture
    def crosswalk_args(self) -> argparse.Namespace:
        """Provide the namespace `crosswalk` receives with every default."""
        return argparse.Namespace(
            config="dummy.yaml",
            json=False,
            quiet=False,
            min_confidence=0.95,
            min_support=5,
            sample_fraction=1.0,
        )

    @pytest.fixture
    def propose(self, mocker: MockerFixture) -> MagicMock:
        """Stub configuration loading and return the patched proposal entry point."""
        mocker.patch(
            "veridelta.cli.load_config", return_value=(_CROSSWALK_CONFIG, "source", "target")
        )
        engine = mocker.patch("veridelta.cli.DiffEngine")
        proposals: MagicMock = engine.propose_value_maps_from_configs
        return proposals

    def test_it_prints_rules_a_configuration_can_hold(
        self,
        propose: MagicMock,
        crosswalk_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure stdout is YAML whose rules load back, codes like Y and 1 still text."""
        propose.return_value = [
            _proposal("gender", {"M": ("Male", 1000, 999)}),
            _proposal("flag", {"Y": ("1", 5, 5)}),
        ]

        exit_code = crosswalk(crosswalk_args)
        printed = yaml.safe_load(capsys.readouterr().out)
        config = DiffConfig.model_validate({"primary_keys": ["id"], **printed})

        assert exit_code == 0
        assert [(rule.column_names, rule.value_map) for rule in config.rules] == [
            (["gender"], {"M": "Male"}),
            (["flag"], {"Y": "1"}),
        ]

    def test_it_writes_the_evidence_to_stderr_only(
        self,
        propose: MagicMock,
        crosswalk_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure the counts behind each entry never end up in the pasted YAML."""
        propose.return_value = [_proposal("gender", {"M": ("Male", 1000, 999)})]

        crosswalk(crosswalk_args)
        captured = capsys.readouterr()

        assert "gender: 1 new value_map entry" in captured.err
        assert "'M' -> 'Male': 999 of 1,000 rows (99.9%)" in captured.err
        assert "rows" not in captured.out

    def test_it_prints_proposals_as_json(
        self,
        propose: MagicMock,
        crosswalk_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure `--json` hands scripts the evidence as well as the map."""
        propose.return_value = [_proposal("gender", {"M": ("Male", 1000, 999)})]
        crosswalk_args.json = True

        exit_code = crosswalk(crosswalk_args)
        (proposal,) = json.loads(capsys.readouterr().out)

        assert exit_code == 0
        assert proposal["value_map"] == {"M": "Male"}
        assert proposal["entries"][0]["confidence"] == 0.999

    @pytest.mark.parametrize(("as_json", "printed"), [(False, ""), (True, "[]\n")])
    def test_it_succeeds_when_nothing_is_proposed(
        self,
        propose: MagicMock,
        crosswalk_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
        as_json: bool,
        printed: str,
    ) -> None:
        """Ensure finding nothing to propose is a result, not a failure."""
        propose.return_value = []
        crosswalk_args.json = as_json

        exit_code = crosswalk(crosswalk_args)
        captured = capsys.readouterr()

        assert exit_code == 0
        assert captured.out == printed
        assert ("No value_map entries met the thresholds." in captured.err) is not as_json

    @pytest.mark.parametrize(
        ("index", "column", "advice"),
        [
            pytest.param(0, "gender", "Add these entries to its value_map", id="own-rule"),
            pytest.param(
                1, "status", "move 'status' into a rule of its own", id="rule-lists-others"
            ),
            pytest.param(2, "flag", "add a rule listing 'flag' by name", id="pattern-rule"),
        ],
    )
    def test_it_says_where_entries_go_when_a_rule_already_governs_the_column(
        self,
        propose: MagicMock,
        crosswalk_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
        index: int,
        column: str,
        advice: str,
    ) -> None:
        """Ensure the note survives `--quiet` and never sends a map into a shared rule."""
        propose.return_value = [
            _proposal(
                column,
                {"M": ("Male", 5, 5)},
                existing={"F": "Female"},
                governing_rule_index=index,
            )
        ]
        crosswalk_args.quiet = True

        crosswalk(crosswalk_args)
        captured = capsys.readouterr()

        assert f"rules[{index}] already governs '{column}'" in captured.err
        assert advice in captured.err
        assert "Loading configuration" not in captured.err
        assert yaml.safe_load(captured.out)["rules"][0]["value_map"] == {
            "F": "Female",
            "M": "Male",
        }

    def test_it_passes_the_thresholds_to_the_engine(
        self, propose: MagicMock, crosswalk_args: argparse.Namespace
    ) -> None:
        """Ensure every flag reaches the proposal entry point unchanged."""
        propose.return_value = []
        crosswalk_args.min_confidence = 0.9
        crosswalk_args.min_support = 3
        crosswalk_args.sample_fraction = 0.5

        crosswalk(crosswalk_args)

        propose.assert_called_once_with(
            _CROSSWALK_CONFIG,
            "source",
            "target",
            min_confidence=0.9,
            min_support=3,
            sample_fraction=0.5,
        )

    @pytest.mark.parametrize(
        ("failure", "header"),
        [
            pytest.param(ConfigError("bad rule"), "Configuration Error", id="config"),
            pytest.param(ConnectorError("warehouse"), "ConnectorError", id="connector"),
            pytest.param(RuntimeError("disk full"), "Unexpected System Error", id="unexpected"),
        ],
    )
    def test_it_reports_failures_the_way_run_does(
        self,
        propose: MagicMock,
        crosswalk_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
        failure: Exception,
        header: str,
    ) -> None:
        """Ensure a failure exits 1 with the same explanation `run` would give."""
        propose.side_effect = failure

        exit_code = crosswalk(crosswalk_args)
        captured = capsys.readouterr()

        assert exit_code == 1
        assert header in captured.err
        assert str(failure) in captured.err
        assert captured.out == ""

    def test_it_parses_the_documented_defaults(self) -> None:
        """Ensure a bare `crosswalk` uses the engine's thresholds and the default config."""
        args = build_parser().parse_args(["crosswalk"])

        assert (args.config, args.min_confidence, args.min_support, args.sample_fraction) == (
            "veridelta.yaml",
            0.95,
            5,
            1.0,
        )

    @pytest.mark.parametrize(
        ("flag", "value", "message"),
        [
            pytest.param("--min-confidence", "0.5", "above 0.5", id="confidence-at-half"),
            pytest.param("--min-confidence", "1.5", "at most 1", id="confidence-above-one"),
            pytest.param("--min-confidence", "nan", "above 0.5", id="confidence-nan"),
            pytest.param("--min-confidence", "most", "expected a number", id="confidence-text"),
            pytest.param("--min-support", "0", "at least 1", id="support-zero"),
            pytest.param("--min-support", "2.5", "whole number", id="support-fraction"),
            pytest.param("--sample-fraction", "0", "above 0", id="fraction-zero"),
            pytest.param("--sample-fraction", "1.5", "at most 1", id="fraction-above-one"),
        ],
    )
    def test_it_rejects_unusable_thresholds(
        self, capsys: pytest.CaptureFixture[str], flag: str, value: str, message: str
    ) -> None:
        """Ensure a bad threshold is a usage error before any data is read."""
        with pytest.raises(SystemExit) as exc:
            build_parser().parse_args(["crosswalk", flag, value])

        assert exc.value.code == 2
        assert message in capsys.readouterr().err

    def test_main_dispatches_to_crosswalk(self, mocker: MockerFixture) -> None:
        """Ensure the entry point routes the new subcommand and exits with its code."""
        mock_crosswalk = mocker.patch("veridelta.cli.crosswalk", return_value=0)
        mock_exit = mocker.patch("veridelta.cli.sys.exit")
        mocker.patch("veridelta.cli.sys.argv", ["veridelta", "crosswalk", "-c", "custom.yaml"])

        main()

        assert mock_crosswalk.call_args[0][0].config == "custom.yaml"
        mock_exit.assert_called_once_with(0)
