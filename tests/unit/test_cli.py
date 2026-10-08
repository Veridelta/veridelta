# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Veridelta command-line interface."""

import argparse
import json
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock
from urllib.parse import quote

import polars as pl
import pytest
import yaml
from polars.exceptions import PanicException
from pytest_mock import MockerFixture

from tests.otlp_collector import running_collector
from veridelta.cli import build_parser, crosswalk, main, mcp, run, validate
from veridelta.config import config_json_schema
from veridelta.exceptions import ConfigError, ConnectorError, VerideltaError
from veridelta.mcp_server import Settings
from veridelta.models import DiffConfig, DiffRule, ValueMapEntry, ValueMapProposal

pytestmark = [pytest.mark.unit, pytest.mark.fast]


class TestCommandLineInterface:
    """Validate CLI argument parsing, workflow execution, and exit codes."""

    @pytest.fixture
    def default_args(self) -> argparse.Namespace:
        """Provide a default argparse namespace for testing the run function."""
        return argparse.Namespace(
            config="dummy.yaml",
            files=[],
            key=None,
            json=False,
            quiet=False,
            baseline=None,
            save_baseline=None,
            html=None,
            html_max_rows=1000,
            markdown=None,
            markdown_max_rows=0,
            otel=None,
            otel_send=False,
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
            mock_diff_config, mock_source, mock_target, baseline=None
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

    @pytest.mark.parametrize(
        ("failing_call", "failure", "header"),
        [
            pytest.param(
                "veridelta.cli.load_config",
                ConfigError("Invalid schema mode"),
                "Configuration Error",
                id="config",
            ),
            # Warehouse routing failures print a named `VerideltaError` without a traceback.
            pytest.param(
                "veridelta.cli.DiffEngine.run_from_configs",
                ConnectorError("Cross-dialect warehouse pushdown"),
                "ConnectorError",
                id="connector",
            ),
            pytest.param(
                "veridelta.cli.load_config",
                RuntimeError("Disk full"),
                "Unexpected System Error",
                id="unexpected",
            ),
        ],
    )
    def test_it_catches_errors_and_returns_exit_code_three_via_stderr(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
        failing_call: str,
        failure: Exception,
        header: str,
    ) -> None:
        """Ensure a failure exits 3, apart from drift, and explains itself on standard error."""
        mocker.patch(
            "veridelta.cli.load_config", return_value=(MagicMock(), MagicMock(), MagicMock())
        )
        mocker.patch(failing_call, side_effect=failure)

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 3
        assert header in captured.err
        assert str(failure) in captured.err
        assert captured.out == ""

    @pytest.mark.parametrize(
        ("failing_call", "failure", "header"),
        [
            pytest.param(
                "veridelta.cli.load_config",
                ConfigError("Invalid schema mode"),
                "Configuration Error",
                id="config",
            ),
            pytest.param(
                "veridelta.cli.DiffEngine.run_from_configs",
                ConnectorError("Cross-dialect warehouse pushdown"),
                "ConnectorError",
                id="connector",
            ),
            pytest.param(
                "veridelta.cli.load_config",
                RuntimeError("Disk full"),
                "Unexpected System Error",
                id="unexpected",
            ),
        ],
    )
    def test_it_prints_a_failure_as_one_json_object_with_json(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
        failing_call: str,
        failure: Exception,
        header: str,
    ) -> None:
        """Ensure `--json` keeps stdout parseable when a run fails, and stderr still explains."""
        mocker.patch(
            "veridelta.cli.load_config", return_value=(MagicMock(), MagicMock(), MagicMock())
        )
        mocker.patch(failing_call, side_effect=failure)
        default_args.json = True

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 3
        assert json.loads(captured.out) == {
            "error": {"type": type(failure).__name__, "message": str(failure)}
        }
        assert header in captured.err

    @pytest.mark.parametrize("flag", ["html", "markdown", "otel"])
    def test_it_prints_only_the_error_when_a_file_fails_after_the_comparison(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
        flag: str,
    ) -> None:
        """Ensure a file that cannot be written leaves one JSON object on stdout, not two.

        The comparison has finished by then, so `--json` holds its summary back
        until every file is written.
        """
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")
        mock_load.return_value = (MagicMock(output_path=None), MagicMock(), MagicMock())
        mock_summary = MagicMock(is_match=False, report_summary="Status: FAILED")
        mock_summary.model_dump_json.return_value = '{"is_match": false}'
        mock_engine.run_from_configs.return_value = MagicMock(summary=mock_summary)
        writers = {"html": "write_html", "markdown": "write_markdown", "otel": "write_otlp_metrics"}
        mocker.patch(f"veridelta.cli.{writers[flag]}", side_effect=OSError("No space left"))
        setattr(default_args, flag, "out/file")
        default_args.json = True

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 3
        assert json.loads(captured.out) == {
            "error": {"type": "OSError", "message": "No space left"}
        }

    def test_it_prints_the_summary_alone_after_writing_its_files(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure drift with every file written leaves exactly the summary on stdout."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")
        mock_load.return_value = (MagicMock(output_path=None), MagicMock(), MagicMock())
        mock_summary = MagicMock(is_match=False, report_summary="Status: FAILED")
        mock_summary.model_dump_json.return_value = '{"is_match": false}'
        mock_engine.run_from_configs.return_value = MagicMock(summary=mock_summary)
        mocker.patch("veridelta.cli.write_html", return_value=Path("report.html"))
        mocker.patch("veridelta.cli.write_markdown", return_value=Path("summary.md"))
        default_args.json = True
        default_args.html = "report.html"
        default_args.markdown = "summary.md"

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 1
        assert captured.out == '{"is_match": false}\n'

    def test_it_prints_the_summary_as_json_on_stdout(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure `--json` prints only a parseable payload on stdout and keeps progress on stderr."""
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
        assert "Loading configuration" in captured.err
        assert "Loading configuration" not in captured.out
        # The product compares under declared rules; "semantic diff" names nothing it does.
        assert "Comparing...\n" in captured.err
        mock_summary.model_dump_json.assert_called_once_with(indent=2)

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

    def test_it_writes_a_markdown_summary_when_asked(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure `--markdown` writes the summary file and reports where, on stderr only."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")
        mock_write = mocker.patch("veridelta.cli.write_markdown", return_value=Path("summary.md"))
        mock_result = MagicMock(summary=MagicMock(is_match=True, report_summary="PASSED"))
        mock_load.return_value = (MagicMock(output_path=None), MagicMock(), MagicMock())
        mock_engine.run_from_configs.return_value = mock_result
        default_args.markdown = "summary.md"
        default_args.markdown_max_rows = 5

        run(default_args)
        captured = capsys.readouterr()

        mock_write.assert_called_once_with(mock_result, "summary.md", max_rows=5)
        assert "Markdown summary saved to" in captured.err
        assert "Markdown summary saved to" not in captured.out

    @pytest.mark.parametrize("is_match", [True, False])
    def test_it_writes_otel_metrics_when_asked(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
        is_match: bool,
    ) -> None:
        """Ensure `--otel` writes the metrics for a match or drift, and says where on stderr."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_engine = mocker.patch("veridelta.cli.DiffEngine")
        mock_write = mocker.patch(
            "veridelta.cli.write_otlp_metrics", return_value=Path("otel-metrics.json")
        )
        mock_result = MagicMock(summary=MagicMock(is_match=is_match, report_summary="DONE"))
        source, target = MagicMock(), MagicMock()
        mock_load.return_value = (MagicMock(output_path=None), source, target)
        mock_engine.run_from_configs.return_value = mock_result
        mocker.patch("veridelta.cli.time.time_ns", return_value=1_790_000_000_000_000_000)
        default_args.otel = "otel-metrics.json"

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == (0 if is_match else 1)
        mock_write.assert_called_once_with(
            mock_result,
            "otel-metrics.json",
            config_path="dummy.yaml",
            source=source,
            target=target,
            time_unix_nano=1_790_000_000_000_000_000,
        )
        assert "OpenTelemetry metrics saved to" in captured.err
        assert "OpenTelemetry metrics saved to" not in captured.out

    def test_it_writes_no_otel_metrics_when_the_run_fails(
        self, mocker: MockerFixture, default_args: argparse.Namespace
    ) -> None:
        """Ensure a run that never finished leaves no metrics a collector could mistake for one."""
        mock_load = mocker.patch("veridelta.cli.load_config")
        mock_load.side_effect = ConfigError("Invalid schema mode")
        mock_write = mocker.patch("veridelta.cli.write_otlp_metrics")
        default_args.otel = "otel-metrics.json"

        exit_code = run(default_args)

        assert exit_code == 3
        mock_write.assert_not_called()

    @pytest.mark.parametrize(
        ("flag", "path"),
        [
            pytest.param("markdown", "out/summary.md", id="markdown"),
            pytest.param("otel", "out/metrics.json", id="otel"),
        ],
    )
    def test_it_parses_an_output_file_flag(self, flag: str, path: str) -> None:
        """Ensure `--markdown` and `--otel` take a path and default to writing nothing."""
        parser = build_parser()

        assert getattr(parser.parse_args(["run"]), flag) is None
        assert getattr(parser.parse_args(["run", f"--{flag}", path]), flag) == path

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

    @pytest.mark.parametrize("flag", ["--html-max-rows", "--markdown-max-rows"])
    @pytest.mark.parametrize(
        ("value", "message"),
        [
            pytest.param("-5", "zero or more", id="negative"),
            pytest.param("many", "whole number", id="not-a-number"),
        ],
    )
    def test_it_rejects_an_unusable_row_cap(
        self, capsys: pytest.CaptureFixture[str], flag: str, value: str, message: str
    ) -> None:
        """Ensure a bad row cap is a usage error, not a silently wrong report."""
        with pytest.raises(SystemExit) as exc:
            build_parser().parse_args(["run", flag, value])

        assert exc.value.code == 2
        assert message in capsys.readouterr().err

    @pytest.mark.parametrize(
        ("flag", "default"),
        [
            pytest.param("html_max_rows", 1000, id="html"),
            pytest.param("markdown_max_rows", 0, id="markdown"),
        ],
    )
    def test_it_accepts_a_zero_or_positive_row_cap(self, flag: str, default: int) -> None:
        """Ensure a valid cap, zero included, parses to an integer, and values stay off by default."""
        parser = build_parser()
        option = "--" + flag.replace("_", "-")

        assert getattr(parser.parse_args(["run"]), flag) == default
        assert getattr(parser.parse_args(["run", option, "0"]), flag) == 0
        assert getattr(parser.parse_args(["run", option, "25"]), flag) == 25

    def test_main_exits_3_when_polars_panics(
        self, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a panic, which no `except Exception` catches, is an error and not drift.

        Python exits 1 on an uncaught exception, the code a run gives for drift.
        """
        mocker.patch("veridelta.cli.load_config", return_value=(MagicMock(), None, None))
        mocker.patch(
            "veridelta.cli.DiffEngine.run_from_configs",
            side_effect=PanicException("not yet implemented: Writing BinaryView to JSON"),
        )
        mocker.patch("veridelta.cli.sys.argv", ["veridelta", "run", "-c", "a.yaml", "--json"])

        with pytest.raises(SystemExit) as stopped:
            main()

        captured = capsys.readouterr()
        assert stopped.value.code == 3
        assert "Unexpected System Error\nPanicException: not yet implemented" in captured.err
        assert json.loads(captured.out)["error"]["type"] == "PanicException"

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


class TestRunOnTwoFiles:
    """Validate `veridelta run SOURCE TARGET --key COLUMN`, which needs no configuration file."""

    @pytest.fixture
    def files(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        """Write a source and two targets, one equal and one drifting, and work beside them."""
        monkeypatch.chdir(tmp_path)
        source = pl.DataFrame({"id": [1, 2, 3], "status": ["open", "shipped", "open"]})
        source.write_csv(tmp_path / "legacy.csv")
        source.write_parquet(tmp_path / "same.parquet")
        source.with_columns(pl.lit("open").alias("status")).write_csv(tmp_path / "modern.csv")
        return tmp_path

    @staticmethod
    def _main(mocker: MockerFixture, *arguments: str) -> object:
        """Run `veridelta run` with the arguments given, and return its exit code."""
        mocker.patch("veridelta.cli.sys.argv", ["veridelta", "run", *arguments])
        with pytest.raises(SystemExit) as stopped:
            main()
        return stopped.value.code

    def test_it_parses_two_files_and_their_keys(self) -> None:
        """Ensure the files and every repeated --key reach the command, and -c stays unset."""
        args = build_parser().parse_args(["run", "a.csv", "b.csv", "-k", "id", "--key", "day"])

        assert args.files == ["a.csv", "b.csv"]
        assert args.key == ["id", "day"]
        assert args.config is None

    def test_it_reports_a_match_without_a_configuration_file(
        self, files: Path, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure two equal files exit 0, each read in the format its suffix names."""
        code = self._main(mocker, "legacy.csv", "same.parquet", "--key", "id")
        captured = capsys.readouterr()

        assert code == 0
        assert "PASSED" in captured.out
        assert "Loading configuration" not in captured.err

    def test_it_reports_drift_as_a_configured_run_does(
        self, files: Path, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure the summary is the one a file with the same keys and paths gives."""
        (files / "veridelta.yaml").write_text(
            "primary_keys: [id]\nsource:\n  path: legacy.csv\ntarget:\n  path: modern.csv\n"
        )
        configured = self._main(mocker, "--json")
        expected = json.loads(capsys.readouterr().out)

        code = self._main(mocker, "legacy.csv", "modern.csv", "--key", "id", "--json")

        assert configured == code == 1
        assert json.loads(capsys.readouterr().out) == expected

    def test_it_names_no_configuration_file_in_the_metrics(
        self, files: Path, mocker: MockerFixture
    ) -> None:
        """Ensure an OTLP export of a run on two files carries no configuration path."""
        self._main(mocker, "legacy.csv", "modern.csv", "--key", "id", "--otel", "otel.json")

        export = json.loads((files / "otel.json").read_text())
        attributes = export["resourceMetrics"][0]["resource"]["attributes"]

        assert "veridelta.config.path" not in {attribute["key"] for attribute in attributes}

    def test_it_says_the_arguments_hold_the_mistake(
        self, files: Path, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a key neither file has is blamed on the arguments, not on a configuration file."""
        code = self._main(mocker, "legacy.csv", "modern.csv", "--key", "order_id")
        captured = capsys.readouterr()

        assert code == 3
        assert "'order_id'" in captured.err
        assert "problem with the FILE arguments and --key" in captured.err
        assert "configuration file" not in captured.err

    @pytest.mark.parametrize(
        ("arguments", "problem"),
        [
            (
                ["legacy.csv", "modern.csv"],
                "name the primary key the two files share with --key, such as --key id",
            ),
            (
                ["--key", "id"],
                "--key names the key of two FILE arguments; a configuration file sets primary_keys",
            ),
            (
                ["legacy.csv", "--key", "id"],
                "name two files, the source and then the target, not 1",
            ),
            (
                ["legacy.csv", "modern.csv", "same.parquet", "--key", "id"],
                "name two files, the source and then the target, not 3",
            ),
            (
                ["legacy.csv", "modern.csv", "--key", "id", "-c", "veridelta.yaml"],
                "compare two FILE arguments or the configuration file -c names, not both",
            ),
        ],
    )
    def test_it_refuses_arguments_that_do_not_fit_together(
        self,
        files: Path,
        mocker: MockerFixture,
        capsys: pytest.CaptureFixture[str],
        arguments: list[str],
        problem: str,
    ) -> None:
        """Ensure a wrong mix of files, --key, and -c exits 2, as any invalid argument does."""
        load = mocker.patch("veridelta.cli.load_config")

        code = self._main(mocker, *arguments)

        assert code == 2
        assert capsys.readouterr().err == f"veridelta run: error: {problem}\n"
        load.assert_not_called()

    def test_it_still_reads_the_default_configuration_file(
        self, files: Path, mocker: MockerFixture
    ) -> None:
        """Ensure `veridelta run` with neither files nor -c reads veridelta.yaml, as before."""
        load = mocker.patch("veridelta.cli.load_config", side_effect=ConfigError("stop here"))

        code = self._main(mocker)

        assert code == 3
        load.assert_called_once_with("veridelta.yaml")


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
        """Ensure a failure exits 3 with the same explanation `run` would give."""
        propose.side_effect = failure

        exit_code = crosswalk(crosswalk_args)
        captured = capsys.readouterr()

        assert exit_code == 3
        assert header in captured.err
        assert str(failure) in captured.err
        assert captured.out == ""

    def test_it_prints_a_failure_as_one_json_object_with_json(
        self,
        propose: MagicMock,
        crosswalk_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure `--json` keeps stdout parseable when proposing fails."""
        propose.side_effect = ConnectorError("Warehouse unreachable")
        crosswalk_args.json = True

        exit_code = crosswalk(crosswalk_args)

        assert exit_code == 3
        assert json.loads(capsys.readouterr().out) == {
            "error": {"type": "ConnectorError", "message": "Warehouse unreachable"}
        }

    def test_it_parses_the_documented_defaults(self) -> None:
        """Ensure a bare `crosswalk` uses the engine's thresholds and the default config."""
        args = build_parser().parse_args(["crosswalk"])

        assert (args.config, args.min_confidence, args.min_support, args.sample_fraction) == (
            "veridelta.yaml",
            0.95,
            5,
            1.0,
        )

    def test_it_parses_thresholds_given_on_the_command_line(self) -> None:
        """Ensure valid values pass through each parser, the bounds themselves included."""
        args = build_parser().parse_args(
            [
                "crosswalk",
                "--min-confidence",
                "1",
                "--min-support",
                "1",
                "--sample-fraction",
                "0.25",
            ]
        )

        assert (args.min_confidence, args.min_support, args.sample_fraction) == (1.0, 1, 0.25)

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


class TestSchemaCommand:
    """Validate `veridelta schema`."""

    def test_main_dispatches_to_schema(
        self, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `veridelta schema` prints the JSON Schema on stdout and exits 0."""
        mock_exit = mocker.patch("veridelta.cli.sys.exit")
        mocker.patch("veridelta.cli.sys.argv", ["veridelta", "schema"])

        main()

        assert json.loads(capsys.readouterr().out) == config_json_schema()
        mock_exit.assert_called_once_with(0)


def _serve_args(
    roots: list[Path] | None,
    *,
    allow_row_values: bool = False,
    max_rows: int = 50,
    allow_queries: bool = False,
) -> argparse.Namespace:
    """Build the arguments `veridelta mcp` parses, with row values and queries off by default."""
    return argparse.Namespace(
        root=roots,
        allow_row_values=allow_row_values,
        max_rows=max_rows,
        allow_queries=allow_queries,
    )


class TestMCPCommand:
    """Validate `veridelta mcp`, which serves the checks to an agent over stdio."""

    def test_it_takes_each_root_resolved(self, tmp_path: Path) -> None:
        """Ensure `--root` repeats, resolves, and is absent when not given."""
        (tmp_path / "data").mkdir()

        args = build_parser().parse_args(
            ["mcp", "--root", str(tmp_path / "data" / ".."), "--root", str(tmp_path / "data")]
        )

        assert args.root == [tmp_path.resolve(), (tmp_path / "data").resolve()]
        assert build_parser().parse_args(["mcp"]).root is None

    def test_it_refuses_a_root_that_is_not_a_directory(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a missing folder or a file is an argument error, before anything starts."""
        (tmp_path / "file.txt").write_text("not a folder")

        for root in (tmp_path / "missing", tmp_path / "file.txt"):
            with pytest.raises(SystemExit) as stopped:
                build_parser().parse_args(["mcp", "--root", str(root)])
            assert stopped.value.code == 2
            assert f"not a directory: {str(root)!r}" in capsys.readouterr().err

    def test_it_takes_the_row_value_flags(self) -> None:
        """Ensure row values are off and capped at 50 unless the flags say otherwise."""
        default = build_parser().parse_args(["mcp"])
        allowed = build_parser().parse_args(["mcp", "--allow-row-values", "--max-rows", "5"])

        assert (default.allow_row_values, default.max_rows) == (False, 50)
        assert (allowed.allow_row_values, allowed.max_rows) == (True, 5)

    def test_it_takes_the_query_flag(self) -> None:
        """Ensure a side's query stays off unless `--allow-queries` is given."""
        assert build_parser().parse_args(["mcp"]).allow_queries is False
        assert build_parser().parse_args(["mcp", "--allow-queries"]).allow_queries is True

    @pytest.mark.parametrize(
        ("value", "message"),
        [("0", "must be at least 1, got 0"), ("many", "expected a whole number, got 'many'")],
    )
    def test_it_refuses_a_row_cap_below_one(
        self, value: str, message: str, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a cap that lets no row through is an argument error, before anything starts."""
        with pytest.raises(SystemExit) as stopped:
            build_parser().parse_args(["mcp", "--max-rows", value])

        assert stopped.value.code == 2
        assert message in capsys.readouterr().err

    def test_it_passes_the_row_value_flags_to_the_server(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure the server is built with the flags, which no tool call can change."""
        monkeypatch.chdir(tmp_path)
        serve = mocker.patch("veridelta.cli.serve")

        assert mcp(_serve_args(None, allow_row_values=True, max_rows=5, allow_queries=True)) == 0
        serve.assert_called_once_with(
            Settings((tmp_path,), allow_row_values=True, max_rows=5, allow_queries=True)
        )

    def test_it_serves_from_the_first_root(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure the server gets every root and runs in the first, with stdout left alone."""
        first, second = tmp_path / "first", tmp_path / "second"
        first.mkdir()
        second.mkdir()
        monkeypatch.chdir(second)
        serve = mocker.patch("veridelta.cli.serve")

        exit_code = mcp(_serve_args([first, second]))

        assert exit_code == 0
        serve.assert_called_once_with(Settings((first, second)))
        assert Path.cwd() == first.resolve()
        assert capsys.readouterr().out == ""

    def test_it_serves_the_current_directory_by_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a server started with no `--root` reads only where it was started."""
        monkeypatch.chdir(tmp_path)
        serve = mocker.patch("veridelta.cli.serve")

        assert mcp(_serve_args(None)) == 0
        serve.assert_called_once_with(Settings((tmp_path,)))

    def test_it_explains_a_server_that_cannot_start(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure a missing extra exits 3 with the install hint on stderr and nothing on stdout."""
        monkeypatch.chdir(tmp_path)
        mocker.patch(
            "veridelta.cli.serve",
            side_effect=VerideltaError(
                "MCP extra is not installed. Install it with: uv add 'veridelta[mcp]'"
            ),
        )

        exit_code = mcp(_serve_args(None))
        captured = capsys.readouterr()

        assert exit_code == 3
        assert "uv add 'veridelta[mcp]'" in captured.err
        assert captured.out == ""

    def test_it_stops_quietly_on_ctrl_c(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure Ctrl-C, how a person stops a server started by hand, exits 0 with no trace."""
        monkeypatch.chdir(tmp_path)
        mocker.patch("veridelta.cli.serve", side_effect=KeyboardInterrupt)

        exit_code = mcp(_serve_args(None))

        assert exit_code == 0
        assert capsys.readouterr() == ("", "")

    def test_main_dispatches_to_mcp(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure `veridelta mcp` reaches the server and exits 0 when the host disconnects."""
        monkeypatch.chdir(tmp_path)
        serve = mocker.patch("veridelta.cli.serve")
        mock_exit = mocker.patch("veridelta.cli.sys.exit")
        mocker.patch("veridelta.cli.sys.argv", ["veridelta", "mcp", "--root", str(tmp_path)])

        main()

        serve.assert_called_once_with(Settings((tmp_path,)))
        mock_exit.assert_called_once_with(0)


_SNOWFLAKE_SIDE = (
    "  type: snowflake\n  account: xy12345\n  user: analyst\n  warehouse: COMPUTE_WH\n"
    "  database: ANALYTICS\n  schema_name: PUBLIC\n"
)


class TestValidateCommand:
    """Validate `veridelta validate`, which checks a configuration without reading data."""

    @staticmethod
    def _args(path: Path, **overrides: object) -> argparse.Namespace:
        """Build the namespace `validate` receives, with every default."""
        fields: dict[str, object] = {
            "config": str(path),
            "schemas": False,
            "allow_missing_env": False,
            "json": False,
            "quiet": False,
            **overrides,
        }
        return argparse.Namespace(**fields)

    @staticmethod
    def _write(tmp_path: Path, text: str) -> Path:
        """Write a configuration file."""
        path = tmp_path / "veridelta.yaml"
        path.write_text(text)
        return path

    def test_it_passes_a_configuration_that_will_run(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a clean file exits 0, prints no findings, and says so on stderr."""
        path = self._write(
            tmp_path, "source:\n  path: a.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n"
        )

        exit_code = validate(self._args(path))
        captured = capsys.readouterr()

        assert exit_code == 0
        assert captured.out == ""
        assert captured.err == f"{path}: valid.\n"

    def test_it_fails_a_configuration_that_does_not_load(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a load failure is one error finding on stdout, and exit 1."""
        path = self._write(
            tmp_path,
            "source:\n  path: a.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n"
            "rules:\n  - column_names: [amount]\n    absolute_tolerence: 0.1\n",
        )

        exit_code = validate(self._args(path))
        captured = capsys.readouterr()

        assert exit_code == 1
        assert captured.out.startswith("error: Configuration Validation Failed:")
        assert "absolute_tolerence" in captured.out
        assert captured.err == f"{path}: 1 error, 0 warnings.\n"

    def test_it_reports_findings_as_json(
        self, tmp_path: Path, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `--json` prints one object a CI step can read."""
        mocker.patch("veridelta.connectors.warehouse.snowflake_connector", object())
        path = self._write(
            tmp_path,
            "source:\n" + _SNOWFLAKE_SIDE + "  table: SRC\ntarget:\n  path: b.csv\n"
            "primary_keys: [id]\n",
        )

        exit_code = validate(self._args(path, json=True, quiet=True))
        captured = capsys.readouterr()

        assert exit_code == 1
        assert json.loads(captured.out) == {
            "config": str(path),
            "valid": False,
            "errors": ["Mixed file/lakehouse/database and warehouse backends are unsupported."],
            "warnings": [],
        }
        assert captured.err == ""

    def test_it_passes_with_warnings(
        self, tmp_path: Path, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure warnings print but do not fail the check."""
        mocker.patch("veridelta.connectors.warehouse.snowflake_connector", object())
        path = self._write(
            tmp_path,
            "source:\n" + _SNOWFLAKE_SIDE + "  table: SRC\n"
            "target:\n" + _SNOWFLAKE_SIDE + "  table: TGT\n"
            "primary_keys: [ID]\nnormalize_column_names: true\n",
        )

        exit_code = validate(self._args(path))
        captured = capsys.readouterr()

        assert exit_code == 0
        assert captured.out.startswith("warning: normalize_column_names is on.")
        assert captured.err == f"{path}: valid, with 1 warning.\n"

    def test_it_needs_every_variable_unless_told_otherwise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure an unset variable fails by default, and is a warning with the flag."""
        monkeypatch.delenv("VD_VALIDATE_TABLE", raising=False)
        path = self._write(
            tmp_path,
            "source:\n  path: ${VD_VALIDATE_TABLE}.csv\ntarget:\n  path: b.csv\n"
            "primary_keys: [id]\n",
        )

        strict = validate(self._args(path))
        strict_out = capsys.readouterr().out
        lenient = validate(self._args(path, allow_missing_env=True))
        lenient_out = capsys.readouterr().out

        assert strict == 1
        assert "Environment variable 'VD_VALIDATE_TABLE' is not set" in strict_out
        assert lenient == 0
        assert lenient_out == (
            "warning: Environment variable 'VD_VALIDATE_TABLE' is not set, so its "
            "references were checked as the text 'VD_VALIDATE_TABLE'.\n"
        )

    def test_it_checks_rules_against_stored_columns_with_schemas(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `--schemas` reads the files' columns, so a rule that cannot fit fails."""
        (tmp_path / "a.csv").write_text("id,amount\n1,10\n")
        (tmp_path / "b.csv").write_text("id,amount\n1,10\n")
        path = self._write(
            tmp_path,
            f"source:\n  path: {tmp_path / 'a.csv'}\ntarget:\n  path: {tmp_path / 'b.csv'}\n"
            "primary_keys: [id]\nrules:\n  - column_names: [amount]\n    null_values: ['N/A']\n",
        )

        offline = validate(self._args(path))
        capsys.readouterr()
        live = validate(self._args(path, schemas=True))

        assert offline == 0
        assert live == 1
        assert "Column 'amount' has type Int64, which cannot hold" in capsys.readouterr().out

    def test_quiet_keeps_the_findings(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `-q` silences only the verdict line on stderr."""
        path = self._write(tmp_path, "source: {}\n")

        exit_code = validate(self._args(path, quiet=True))
        captured = capsys.readouterr()

        assert exit_code == 1
        assert captured.out.startswith("error: Configuration must contain both")
        assert captured.err == ""

    def test_it_reports_an_unexpected_failure(
        self, tmp_path: Path, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a bug in a check exits 3 with the usual report, not a traceback."""
        mocker.patch("veridelta.cli.DiffEngine.check_configs", side_effect=RuntimeError("boom"))
        path = self._write(
            tmp_path, "source:\n  path: a.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n"
        )

        exit_code = validate(self._args(path))
        captured = capsys.readouterr()

        assert exit_code == 3
        assert "Unexpected System Error\nRuntimeError: boom" in captured.err
        assert captured.out == ""

    def test_it_prints_a_check_that_cannot_finish_as_an_error_object_with_json(
        self, tmp_path: Path, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `--json` prints the error object, not a report, when the check cannot finish.

        A finding is part of a finished check, at exit 1, and stays in the report.
        """
        mocker.patch(
            "veridelta.cli.DiffEngine.check_configs",
            side_effect=ConnectorError("Warehouse unreachable"),
        )
        path = self._write(
            tmp_path, "source:\n  path: a.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n"
        )

        exit_code = validate(self._args(path, json=True, schemas=True))

        assert exit_code == 3
        assert json.loads(capsys.readouterr().out) == {
            "error": {"type": "ConnectorError", "message": "Warehouse unreachable"}
        }

    def test_main_dispatches_to_validate(self, mocker: MockerFixture) -> None:
        """Ensure the subcommand and its flags reach the handler."""
        handler = mocker.patch("veridelta.cli.validate", return_value=0)
        mock_exit = mocker.patch("veridelta.cli.sys.exit")
        mocker.patch(
            "veridelta.cli.sys.argv",
            [
                "veridelta",
                "validate",
                "-c",
                "x.yaml",
                "--schemas",
                "--allow-missing-env",
                "--json",
                "-q",
            ],
        )

        main()

        (args,) = handler.call_args.args
        assert (args.config, args.schemas, args.allow_missing_env, args.json, args.quiet) == (
            "x.yaml",
            True,
            True,
            True,
            True,
        )
        mock_exit.assert_called_once_with(0)


_DATABASE_PASSWORD = "p@ss:w/rd %+&?#"
"""A password holding every character a URI treats specially."""

_DATABASE_CONFIG = """\
source:
  type: database
  uri: mysql://analyst@db.internal/sales
  password: ${VD_TEST_DB_PASSWORD}
  table: orders
target:
  path: orders.csv
primary_keys: [id]
"""
"""A MySQL table, read through a patched driver, against a CSV file in the working directory."""


class TestVerboseLogging:
    """Validate `--verbose`, which prints Veridelta's own log records on stderr."""

    @pytest.fixture
    def driver(
        self, tmp_path: Path, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
    ) -> MagicMock:
        """Write the configuration, and patch the driver to return two rows."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("VD_TEST_DB_PASSWORD", _DATABASE_PASSWORD)
        (tmp_path / "veridelta.yaml").write_text(_DATABASE_CONFIG)
        pl.DataFrame({"id": [1, 2], "status": ["open", "shipped"]}).write_csv(
            tmp_path / "orders.csv"
        )
        mocker.patch("veridelta.connectors.database.connectorx", object())
        return mocker.patch(
            "veridelta.connectors.database.pl.read_database_uri",
            return_value=pl.DataFrame({"id": [1, 2], "status": ["open", "closed"]}),
        )

    @staticmethod
    def _main(mocker: MockerFixture, *flags: str) -> object:
        """Run `veridelta run` with the flags given, and return its exit code."""
        mocker.patch("veridelta.cli.sys.argv", ["veridelta", "run", *flags])
        with pytest.raises(SystemExit) as stopped:
            main()
        return stopped.value.code

    @pytest.mark.parametrize("command", ["run", "validate", "crosswalk", "mcp"])
    @pytest.mark.parametrize("flag", ["-v", "--verbose"])
    def test_it_takes_the_flag_on_each_command_that_reads_a_configuration(
        self, command: str, flag: str
    ) -> None:
        """Ensure `-v` and `--verbose` parse wherever a configuration is read, and default off."""
        assert build_parser().parse_args([command, flag]).verbose is True
        assert build_parser().parse_args([command]).verbose is False

    def test_it_prints_each_read_on_stderr(
        self, driver: MagicMock, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a read logs its row count and source on stderr, and leaves no handler behind."""
        logger = logging.getLogger("veridelta")
        before = (list(logger.handlers), logger.level)

        code = self._main(mocker, "--verbose")
        captured = capsys.readouterr()

        assert code == 1
        assert (
            "INFO veridelta.connectors.database: Read 2 rows of table 'orders' "
            "from mysql://analyst@db.internal/sales in "
        ) in captured.err
        assert "veridelta.connectors" not in captured.out
        assert (list(logger.handlers), logger.level) == before

    def test_it_prints_no_record_without_the_flag(
        self, driver: MagicMock, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a run stays as quiet as before unless asked."""
        code = self._main(mocker)

        assert code == 1
        assert "veridelta.connectors" not in capsys.readouterr().err

    def test_it_keeps_stdout_one_json_object(
        self, driver: MagicMock, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `--json --verbose` sends the records to stderr, so stdout still parses."""
        code = self._main(mocker, "--json", "--verbose")
        captured = capsys.readouterr()

        assert code == 1
        assert json.loads(captured.out)["is_match"] is False
        assert "INFO veridelta.connectors.database: Read 2 rows" in captured.err

    def test_it_keeps_the_password_out_of_a_failed_read(
        self, driver: MagicMock, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a failed read logs a warning naming the source, and no form of the password.

        Drivers echo connection strings in their errors, so the stand-in error
        repeats the password raw and percent-encoded.
        """
        encoded = quote(_DATABASE_PASSWORD, safe="")
        driver.side_effect = RuntimeError(
            f"Access denied for mysql://analyst:{encoded}@db.internal/sales "
            f"with password {_DATABASE_PASSWORD}"
        )

        code = self._main(mocker, "--verbose")
        captured = capsys.readouterr()

        assert code == 3
        assert (
            "WARNING veridelta.connectors.database: Database read of table 'orders' "
            "from mysql://analyst@db.internal/sales failed after "
        ) in captured.err
        assert "Access denied" in captured.err
        printed = captured.out + captured.err
        assert _DATABASE_PASSWORD not in printed
        assert encoded not in printed


_DRIFT_CONFIG = """\
source:
  path: source.csv
target:
  path: target.csv
primary_keys: [id]
"""
"""Two CSV files in the working directory that differ in one value."""


class TestOTLPSend:
    """Validate `run --otel-send`, which sends the metrics `--otel` writes."""

    @pytest.fixture(autouse=True)
    def _drifting_files(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Write the drifting pair, and clear any OpenTelemetry variable the shell sets."""
        for name in list(os.environ):
            if name.startswith("OTEL_"):
                monkeypatch.delenv(name)
        monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
        monkeypatch.chdir(tmp_path)
        (tmp_path / "veridelta.yaml").write_text(_DRIFT_CONFIG)
        pl.DataFrame({"id": [1, 2], "val": ["A", "B"]}).write_csv(tmp_path / "source.csv")
        pl.DataFrame({"id": [1, 2], "val": ["A", "X"]}).write_csv(tmp_path / "target.csv")

    @staticmethod
    def _main(mocker: MockerFixture, *flags: str) -> object:
        """Run `veridelta run` with the flags given, and return its exit code."""
        mocker.patch("veridelta.cli.sys.argv", ["veridelta", "run", *flags])
        with pytest.raises(SystemExit) as stopped:
            main()
        return stopped.value.code

    def test_it_takes_the_flag_only_on_run(self) -> None:
        """Ensure `--otel-send` parses on `run`, and defaults off."""
        assert build_parser().parse_args(["run", "--otel-send"]).otel_send is True
        assert build_parser().parse_args(["run"]).otel_send is False

    def test_it_sends_the_export_it_writes(
        self,
        tmp_path: Path,
        mocker: MockerFixture,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure the request body and the file hold the same bytes, one timestamp included."""
        with running_collector() as collector:
            monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.url)

            code = self._main(mocker, "--otel", "otel-metrics.json", "--otel-send")

        assert code == 1
        (request,) = collector.received
        assert request.body + b"\n" == (tmp_path / "otel-metrics.json").read_bytes()
        assert (
            f"OpenTelemetry metrics sent to: {collector.url}/v1/metrics" in capsys.readouterr().err
        )

    def test_a_failed_send_stops_the_run_with_an_error(
        self,
        mocker: MockerFixture,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure a send that fails exits 3, and `--json` prints the error in place of the summary."""
        with running_collector() as collector:
            collector.status = 503
            monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.url)

            code = self._main(mocker, "--json", "--otel-send")

        assert code == 3
        error = json.loads(capsys.readouterr().out)["error"]
        assert error["type"] == "ConnectorError"
        assert error["message"] == (
            f"Sending metrics to {collector.url}/v1/metrics failed: "
            "the endpoint answered with HTTP 503 Service Unavailable."
        )
