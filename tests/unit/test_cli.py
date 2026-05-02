# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Veridelta command-line interface."""

import argparse
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from veridelta.cli import main, run
from veridelta.exceptions import ConfigError


@pytest.mark.unit
@pytest.mark.fast
class TestCommandLineInterface:
    """Validate CLI argument parsing, workflow execution, and exit codes."""

    @pytest.fixture
    def default_args(self) -> argparse.Namespace:
        """Provide a default argparse namespace for testing the run function."""
        return argparse.Namespace(config="dummy.yaml")

    def test_it_returns_exit_code_zero_when_datasets_match(
        self, mocker: MockerFixture, default_args: argparse.Namespace
    ) -> None:
        """Ensure a successful comparison returns a 0 exit code for CI/CD pipelines."""
        mock_load = mocker.patch("veridelta.config.load_config")
        mock_engine = mocker.patch("veridelta.engine.DiffEngine")

        mock_diff_config = MagicMock(output_path=None)
        mock_src_cfg = MagicMock()
        mock_tgt_cfg = MagicMock()
        mock_load.return_value = (mock_diff_config, mock_src_cfg, mock_tgt_cfg)

        mock_summary = MagicMock(is_match=True, report_summary="Status: PASSED")
        mock_engine.return_value.execute.return_value = mock_summary

        exit_code = run(default_args)

        assert exit_code == 0
        mock_load.assert_called_once_with("dummy.yaml")
        mock_engine.assert_called_once_with(mock_diff_config)
        mock_engine.return_value.execute.assert_called_once_with(mock_src_cfg, mock_tgt_cfg)

    def test_it_returns_exit_code_one_when_datasets_do_not_match(
        self, mocker: MockerFixture, default_args: argparse.Namespace
    ) -> None:
        """Ensure a failed comparison returns a 1 exit code."""
        mock_load = mocker.patch("veridelta.config.load_config")
        mock_engine = mocker.patch("veridelta.engine.DiffEngine")

        mock_diff_config = MagicMock(output_path=None)
        mock_load.return_value = (mock_diff_config, MagicMock(), MagicMock())

        mock_summary = MagicMock(is_match=False, report_summary="Status: FAILED")
        mock_engine.return_value.execute.return_value = mock_summary

        exit_code = run(default_args)

        assert exit_code == 1

    def test_it_prints_artifact_paths_when_output_is_configured_and_mismatch_occurs(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure users are notified of where artifacts are saved upon failure."""
        mock_load = mocker.patch("veridelta.config.load_config")
        mock_engine = mocker.patch("veridelta.engine.DiffEngine")

        mock_diff_config = MagicMock(output_path="/tmp/diffs")
        mock_load.return_value = (mock_diff_config, MagicMock(), MagicMock())

        mock_summary = MagicMock(is_match=False, report_summary="Status: FAILED")
        mock_engine.return_value.execute.return_value = mock_summary

        run(default_args)
        captured = capsys.readouterr()

        assert "Artifacts saved to:" in captured.out
        assert "diffs" in captured.out

    def test_it_catches_config_errors_and_returns_exit_code_two_via_stderr(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure validation errors gracefully halt execution and return Exit 2."""
        mock_load = mocker.patch("veridelta.config.load_config")
        mock_load.side_effect = ConfigError("Invalid schema mode")

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 2
        assert "Configuration Error" in captured.err
        assert "Invalid schema mode" in captured.err

    def test_it_catches_unexpected_exceptions_and_returns_exit_code_two_via_stderr(
        self,
        mocker: MockerFixture,
        default_args: argparse.Namespace,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Ensure unhandled system errors halt the runner and return Exit 2."""
        mock_load = mocker.patch("veridelta.config.load_config")
        mock_load.side_effect = RuntimeError("Disk full")

        exit_code = run(default_args)
        captured = capsys.readouterr()

        assert exit_code == 2
        assert "Unexpected System Error" in captured.err
        assert "Disk full" in captured.err

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

    def test_main_handles_version_flag(
        self, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure the --version flag dynamically prints the package version and exits."""
        mocker.patch("importlib.metadata.version", return_value="99.9.9")
        mocker.patch("veridelta.cli.sys.argv", ["veridelta", "--version"])

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "veridelta 99.9.9" in captured.out
