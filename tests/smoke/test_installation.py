# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Smoke tests to verify the package is installed and executable."""

import subprocess

import pytest


@pytest.mark.smoke
@pytest.mark.fast
class TestInstallationAndBoot:
    """Validate the environment plumbing and CLI entry points."""

    def test_it_imports_the_core_package_without_circular_dependencies(self) -> None:
        """Ensure the package can be loaded into memory without crashing."""
        from veridelta import (
            cli,
            datasets,
            engine,
            exceptions,
            models,
        )

        assert cli.main is not None
        assert engine.DiffEngine is not None
        assert models.DiffConfig is not None
        assert datasets.load_nyc_taxi is not None
        assert exceptions.ConfigError is not None

    def test_it_executes_the_cli_help_command_successfully(self) -> None:
        """Ensure the CLI entrypoint is registered and can boot without crashing."""
        result = subprocess.run(
            ["veridelta", "--help"], capture_output=True, text=True, check=False
        )

        assert result.returncode == 0

        assert "veridelta" in result.stdout.lower()
        assert "Semantic diffing" in result.stdout

    def test_it_exposes_a_valid_version_string(self) -> None:
        """Ensure the package version is accessible for debugging and pip distribution."""
        import veridelta

        assert hasattr(veridelta, "__version__")
        assert isinstance(veridelta.__version__, str)
        assert len(veridelta.__version__) > 0

    def test_it_handles_fatal_errors_gracefully_without_dumping_raw_tracebacks(self) -> None:
        """Ensure the CLI global exception handler catches errors and exits cleanly."""
        result = subprocess.run(
            ["veridelta", "run", "-c", "this_file_definitely_does_not_exist.yaml"],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 1

        assert "Configuration Error" in result.stderr
        assert "Traceback" not in result.stderr
        assert "Traceback" not in result.stdout
