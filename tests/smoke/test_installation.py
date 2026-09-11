# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Smoke tests to verify the package is installed and executable."""

import subprocess
from pathlib import Path

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

    def test_it_exports_every_name_it_advertises(self) -> None:
        """Ensure `__all__` resolves and stays sorted.

        A name listed but not imported passes a plain `import veridelta` and
        only fails on the user's `from veridelta import ...`.
        """
        import veridelta

        missing = [name for name in veridelta.__all__ if not hasattr(veridelta, name)]

        assert missing == []
        assert veridelta.__all__ == sorted(veridelta.__all__)

    def test_it_exports_the_public_surface_users_are_told_to_import(self) -> None:
        """Ensure the documented entry points resolve from the package root.

        The docs tell users to catch `VerideltaError` and to build warehouse
        configs directly, both of which previously required reaching into
        submodules.
        """
        from veridelta import (  # noqa: F401
            ConfigError,
            DiffResult,
            SnowflakeConfig,
            VerideltaError,
        )

        assert issubclass(ConfigError, VerideltaError)

    def test_it_ships_the_typing_marker(self) -> None:
        """Ensure downstream type checkers can see the annotations.

        Without `py.typed`, PEP 561 tells mypy and pyright to treat an
        installed package as untyped, silently, no matter how complete its
        annotations are.
        """
        import veridelta

        package_root = Path(veridelta.__file__).parent

        assert (package_root / "py.typed").is_file()

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
