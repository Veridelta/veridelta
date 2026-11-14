# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for `.github/scripts/lock_diff.py`, which refuses a lockfile that moves backward.

CI runs the script on every pull request's `uv.lock`, and the Security Updates
workflow runs it on the lockfile it proposes. A Dependabot pull request once
pinned soupsieve 4.0 for every Python, which Python 3.11.0 to 3.11.4 cannot
install, and the lock fell back to beautifulsoup4 4.6.3 there. The cases below
keep that shape failing, and keep a new release for one range of Python passing.
"""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]


def _load_script() -> ModuleType:
    """Import the script by path, as CI runs it."""
    path = _ROOT / ".github" / "scripts" / "lock_diff.py"
    spec = importlib.util.spec_from_file_location("lock_diff", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


lock_diff = _load_script()


def _write_lock(path: Path, packages: dict[str, list[str]], revision: int = 3) -> Path:
    """Write a lockfile that holds each package at each of its versions."""
    lines = ["version = 1", f"revision = {revision}", 'requires-python = ">=3.11"', ""]
    for name, versions in packages.items():
        for version in versions:
            lines += ["[[package]]", f'name = "{name}"', f'version = "{version}"', ""]
    lines += ["[[package]]", 'name = "virtual"', 'source = { virtual = "." }', ""]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


class TestVersionKey:
    """Pin the PEP 440 order the comparison relies on."""

    def test_it_orders_development_pre_final_and_post_releases(self) -> None:
        """Ensure each kind of release sorts where PEP 440 puts it."""
        ordered = [
            "1.0.dev1",
            "1.0a1.dev1",
            "1.0a1",
            "1.0b2",
            "1.0rc1",
            "1.0",
            "1.0.post1.dev1",
            "1.0.post1",
            "1.1",
        ]
        keys = [lock_diff.version_key(version) for version in ordered]

        assert keys == sorted(keys)
        assert len(set(keys)) == len(keys)

    def test_it_ignores_trailing_zeros(self) -> None:
        """Ensure `1.0` and `1` are the same release, and `1.0.1` is newer."""
        assert lock_diff.version_key("1.0") == lock_diff.version_key("1")
        assert lock_diff.version_key("1.0") < lock_diff.version_key("1.0.1")

    def test_it_compares_dated_releases_as_numbers(self) -> None:
        """Ensure a typeshed stub dated later sorts later."""
        assert lock_diff.version_key("6.0.12.20261030") < lock_diff.version_key("6.0.12.20261103")
        assert lock_diff.version_key("4.6.3") < lock_diff.version_key("4.15.0")

    def test_it_reads_no_other_shape(self) -> None:
        """Ensure a local or epoch version is left out instead of misread."""
        assert lock_diff.version_key("1.0+local") is None
        assert lock_diff.version_key("1!2.0") is None


class TestBackward:
    """Pin which changes count as moving a package to an older version."""

    def test_a_downgrade_fails(self) -> None:
        """Ensure a package that moves down is reported."""
        assert lock_diff.backward({"tornado": {"6.6"}}, {"tornado": {"6.5"}}) == [
            "tornado 6.5 is older than 6.6, which the base lock holds."
        ]

    def test_an_upgrade_passes(self) -> None:
        """Ensure an ordinary upgrade is not reported."""
        assert lock_diff.backward({"tornado": {"6.6"}}, {"tornado": {"6.6.1"}}) == []

    def test_an_old_release_added_for_one_python_fails(self) -> None:
        """Ensure the pinned-upgrade fallback from the module docstring is reported."""
        base = {"beautifulsoup4": {"4.15.0"}, "soupsieve": {"2.10", "3.0.4"}}
        head = {"beautifulsoup4": {"4.6.3", "4.15.0"}, "soupsieve": {"4.0"}}

        assert lock_diff.backward(base, head) == [
            "beautifulsoup4 4.6.3 is older than 4.15.0, which the base lock holds."
        ]

    def test_an_upgrade_of_one_python_range_passes(self) -> None:
        """Ensure moving the newer of two versions up is not reported."""
        assert (
            lock_diff.backward({"soupsieve": {"2.10", "3.0.4"}}, {"soupsieve": {"2.10", "4.1.1"}})
            == []
        )

    def test_a_new_release_for_the_older_python_range_passes(self) -> None:
        """Ensure each version is held to the one it replaces, not to the newest."""
        assert (
            lock_diff.backward({"soupsieve": {"2.10", "3.0.4"}}, {"soupsieve": {"2.11", "4.1.1"}})
            == []
        )

    def test_a_downgrade_of_one_python_range_fails(self) -> None:
        """Ensure versions replaced one for one are compared in pairs."""
        assert lock_diff.backward(
            {"soupsieve": {"2.10", "3.0.4"}}, {"soupsieve": {"2.11", "3.0.1"}}
        ) == ["soupsieve 3.0.1 is older than 3.0.4, which the base lock holds."]

    def test_ranges_that_merge_pass(self) -> None:
        """Ensure two versions that become one newer version are not reported."""
        assert lock_diff.backward({"soupsieve": {"2.10", "3.0.4"}}, {"soupsieve": {"4.1.1"}}) == []

    def test_an_unreadable_version_is_left_out(self) -> None:
        """Ensure a version of no known shape neither fails nor hides another."""
        assert lock_diff.backward({"tool": {"1.0+local"}}, {"tool": {"0.9"}}) == []

    def test_an_added_or_removed_package_passes(self) -> None:
        """Ensure a new dependency or a dropped one is not a downgrade."""
        assert lock_diff.backward({"old": {"2.0"}}, {"new": {"1.0"}}) == []


class TestMain:
    """Pin the command line CI and the Security Updates workflow run."""

    def test_it_lists_changes_and_passes_an_upgrade(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure an upgrade exits 0 and writes the Markdown table it was asked for."""
        base = _write_lock(tmp_path / "base.lock", {"tornado": ["6.6"], "zstd": ["1.0"]})
        head = _write_lock(
            tmp_path / "head.lock", {"tornado": ["6.6.1"], "new": ["2.0"], "zstd": ["1.0"]}
        )
        table = tmp_path / "changes.md"

        assert lock_diff.main([str(base), str(head), "--markdown", str(table)]) == 0
        assert capsys.readouterr().out == "new: - -> 2.0\ntornado: 6.6 -> 6.6.1\n"
        assert table.read_text(encoding="utf-8") == (
            "| Package | From | To |\n| :--- | :--- | :--- |\n| new | - | 2.0 |\n| tornado | 6.6 | 6.6.1 |\n"
        )

    def test_it_fails_a_lock_that_moves_backward(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a downgrade exits 1 with an error annotation."""
        base = _write_lock(tmp_path / "base.lock", {"tornado": ["6.6"]})
        head = _write_lock(tmp_path / "head.lock", {"tornado": ["6.5"]})

        assert lock_diff.main([str(base), str(head)]) == 1
        assert (
            "::error::tornado 6.5 is older than 6.6, which the base lock holds.\n"
            in capsys.readouterr().out
        )

    def test_it_warns_when_another_uv_wrote_the_lock(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a changed revision warns and still passes, as every Dependabot lock does."""
        base = _write_lock(tmp_path / "base.lock", {"tornado": ["6.6"]})
        head = _write_lock(tmp_path / "head.lock", {"tornado": ["6.6.1"]}, revision=5)

        assert lock_diff.main([str(base), str(head)]) == 0
        assert "::warning::The lockfile's revision moves from 3 to 5" in capsys.readouterr().out

    @pytest.mark.parametrize("argv", [[], ["one"], ["a", "b", "--table", "c"], ["a", "b", "c"]])
    def test_it_refuses_another_command_line(
        self, argv: list[str], capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a wrong command line exits 2 with the usage."""
        assert lock_diff.main(argv) == 2
        assert capsys.readouterr().err.startswith("usage: lock_diff.py BASE HEAD")
