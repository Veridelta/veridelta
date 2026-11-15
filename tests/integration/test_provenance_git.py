# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for the run record against a real git, in folders made for each test."""

import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from veridelta._provenance import collect_provenance

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(shutil.which("git") is None, reason="these tests run a real git"),
]

_STARTED_AT = datetime(2026, 10, 28, 9, 30, tzinfo=UTC)
_FORGED_SHA = "0123456789abcdef0123456789abcdef01234567"


@pytest.fixture(autouse=True)
def _isolated_git(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep the user's git settings and any enclosing repository out of each test."""
    for variable in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_CONFIG_PARAMETERS"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path.parent))


def _git(folder: Path, *arguments: str) -> str:
    """Run git in a folder and return what it prints.

    Returns:
        str: The command's output, without the trailing newline.
    """
    completed = subprocess.run(
        ["git", *arguments], capture_output=True, check=True, cwd=folder, text=True
    )
    return completed.stdout.strip()


class TestGitProvenance:
    """Validate which commit `collect_provenance` records from the folder it runs in."""

    def test_it_ignores_a_committed_folder_shaped_like_a_bare_repository(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Ensure ordinary files that look like a bare repository cannot name the commit."""
        bundle = tmp_path / "bundle"
        for folder in ("objects", "refs/heads"):
            (bundle / folder).mkdir(parents=True)
        (bundle / "HEAD").write_text(f"{_FORGED_SHA}\n")
        (bundle / "config").write_text("[core]\n\trepositoryformatversion = 0\n\tbare = false\n")
        assert _git(bundle, "rev-parse", "HEAD") == _FORGED_SHA
        monkeypatch.chdir(bundle)

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ={})

        assert (record.commit_sha, record.working_tree_clean) == (None, None)

    def test_it_records_a_repository_commit_without_its_fsmonitor(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Ensure an ordinary repository still gives its commit, and its fsmonitor never starts."""
        marker = tmp_path / "fsmonitor-ran"
        repository = tmp_path / "repository"
        repository.mkdir()
        _git(repository, "init", "--quiet")
        (repository / "veridelta.yaml").write_text("primary_keys: [order_id]\n")
        _git(repository, "add", "veridelta.yaml")
        _git(
            repository,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--quiet",
            "--message",
            "Add the configuration",
        )
        _git(repository, "config", "core.fsmonitor", f"touch '{marker}'; false")
        monkeypatch.chdir(repository)

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ={})

        assert (record.commit_sha, record.working_tree_clean) == (
            _git(repository, "-c", "core.fsmonitor=false", "rev-parse", "HEAD"),
            True,
        )
        assert not marker.exists()
