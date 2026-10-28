# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the run record: the models, the hashes, git, and the CI run URL.

Git and file reads are external side effects, so they are mocked here. The
integration suite runs the same code against a real repository and real files.
"""

import subprocess
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from veridelta import __version__
from veridelta._provenance import collect_provenance
from veridelta.models import HashedFile, RunProvenance

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_STARTED_AT = datetime(2026, 10, 28, 9, 30, tzinfo=UTC)
_COMMIT_SHA = "a" * 40
_CONFIG_BYTES = b"primary_keys: [order_id]\n"
_BASELINE_BYTES = b'{"primary_keys": ["order_id"]}\n'
_GITHUB_ACTIONS = {
    "GITHUB_SERVER_URL": "https://github.com",
    "GITHUB_REPOSITORY": "acme/orders",
    "GITHUB_RUN_ID": "123",
    "GITHUB_RUN_ATTEMPT": "2",
    "GITHUB_SHA": "b" * 40,
}


def _git_answers(mocker: MockerFixture, *, head: str | None, status: str | None) -> MagicMock:
    """Replace git: `rev-parse HEAD` prints `head`, `status` prints `status`, None fails.

    Returns:
        MagicMock: The mocked `subprocess.run`.
    """

    def answer(arguments: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        output = head if arguments[1] == "rev-parse" else status
        if output is None:
            raise subprocess.CalledProcessError(128, arguments)
        return subprocess.CompletedProcess(arguments, 0, stdout=f"{output}\n", stderr="")

    return mocker.patch("veridelta._provenance.subprocess.run", side_effect=answer)


def _files(mocker: MockerFixture, contents_by_path: dict[str, bytes]) -> None:
    """Replace file reads, so each path reads the bytes given for it."""

    def read_bytes(path: Path) -> bytes:
        return contents_by_path[str(path)]

    mocker.patch("veridelta._provenance.Path.read_bytes", autospec=True, side_effect=read_bytes)


class TestRunProvenanceModel:
    """Validate the run record's models, which `run --json` prints."""

    def test_it_refuses_a_hash_that_is_not_sha256_hex(self) -> None:
        """Ensure a hash is 64 lowercase hex digits, so a reader can compare it with `sha256sum`."""
        with pytest.raises(ValidationError, match="sha256"):
            HashedFile(path="veridelta.yaml", sha256="ABC")

    def test_it_refuses_a_commit_that_is_not_a_full_sha(self) -> None:
        """Ensure a commit is a full SHA-1 or SHA-256 hex digest, never a branch name."""
        with pytest.raises(ValidationError, match="commit_sha"):
            RunProvenance(veridelta_version="1.0.0", started_at=_STARTED_AT, commit_sha="main")

    @pytest.mark.parametrize(
        "url",
        [
            pytest.param("http://github.com/acme/orders/actions/runs/1", id="http"),
            pytest.param("javascript:alert(1)", id="javascript"),
            pytest.param("https://x.test/run) [a](https://evil.test", id="markdown-break"),
            pytest.param('https://x.test/"onmouseover="alert(1)', id="attribute-break"),
            pytest.param("https://x.test/a b", id="space"),
        ],
    )
    def test_it_refuses_a_ci_run_url_that_is_not_https(self, url: str) -> None:
        """Ensure the report can link the CI run, in Markdown or an `href`, without checking it again."""
        with pytest.raises(ValidationError, match="ci_run_url"):
            RunProvenance(veridelta_version="1.0.0", started_at=_STARTED_AT, ci_run_url=url)

    def test_it_refuses_a_start_time_without_a_zone(self) -> None:
        """Ensure the start time names its zone, so two machines' records compare."""
        with pytest.raises(ValidationError, match="started_at"):
            RunProvenance(veridelta_version="1.0.0", started_at=datetime(2026, 10, 28, 9, 30))

    def test_it_cannot_be_changed_after_it_is_made(self) -> None:
        """Ensure a record is frozen, so nothing rewrites it between the run and the report."""
        record = RunProvenance(veridelta_version="1.0.0", started_at=_STARTED_AT)

        with pytest.raises(ValidationError, match="frozen"):
            record.commit_sha = _COMMIT_SHA  # type: ignore[misc] # the assignment is the test


class TestCollectProvenance:
    """Validate what `collect_provenance` reads from the files, git, and the environment."""

    def test_it_hashes_the_files_and_reads_git_and_github(self, mocker: MockerFixture) -> None:
        """Ensure every field is filled when git, the files, and GitHub Actions all answer."""
        _files(mocker, {"veridelta.yaml": _CONFIG_BYTES, "accepted.json": _BASELINE_BYTES})
        _git_answers(mocker, head=_COMMIT_SHA, status="")

        record = collect_provenance(
            "veridelta.yaml", "accepted.json", started_at=_STARTED_AT, environ=_GITHUB_ACTIONS
        )

        assert record == RunProvenance(
            veridelta_version=__version__,
            started_at=_STARTED_AT,
            config_file=HashedFile(path="veridelta.yaml", sha256=sha256(_CONFIG_BYTES).hexdigest()),
            baseline_file=HashedFile(
                path="accepted.json", sha256=sha256(_BASELINE_BYTES).hexdigest()
            ),
            commit_sha=_COMMIT_SHA,
            working_tree_clean=True,
            ci_run_url="https://github.com/acme/orders/actions/runs/123/attempts/2",
        )

    def test_it_records_no_file_for_a_run_of_two_files(self, mocker: MockerFixture) -> None:
        """Ensure a run with no configuration file and no baseline hashes nothing."""
        read_bytes = mocker.patch("veridelta._provenance.Path.read_bytes")
        _git_answers(mocker, head=None, status=None)

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ={})

        assert (record.config_file, record.baseline_file) == (None, None)
        read_bytes.assert_not_called()

    def test_it_records_no_hash_for_a_file_it_cannot_read(self, mocker: MockerFixture) -> None:
        """Ensure a file that vanished since it was loaded leaves its field empty, not a failed run."""
        mocker.patch("veridelta._provenance.Path.read_bytes", side_effect=PermissionError("denied"))
        _git_answers(mocker, head=None, status=None)

        record = collect_provenance(
            "veridelta.yaml", "accepted.json", started_at=_STARTED_AT, environ={}
        )

        assert (record.config_file, record.baseline_file) == (None, None)

    def test_it_says_when_a_tracked_file_changed(self, mocker: MockerFixture) -> None:
        """Ensure a modified tracked file marks the tree as not clean."""
        _git_answers(mocker, head=_COMMIT_SHA, status=" M veridelta.yaml")

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ={})

        assert (record.commit_sha, record.working_tree_clean) == (_COMMIT_SHA, False)

    def test_it_ignores_untracked_files(self, mocker: MockerFixture) -> None:
        """Ensure git is asked about tracked files only, so a run's own outputs leave it clean."""
        run_git = _git_answers(mocker, head=_COMMIT_SHA, status="")

        collect_provenance(None, None, started_at=_STARTED_AT, environ={})

        status_call = run_git.call_args_list[1]
        assert status_call.args[0] == ["git", "status", "--porcelain", "--untracked-files=no"]

    def test_it_falls_back_to_the_ci_commit_without_git(self, mocker: MockerFixture) -> None:
        """Ensure a run outside a repository takes GitHub's commit, and leaves cleanliness unknown."""
        mocker.patch("veridelta._provenance.subprocess.run", side_effect=FileNotFoundError("git"))

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ=_GITHUB_ACTIONS)

        assert (record.commit_sha, record.working_tree_clean) == ("b" * 40, None)

    def test_it_leaves_cleanliness_unknown_when_status_fails(self, mocker: MockerFixture) -> None:
        """Ensure a failed `git status` leaves the field empty instead of guessing."""
        _git_answers(mocker, head=_COMMIT_SHA, status=None)

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ={})

        assert (record.commit_sha, record.working_tree_clean) == (_COMMIT_SHA, None)

    @pytest.mark.parametrize(
        "failure",
        [
            pytest.param(subprocess.TimeoutExpired(["git"], 5), id="timeout"),
            pytest.param(UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad"), id="undecodable"),
        ],
    )
    def test_it_never_fails_the_run_when_git_cannot_answer(
        self, mocker: MockerFixture, failure: Exception
    ) -> None:
        """Ensure a git that hangs or prints undecodable text leaves the fields empty."""
        mocker.patch("veridelta._provenance.subprocess.run", side_effect=failure)

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ={})

        assert (record.commit_sha, record.working_tree_clean) == (None, None)

    def test_it_ignores_a_head_that_is_not_a_commit(self, mocker: MockerFixture) -> None:
        """Ensure only a full commit SHA is recorded, whatever git prints."""
        _git_answers(mocker, head="HEAD", status="")

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ={"GITHUB_SHA": "x"})

        assert (record.commit_sha, record.working_tree_clean) == (None, None)

    def test_it_records_no_url_outside_github_actions(self, mocker: MockerFixture) -> None:
        """Ensure a local run names no CI run."""
        _git_answers(mocker, head=None, status=None)

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ={})

        assert record.ci_run_url is None

    def test_it_links_the_run_without_an_attempt(self, mocker: MockerFixture) -> None:
        """Ensure a run with no attempt number links the run itself."""
        _git_answers(mocker, head=None, status=None)
        environment = {key: value for key, value in _GITHUB_ACTIONS.items() if "ATTEMPT" not in key}

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ=environment)

        assert record.ci_run_url == "https://github.com/acme/orders/actions/runs/123"

    @pytest.mark.parametrize(
        ("variable", "value"),
        [
            pytest.param("GITHUB_SERVER_URL", "http://github.com", id="http-server"),
            pytest.param("GITHUB_SERVER_URL", "https://user:pw@github.com", id="credentials"),
            pytest.param("GITHUB_SERVER_URL", "https://github.com/evil?x=1", id="path-and-query"),
            pytest.param("GITHUB_SERVER_URL", "https://[::1", id="unparsable"),
            pytest.param("GITHUB_REPOSITORY", "acme/orders/../x", id="repository-path"),
            pytest.param("GITHUB_REPOSITORY", "acme orders", id="repository-space"),
            pytest.param("GITHUB_RUN_ID", "12a", id="run-id"),
        ],
    )
    def test_it_records_no_url_from_a_variable_it_cannot_trust(
        self, mocker: MockerFixture, variable: str, value: str
    ) -> None:
        """Ensure a variable outside its expected shape names no CI run, rather than a link."""
        _git_answers(mocker, head=None, status=None)
        environment = {**_GITHUB_ACTIONS, variable: value}

        record = collect_provenance(None, None, started_at=_STARTED_AT, environ=environment)

        assert record.ci_run_url is None

    def test_it_reads_the_process_environment_by_default(self, mocker: MockerFixture) -> None:
        """Ensure the CLI's call, which passes no environment, reads the real one."""
        _git_answers(mocker, head=None, status=None)
        mocker.patch.dict("os.environ", _GITHUB_ACTIONS, clear=True)

        record = collect_provenance(None, None, started_at=_STARTED_AT)

        assert record.ci_run_url == "https://github.com/acme/orders/actions/runs/123/attempts/2"
