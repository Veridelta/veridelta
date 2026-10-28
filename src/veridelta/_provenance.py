# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The run record: what a run read and where it ran, so a verdict can be traced.

`collect_provenance` hashes the configuration and baseline files with
SHA-256, asks git for the checked-out commit and whether a tracked file
changed, and builds the CI run's URL from the variables GitHub Actions sets.
Git and GitHub keep who declared a rule and who approved a run; the hashes and
the commit tie a verdict to them.

Nothing here fails a run. A missing git, a folder outside a repository, or an
unset variable leaves its field empty. A variable outside its expected shape
is treated as unset, so the record never links anywhere but GitHub's run page.
"""

import os
import re
import subprocess
from collections.abc import Mapping
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit

from veridelta import __version__
from veridelta.models import HashedFile, RunProvenance

_GIT_TIMEOUT_SECONDS: Final = 5.0
"""How long one git command may take before its field is left empty."""

_COMMIT_SHA: Final = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
"""A full SHA-1 or SHA-256 commit id, the forms git and `GITHUB_SHA` print."""

_REPOSITORY: Final = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
"""`GITHUB_REPOSITORY`: an owner and a name, which GitHub limits to these characters."""

_DIGITS: Final = re.compile(r"[0-9]+")
"""`GITHUB_RUN_ID` and `GITHUB_RUN_ATTEMPT`."""


def collect_provenance(
    config_path: str | None,
    baseline_path: str | None,
    *,
    started_at: datetime,
    environ: Mapping[str, str] | None = None,
) -> RunProvenance:
    """Record what a run read and where it ran.

    Args:
        config_path (str | None): The configuration file as given, or None for
            a run of two files.
        baseline_path (str | None): The `--baseline` file as given, or None.
        started_at (datetime): When the run started, with its time zone.
        environ (Mapping[str, str] | None): The environment to read GitHub
            Actions' variables from. None reads the process's own.

    Returns:
        RunProvenance: The hashes, the commit, and the CI run, each empty when
            its source is missing.
    """
    environment = os.environ if environ is None else environ
    commit_sha, working_tree_clean = _git_state(environment)
    return RunProvenance(
        veridelta_version=__version__,
        started_at=started_at,
        config_file=_hashed_file(config_path),
        baseline_file=_hashed_file(baseline_path),
        commit_sha=commit_sha,
        working_tree_clean=working_tree_clean,
        ci_run_url=_ci_run_url(environment),
    )


def _hashed_file(path: str | None) -> HashedFile | None:
    """Hash a file's bytes, keeping the path as given, or return None without one to read.

    The run has already read the file, so a failure here means it changed since,
    and the record leaves the field empty rather than failing the run.
    """
    if path is None:
        return None
    try:
        file_bytes = Path(path).read_bytes()
    except OSError:
        return None
    return HashedFile(path=path, sha256=sha256(file_bytes).hexdigest())


def _git_state(environment: Mapping[str, str]) -> tuple[str | None, bool | None]:
    """Return the checked-out commit and whether tracked files match it.

    Git answers first, since it sees the files this run read. Without git, or
    outside a repository, `GITHUB_SHA` names the commit and cleanliness stays
    unknown. Untracked files are ignored, so a run's own outputs leave it clean.
    """
    head_output = _git("rev-parse", "HEAD")
    if head_output is None or not _COMMIT_SHA.fullmatch(head_output):
        ci_commit_sha = environment.get("GITHUB_SHA", "")
        return (ci_commit_sha if _COMMIT_SHA.fullmatch(ci_commit_sha) else None), None
    status_output = _git("status", "--porcelain", "--untracked-files=no")
    working_tree_clean = None if status_output is None else status_output == ""
    return head_output, working_tree_clean


def _git(*arguments: str) -> str | None:
    """Run one read-only git command in the working folder, or return None if it fails."""
    try:
        completed = subprocess.run(  # fixed arguments, no shell, read-only commands
            ["git", *arguments],  # git from PATH, as the user runs it
            capture_output=True,
            check=True,
            stdin=subprocess.DEVNULL,
            text=True,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError, UnicodeDecodeError):
        return None
    return completed.stdout.strip()


def _ci_run_url(environment: Mapping[str, str]) -> str | None:
    """Build the GitHub Actions run's https URL, or return None outside a trusted shape."""
    try:
        server = urlsplit(environment.get("GITHUB_SERVER_URL", ""))
    except ValueError:
        return None
    repository = environment.get("GITHUB_REPOSITORY", "")
    run_id = environment.get("GITHUB_RUN_ID", "")
    server_is_plain_https = (
        server.scheme == "https"
        and bool(server.hostname)
        and "@" not in server.netloc
        and server.path in ("", "/")
        and not server.query
        and not server.fragment
    )
    if not (
        server_is_plain_https
        and _REPOSITORY.fullmatch(repository)
        and _DIGITS.fullmatch(run_id)
        and ".." not in repository
    ):
        return None
    run_url = f"https://{server.netloc}/{repository}/actions/runs/{run_id}"
    attempt = environment.get("GITHUB_RUN_ATTEMPT", "")
    return f"{run_url}/attempts/{attempt}" if _DIGITS.fullmatch(attempt) else run_url
