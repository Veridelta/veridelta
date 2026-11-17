# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold the package's dependency floors past the releases they were raised for.

A floor is a promise to users. Someone who already has an older driver keeps
it, since Dependabot moves only the lockfile, so each floor stays at or above
the first release with no known advisory, or the first whose behavior the
package's checks follow, as `pyproject.toml` says beside it.
"""

import tomllib
from itertools import chain
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.version import Version

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]

_FIRST_WITHOUT_ADVISORY = {
    # CVE-2023-47248 and CVE-2026-25087, both in reading an Arrow IPC file.
    "pyarrow": Version("23.0.1"),
    # CVE-2024-49750 and CVE-2026-15925, among others.
    "snowflake-connector-python": Version("4.7.3"),
}

_FIRST_THE_CHECKS_FOLLOW = {
    # The MCP server names the reader a database URI runs on from
    # `connectorx_url`, which follows this release's scheme rewrite. 0.4.5
    # reads `clickhouse://` as MySQL and `redshift-iam://` as Postgres.
    "connectorx": Version("0.4.6"),
    # `checked_file_io` parses each location as this release does, with the
    # storage options, and reads a Windows path with a drive letter as a path
    # on this machine. Before 0.11.0, the listing and the scan could read a
    # manifest list from a cache an earlier scan filled, not from the file.
    "pyiceberg": Version("0.12.0"),
}


def _requirements(name: str) -> list[Requirement]:
    """Return every requirement on `name`, in the dependencies and in every extra."""
    with (_ROOT / "pyproject.toml").open("rb") as file:
        project = tomllib.load(file)["project"]
    lines = chain(project["dependencies"], *project["optional-dependencies"].values())
    return [requirement for line in lines if (requirement := Requirement(line)).name == name]


@pytest.mark.parametrize(
    ("name", "first"), sorted({**_FIRST_WITHOUT_ADVISORY, **_FIRST_THE_CHECKS_FOLLOW}.items())
)
def test_every_requirement_starts_at_its_first_safe_release(name: str, first: Version) -> None:
    """Ensure no extra lets an install keep a release older than its floor's reason allows."""
    requirements = _requirements(name)

    assert requirements, f"pyproject.toml no longer requires {name}."
    for requirement in requirements:
        floors = [Version(spec.version) for spec in requirement.specifier if spec.operator == ">="]
        assert floors, f"{requirement} has no floor."
        assert max(floors) >= first, str(requirement)
