# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold the package's dependency floors past the advisories they were raised for.

A floor is a promise to users. Someone who already has an older driver keeps
it, since Dependabot moves only the lockfile, so each floor stays at or above
the first release with no known advisory, as `pyproject.toml` says beside it.
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


def _requirements(name: str) -> list[Requirement]:
    """Return every requirement on `name`, in the dependencies and in every extra."""
    with (_ROOT / "pyproject.toml").open("rb") as file:
        project = tomllib.load(file)["project"]
    lines = chain(project["dependencies"], *project["optional-dependencies"].values())
    return [requirement for line in lines if (requirement := Requirement(line)).name == name]


@pytest.mark.parametrize("name", sorted(_FIRST_WITHOUT_ADVISORY))
def test_every_requirement_starts_past_the_known_advisories(name: str) -> None:
    """Ensure no extra lets an install keep a release with a known advisory."""
    requirements = _requirements(name)

    assert requirements, f"pyproject.toml no longer requires {name}."
    for requirement in requirements:
        floors = [Version(spec.version) for spec in requirement.specifier if spec.operator == ">="]
        assert floors, f"{requirement} has no floor."
        assert max(floors) >= _FIRST_WITHOUT_ADVISORY[name], str(requirement)
