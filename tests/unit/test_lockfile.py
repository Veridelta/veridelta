# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep `uv.lock` to the Pythons and platforms that every locked wheel serves.

uv resolves the lockfile for every Python and platform that `[tool.uv]
environments` allows, and refuses to relock when a package that ships wheels
alone has none for one of them. ConnectorX ships no wheel for
Python 3.15 or for Emscripten, so a lockfile that covered either blocked every
Dependabot update, as `decisions/lock-covers-the-tested-pythons.md` records.
These tests read the two files offline and fail where uv would refuse.
"""

import re
import tomllib
from itertools import chain
from pathlib import Path
from typing import Any

import pytest
from packaging.markers import Marker
from packaging.requirements import Requirement
from packaging.tags import Tag
from packaging.utils import canonicalize_name, parse_wheel_filename

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]

_PLATFORMS = {
    "linux": ("manylinux", "musllinux", "linux"),
    "darwin": ("macosx",),
    "win32": ("win",),
    "emscripten": ("emscripten", "pyodide", "pyemscripten"),
}
"""Each `sys_platform` a marker can name, with the wheel platform tags that serve it."""


def _load(name: str) -> dict[str, Any]:
    """Return a TOML file at the repository root."""
    with (_ROOT / name).open("rb") as file:
        return tomllib.load(file)


def _covered() -> set[tuple[int, str]]:
    """Return each Python 3 minor release and platform that the lockfile covers."""
    markers = [Marker(marker) for marker in _load("uv.lock")["supported-markers"]]
    return {
        (minor, platform)
        for minor in range(100)
        for platform in _PLATFORMS
        if any(
            marker.evaluate(
                {
                    "python_full_version": f"3.{minor}.99",
                    "python_version": f"3.{minor}",
                    "sys_platform": platform,
                }
            )
            for marker in markers
        )
    }


def _serves(tag: Tag, minor: int, platform: str) -> bool:
    """Tell whether a wheel with this tag installs on CPython 3.`minor` on `platform`."""
    if not tag.platform.startswith((*_PLATFORMS[platform], "any")):
        return False
    if tag.interpreter.startswith("py3"):
        return True
    built = re.fullmatch(r"cp3(\d+)", tag.interpreter)
    if built is None:
        return False
    return int(built[1]) <= minor if tag.abi == "abi3" else int(built[1]) == minor


def _named_by_the_project() -> set[str]:
    """Return every package the project, its extras, or its dependency groups name."""
    pyproject = _load("pyproject.toml")
    project = pyproject["project"]
    lines = chain(
        project["dependencies"],
        *project["optional-dependencies"].values(),
        *pyproject["dependency-groups"].values(),
    )
    return {canonicalize_name(Requirement(line).name) for line in lines if isinstance(line, str)}


def test_the_lock_covers_the_pythons_the_classifiers_name() -> None:
    """Ensure the lockfile covers each Python that CI tests, and no later one."""
    classifiers = _load("pyproject.toml")["project"]["classifiers"]
    supported = {
        int(match[1])
        for classifier in classifiers
        if (match := re.fullmatch(r"Programming Language :: Python :: 3\.(\d+)", classifier))
    }

    assert {minor for minor, _ in _covered()} == supported


def test_every_wheel_only_requirement_has_a_wheel_wherever_the_lock_reaches() -> None:
    """Ensure no package the project names lacks a wheel for a Python or platform the lock covers."""
    named = _named_by_the_project()
    wheel_only = [
        package
        for package in _load("uv.lock")["package"]
        if package["name"] in named and "sdist" not in package and "wheels" in package
    ]
    missing = [
        f"{package['name']} {package['version']} on Python 3.{minor}, {platform}"
        for package in wheel_only
        for minor, platform in sorted(_covered())
        if not any(
            _serves(tag, minor, platform)
            for wheel in package["wheels"]
            for tag in parse_wheel_filename(wheel["url"].rsplit("/", 1)[1])[3]
        )
    ]

    assert [package["name"] for package in wheel_only], "No locked requirement ships wheels alone."
    assert missing == []
