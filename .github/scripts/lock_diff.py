# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Compare two uv lockfiles, and fail when a package moves to an older version.

    python .github/scripts/lock_diff.py BASE HEAD [--markdown FILE]

A package moves backward when a version the head lock adds is older than the
version it replaces. Where one package holds several versions, one for each
range of Python, the new versions are paired with the old ones in sorted order.
Where the count changes, a new version is held to the oldest one it replaces, or,
if it replaces none, to the oldest one the base lock holds. That catches a pinned
upgrade that one range of Python cannot install, which leaves that range on a
years-old release.

Either case exits with 1. A changed `revision` means another uv release wrote the
lock, which an older uv may not read, so it is a warning. `--markdown` writes a
table of every package whose versions change to FILE.

It reads the two files with the standard library alone, so nothing it installs
can run before it.
"""

import re
import sys
import tomllib
from pathlib import Path

_VERSION = re.compile(r"(\d+(?:\.\d+)*)(?:(a|b|rc)(\d+))?(?:\.post(\d+))?(?:\.dev(\d+))?")
"""The public version shapes PyPI packages publish, after PEP 440 normalization."""

_PHASES = ("a", "b", "rc")

VersionKey = tuple[tuple[int, ...], int, int, int, int]


def version_key(version: str) -> VersionKey | None:
    """Return a key that sorts versions as PEP 440 does, or None for another shape.

    A development release sorts before its pre-release, a pre-release before its
    release, and a post-release after it.
    """
    match = _VERSION.fullmatch(version)
    if match is None:
        return None
    release = tuple(int(part) for part in match[1].split("."))
    while len(release) > 1 and release[-1] == 0:
        release = release[:-1]
    if match[2]:
        phase = _PHASES.index(match[2])
    elif match[5] is not None and match[4] is None:
        phase = -1
    else:
        phase = len(_PHASES)
    pre = int(match[3] or 0)
    post = int(match[4]) if match[4] is not None else -1
    dev = int(match[5]) if match[5] is not None else sys.maxsize
    return release, phase, pre, post, dev


def read_lock(path: Path) -> tuple[int | None, dict[str, set[str]]]:
    """Return a lockfile's `revision` and the versions it holds for each package."""
    with path.open("rb") as handle:
        lock = tomllib.load(handle)
    versions: dict[str, set[str]] = {}
    for package in lock.get("package", []):
        if "version" in package:
            versions.setdefault(package["name"], set()).add(package["version"])
    return lock.get("revision"), versions


def _older(new: str, old: str) -> bool:
    """Tell whether `new` sorts before `old`, and False when either shape is unknown."""
    new_key, old_key = version_key(new), version_key(old)
    return new_key is not None and old_key is not None and new_key < old_key


def _sorted(versions: set[str]) -> list[str]:
    """Sort versions oldest first, with any shape `version_key` cannot read last."""
    return sorted(
        versions,
        key=lambda version: (
            version_key(version) is None,
            version_key(version) or ((), 0, 0, 0, 0),
            version,
        ),
    )


def backward(base: dict[str, set[str]], head: dict[str, set[str]]) -> list[str]:
    """Return one sentence for each version the head lock adds below what it replaces."""
    problems: list[str] = []
    for name in sorted(head.keys() & base.keys()):
        added = _sorted(head[name] - base[name])
        removed = _sorted(base[name] - head[name])
        if removed and len(removed) == len(added):
            pairs = list(zip(added, removed, strict=True))
        else:
            floor = (removed or _sorted(base[name]))[0]
            pairs = [(version, floor) for version in added]
        problems.extend(
            f"{name} {new} is older than {old}, which the base lock holds."
            for new, old in pairs
            if _older(new, old)
        )
    return problems


def changes(base: dict[str, set[str]], head: dict[str, set[str]]) -> list[tuple[str, str, str]]:
    """Return each package whose versions differ, with its old and new versions."""
    return [
        (
            name,
            ", ".join(_sorted(base.get(name, set()))) or "-",
            ", ".join(_sorted(head.get(name, set()))) or "-",
        )
        for name in sorted(base.keys() | head.keys())
        if base.get(name, set()) != head.get(name, set())
    ]


def markdown(rows: list[tuple[str, str, str]]) -> str:
    """Return the changes as a Markdown table."""
    lines = ["| Package | From | To |", "| :--- | :--- | :--- |"]
    lines.extend(f"| {name} | {old} | {new} |" for name, old, new in rows)
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    """Compare the two lockfiles `argv` names, and return the exit code."""
    if len(argv) not in (2, 4) or (len(argv) == 4 and argv[2] != "--markdown"):
        print("usage: lock_diff.py BASE HEAD [--markdown FILE]", file=sys.stderr)
        return 2
    base_revision, base = read_lock(Path(argv[0]))
    head_revision, head = read_lock(Path(argv[1]))
    rows = changes(base, head)
    for name, old, new in rows:
        print(f"{name}: {old} -> {new}")
    if len(argv) == 4:
        Path(argv[3]).write_text(markdown(rows), encoding="utf-8")
    if head_revision != base_revision:
        print(
            f"::warning::The lockfile's revision moves from {base_revision} to {head_revision}, "
            "so another uv release wrote it. An older uv may not read it."
        )
    problems = backward(base, head)
    for problem in problems:
        print(f"::error::{problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
