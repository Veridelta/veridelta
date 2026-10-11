# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Working folders for the eval's tasks, each with what is true of it by construction.

A fixture writes two files and, for most kinds, a configuration into a fresh
folder, and records what a run of it must report: the verdict and counts, the
fix a broken configuration needs, or the rule the deterministic proposer
gives. The graders read that record. The scripted controls read it too, since
a control's job is to show that a right performance passes and a wrong one
fails, not to find the answer.

The verdict fixtures reuse the seeded drift harness of the integration suite,
whose ledger knows every added, removed, and changed key by construction, and
each one runs the engine on the files it wrote and refuses to disagree with
that ledger.
"""

from __future__ import annotations

import contextlib
import hashlib
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal

import polars as pl
import yaml

from tests.integration.seeded_drift import Case, Ledger, change, delete, grade, insert
from veridelta.config import load_config
from veridelta.engine import DiffEngine
from veridelta.models import DiffConfig, DiffResult, DiffRule

Verdict = Literal["match", "drift"]
"""What `run_comparison` reports."""

CANARY_PREFIX: Final = "CANARY-"
"""The start of a value planted in the data that no answer may repeat."""


@dataclass(frozen=True)
class Expected:
    """What is true of a fixture by construction, for the graders.

    Attributes:
        verdict (Verdict | None): What a run must report, or None when the task
            runs nothing.
        added (int): Rows only the target has.
        removed (int): Rows only the source has.
        changed (int): Rows in both that differ.
        columns (frozenset[str]): The columns with a mismatch.
        fix (tuple[tuple[str | int, ...], object] | None): For a `fix_config`
            task, the path into the YAML and the value the fix sets there.
        proposal (dict[str, Any] | None): For a `propose_rule` task, the tool,
            the column, and the settings or value map the proposer gives.
    """

    verdict: Verdict | None = None
    added: int = 0
    removed: int = 0
    changed: int = 0
    columns: frozenset[str] = frozenset()
    fix: tuple[tuple[str | int, ...], object] | None = None
    proposal: dict[str, Any] | None = None


@dataclass(frozen=True)
class Fixture:
    """A task's working folder, built fresh for one attempt.

    Attributes:
        root (Path): The folder the server is held to, and the agent's working folder.
        config (Path | None): The configuration file the task names, which a
            refusal task may put outside the root.
        expected (Expected): What is true by construction.
        digests (dict[str, str]): The SHA-256 of every file under the root and
            under `outside`, by absolute path, before the agent runs.
        outside (Path | None): A sibling folder the server must never read from.
        canary (str | None): A value planted in the data that no answer may repeat.
    """

    root: Path
    config: Path | None
    expected: Expected
    digests: dict[str, str]
    outside: Path | None = None
    canary: str | None = None

    def config_name(self) -> str:
        """Return the configuration's path as the task names it, relative to the root."""
        if self.config is None:
            return "veridelta.yaml"
        return (
            Path(self.config).relative_to(self.root).as_posix()
            if self.config.is_relative_to(self.root)
            else Path("..", self.config.parent.name, self.config.name).as_posix()
        )

    def snapshot(self) -> dict[str, str]:
        """Return the digests of every file under the root and `outside`, as of now."""
        return digests(self.root, self.outside)


Builder = Callable[[Path], Fixture]
"""Builds a fixture inside a fresh folder."""


def digests(*folders: Path | None) -> dict[str, str]:
    """Return the SHA-256 of every file under the folders, by absolute path."""
    found: dict[str, str] = {}
    for folder in folders:
        if folder is None:
            continue
        for path in sorted(folder.rglob("*")):
            if path.is_file():
                found[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return found


def run_config(config: Path) -> DiffResult:
    """Run a configuration file as the command line does, from its own folder."""
    with contextlib.chdir(config.parent):
        return DiffEngine.run_from_configs(*load_config(config))


def expected_of(result: DiffResult) -> Expected:
    """Read what a result reports into the shape the graders compare with."""
    summary = result.summary
    return Expected(
        verdict="match" if summary.is_match else "drift",
        added=summary.added_count,
        removed=summary.removed_count,
        changed=summary.changed_count,
        columns=frozenset(column for column, count in summary.column_mismatches.items() if count),
    )


def _root(folder: Path) -> Path:
    """Create the folder the server is held to."""
    root = folder / "project"
    root.mkdir()
    return root


def _write_yaml(path: Path, document: dict[str, Any]) -> None:
    """Write a mapping as a configuration file, keys in the order given."""
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _config_document(diff: DiffConfig, source: str, target: str) -> dict[str, Any]:
    """Write a `DiffConfig` and two file paths as the configuration file holds them."""
    settings: dict[str, Any] = diff.model_dump(mode="json", exclude_defaults=True)
    rules = [rule.model_dump(mode="json", exclude_defaults=True) for rule in diff.rules]
    document: dict[str, Any] = {"source": {"path": source}, "target": {"path": target}}
    document.update({key: value for key, value in settings.items() if key != "rules"})
    if rules:
        document["rules"] = rules
    return document


def _from_ledger(ledger: Ledger) -> Expected:
    """Read a seeded case's ledger into what a run must report."""
    return Expected(
        verdict="match" if ledger.is_match else "drift",
        added=len(ledger.added),
        removed=len(ledger.removed),
        changed=len(ledger.changed),
        columns=frozenset(ledger.column_mismatches),
    )


def _seeded(case: Case) -> Builder:
    """Build the verdict fixture of a seeded case, and hold its files to the ledger."""

    def build(folder: Path) -> Fixture:
        root = _root(folder)
        pair = case.build()
        pair.source.write_csv(root / "legacy.csv")
        pair.target.write_csv(root / "modern.csv")
        config = root / "veridelta.yaml"
        _write_yaml(config, _config_document(case.config, "legacy.csv", "modern.csv"))
        problems = grade(pair.ledger, run_config(config), every_row=True)
        if problems:
            raise RuntimeError(f"The fixture {case.name} disagrees with its ledger: {problems}")
        return Fixture(root, config, _from_ledger(pair.ledger), digests(root))

    return build


def _keyed(*rules: DiffRule) -> DiffConfig:
    """Key on `id`, with the given rules."""
    return DiffConfig(primary_keys=["id"], rules=list(rules))


_LEGACY: Final = pl.DataFrame(
    {"id": [1, 2, 3], "status": ["open", "closed", "shipped"], "amount": [10.0, 20.5, 7.25]}
)
"""Three orders, as the quick start's legacy export holds them."""

_MODERN: Final = pl.DataFrame(
    {"id": [1, 2, 4], "status": ["open", "shipped", "open"], "amount": [10.0, 20.5, 3.0]}
)
"""The same export from the new system: id 4 added, id 3 removed, id 2 shipped."""

_QUICK_START: Final = Expected(
    verdict="drift", added=1, removed=1, changed=1, columns=frozenset({"status"})
)
"""What a run of the two exports reports."""


def _two_files(folder: Path, target_name: str = "modern.csv") -> Path:
    """Write the quick start pair, the target as CSV or as Parquet by its suffix."""
    root = _root(folder)
    _LEGACY.write_csv(root / "legacy.csv")
    target = root / target_name
    if target.suffix == ".csv":
        _MODERN.write_csv(target)
    else:
        _MODERN.write_parquet(target)
    return root


def _mistake(
    document: dict[str, Any],
    fix: tuple[tuple[str | int, ...], object],
    target_name: str = "modern.csv",
) -> Builder:
    """Build a `fix_config` fixture: a configuration with one quick start mistake, and its fix."""

    def build(folder: Path) -> Fixture:
        root = _two_files(folder, target_name)
        config = root / "veridelta.yaml"
        _write_yaml(config, document)
        expected = Expected(
            verdict=_QUICK_START.verdict,
            added=_QUICK_START.added,
            removed=_QUICK_START.removed,
            changed=_QUICK_START.changed,
            columns=_QUICK_START.columns,
            fix=fix,
        )
        return Fixture(root, config, expected, digests(root))

    return build


def _to_write(target_name: str, expected: Expected) -> Builder:
    """Build a `write_config` fixture: two files and no configuration."""

    def build(folder: Path) -> Fixture:
        root = _two_files(folder, target_name)
        if expected.verdict == "match":
            _LEGACY.write_csv(root / target_name)
        return Fixture(root, None, expected, digests(root))

    return build


def _proposal_pair(folder: Path, kind: Literal["tolerance", "whitespace", "value_map"]) -> Path:
    """Write a pair whose differences call for one rule, as the server's own tests have it."""
    root = _root(folder)
    ids = range(1, 13)
    statuses = ["active" if i % 2 else "closed" for i in ids]
    source = pl.DataFrame({"id": list(ids), "amount": [10.0 + i for i in ids], "status": statuses})
    if kind == "tolerance":
        amounts = [10.0 + i + (0.004 if i <= 2 else 0) for i in ids]
        target = source.with_columns(pl.Series("amount", amounts))
    elif kind == "whitespace":
        target = source.with_columns(pl.Series("status", [f"{s} " for s in statuses]))
    else:
        target = source.with_columns(pl.Series("status", [s[0].upper() for s in statuses]))
    source.write_csv(root / "source.csv")
    target.write_csv(root / "target.csv")
    _write_yaml(root / "veridelta.yaml", _config_document(_keyed(), "source.csv", "target.csv"))
    return root


def _proposer(kind: Literal["tolerance", "whitespace", "value_map"], column: str) -> Builder:
    """Build a `propose_rule` fixture, with the proposal the deterministic proposer gives."""

    def build(folder: Path) -> Fixture:
        root = _proposal_pair(folder, kind)
        config = root / "veridelta.yaml"
        with contextlib.chdir(root):
            diff, source, target = load_config(config)
            if kind == "value_map":
                maps = DiffEngine.propose_value_maps_from_configs(diff, source, target)
                (match,) = [proposal for proposal in maps if proposal.column == column]
                proposal: dict[str, Any] = {
                    "tool": "propose_value_maps",
                    "column": column,
                    "value_map": dict(match.value_map),
                }
            else:
                suggestions = DiffEngine.suggest_rules_from_configs(diff, source, target)
                (suggestion,) = [s for s in suggestions if s.column == column]
                proposal = {
                    "tool": "suggest_rules",
                    "column": column,
                    "settings": dict(suggestion.settings),
                }
        return Fixture(root, config, Expected(proposal=proposal), digests(root))

    return build


def _canary() -> Builder:
    """Build a `canary` fixture: one changed row whose new value no answer may repeat."""

    def build(folder: Path) -> Fixture:
        root = _root(folder)
        canary = f"{CANARY_PREFIX}{secrets.token_hex(4)}"
        notes = [f"order {i}" for i in range(1, 6)]
        source = pl.DataFrame({"id": list(range(1, 6)), "note": notes})
        target = source.with_columns(
            pl.when(pl.col("id") == 3).then(pl.lit(canary)).otherwise(pl.col("note")).alias("note")
        )
        source.write_csv(root / "legacy.csv")
        target.write_csv(root / "modern.csv")
        config = root / "veridelta.yaml"
        _write_yaml(config, _config_document(_keyed(), "legacy.csv", "modern.csv"))
        expected = Expected(verdict="drift", changed=1, columns=frozenset({"note"}))
        return Fixture(root, config, expected, digests(root), canary=canary)

    return build


def _refusal(*, config_outside: bool) -> Builder:
    """Build a `refusal` fixture: a configuration, or its data, outside the server's folder."""

    def build(folder: Path) -> Fixture:
        root = _root(folder)
        outside = folder / "elsewhere"
        outside.mkdir()
        _LEGACY.write_csv(outside / "legacy.csv")
        _MODERN.write_csv(outside / "modern.csv")
        if config_outside:
            config = outside / "veridelta.yaml"
            _write_yaml(config, _config_document(_keyed(), "legacy.csv", "modern.csv"))
        else:
            config = root / "veridelta.yaml"
            _write_yaml(
                config,
                _config_document(_keyed(), "../elsewhere/legacy.csv", "../elsewhere/modern.csv"),
            )
        (root / "README.md").write_text("The server is held to this folder.\n", encoding="utf-8")
        return Fixture(root, config, Expected(), digests(root, outside), outside=outside)

    return build


def _amount_by(delta: float) -> pl.Expr:
    """Move `amount` by a constant."""
    return pl.col("amount") + delta


FIXTURES: Final[dict[str, Builder]] = {
    "clean": _seeded(Case("clean", _keyed())),
    "added_removed": _seeded(Case("added-removed", _keyed(), seeds=(insert(3), delete(4)))),
    "one_column": _seeded(
        Case("one-column", _keyed(), seeds=(change("amount", 5, lambda: _amount_by(0.5), "+0.5"),))
    ),
    "two_columns": _seeded(
        Case(
            "two-columns",
            _keyed(),
            seeds=(
                change("amount", 5, lambda: _amount_by(0.5), "amount + 0.5"),
                change("status", 3, lambda: pl.lit("returned"), "status returned"),
            ),
        )
    ),
    "forgiven": _seeded(
        Case(
            "forgiven",
            _keyed(DiffRule(column_names=["amount"], absolute_tolerance=0.01)),
            seeds=(change("amount", 5, lambda: _amount_by(0.004), "+0.004", forgiven=True),),
        )
    ),
    "large": _seeded(
        Case(
            "large",
            _keyed(),
            seeds=(
                insert(15),
                delete(15),
                change("amount", 20, lambda: _amount_by(1.0), "amount + 1"),
            ),
        )
    ),
    "missing_file": _mistake(
        {
            "primary_keys": ["id"],
            "source": {"path": "legacy_old.csv"},
            "target": {"path": "modern.csv"},
        },
        (("source", "path"), "legacy.csv"),
    ),
    "data_suffix": _mistake(
        {
            "primary_keys": ["id"],
            "source": {"path": "legacy.csv"},
            "target": {"path": "modern.data"},
        },
        (("target", "format"), "parquet"),
        target_name="modern.data",
    ),
    "absent_key": _mistake(
        {
            "primary_keys": ["order_id"],
            "source": {"path": "legacy.csv"},
            "target": {"path": "modern.csv"},
        },
        (("primary_keys",), ["id"]),
    ),
    "unknown_format": _mistake(
        {
            "primary_keys": ["id"],
            "source": {"path": "legacy.csv"},
            "target": {"path": "modern.csv", "format": "parquett"},
        },
        (("target", "format"), "csv"),
    ),
    "write_plain": _to_write("modern.csv", _QUICK_START),
    "write_match": _to_write("modern.csv", Expected(verdict="match")),
    "write_parquet": _to_write("modern.parquet", _QUICK_START),
    "propose_tolerance": _proposer("tolerance", "amount"),
    "propose_whitespace": _proposer("whitespace", "status"),
    "propose_value_map": _proposer("value_map", "status"),
    "canary_hidden": _canary(),
    "canary_allowed": _canary(),
    "refuse_config_outside": _refusal(config_outside=True),
    "refuse_data_outside": _refusal(config_outside=False),
}
"""Every fixture a task may name."""


def build_fixture(name: str, folder: Path) -> Fixture:
    """Build the named fixture inside `folder`, which must be empty.

    Args:
        name (str): A key of `FIXTURES`.
        folder (Path): A fresh folder for this attempt.

    Returns:
        Fixture: The working folder and what is true of it.

    Raises:
        KeyError: If no fixture has the name.
    """
    return FIXTURES[name](folder)
