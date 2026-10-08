# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold every verdict to drift seeded on purpose, in a local run and in pushdown.

Each case seeds a clean order table with drift whose effect is known, and
`seeded_drift.grade` compares the run's summary, and in a local run every
differing key, with that ledger. The cases come in pairs where they can: the
same drift once with no rule, where it must be reported, and once under the
rule meant to forgive it, where it must not be.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest
import yaml

from tests.integration.duckdb_harness import run_local, run_pushdown
from tests.integration.seeded_drift import (
    LEGACY_FORMAT,
    Case,
    Ledger,
    change,
    delete,
    drop_target,
    duplicate_key,
    grade,
    insert,
    legacy_dates,
    null_both,
    rename_target,
    sentinel,
)
from veridelta.cli import EXIT_ERROR, EXIT_MATCH, EXIT_MISMATCH, build_parser, run
from veridelta.exceptions import ConfigError, DataIntegrityError
from veridelta.models import DiffConfig, DiffRule

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_REFUSED_PARSE = pytest.mark.skip_on(
    "postgres",
    reason="Postgres refuses datetime_format pushdown: it has no parse that yields NULL.",
)


def _config(*rules: DiffRule, **settings: object) -> DiffConfig:
    """Key on `id`, with the given rules and settings."""
    return DiffConfig.model_validate({"primary_keys": ["id"], "rules": list(rules), **settings})


def _amount_by(delta: float) -> pl.Expr:
    return pl.col("amount") + delta


CASES = [
    Case("clean", _config()),
    Case("deleted-rows", _config(), seeds=(delete(4),)),
    Case("inserted-rows", _config(), seeds=(insert(3),)),
    Case(
        "amount-past-absolute-tolerance",
        _config(DiffRule(column_names=["amount"], absolute_tolerance=0.01)),
        seeds=(change("amount", 5, lambda: _amount_by(0.5), "amount + 0.5"),),
    ),
    Case(
        "amount-within-absolute-tolerance",
        _config(DiffRule(column_names=["amount"], absolute_tolerance=0.01)),
        seeds=(change("amount", 5, lambda: _amount_by(0.004), "amount + 0.004", forgiven=True),),
    ),
    Case(
        "amount-within-relative-tolerance",
        _config(DiffRule(column_names=["amount"], relative_tolerance=0.01)),
        seeds=(
            change("amount", 5, lambda: pl.col("amount") * 1.005, "amount * 1.005", forgiven=True),
        ),
    ),
    Case(
        "amount-past-relative-tolerance",
        _config(DiffRule(column_names=["amount"], relative_tolerance=0.01)),
        seeds=(change("amount", 5, lambda: pl.col("amount") * 1.05, "amount * 1.05"),),
    ),
    Case(
        "quantity-off-by-one",
        _config(),
        seeds=(change("quantity", 4, lambda: pl.col("quantity") + 1, "quantity + 1"),),
    ),
    Case(
        "quantity-at-integer-tolerance",
        _config(DiffRule(column_names=["quantity"], absolute_tolerance=1.0)),
        seeds=(
            change("quantity", 4, lambda: pl.col("quantity") + 1, "quantity + 1", forgiven=True),
            change("quantity", 3, lambda: pl.col("quantity") + 2, "quantity + 2"),
        ),
    ),
    Case(
        "status-recased",
        _config(),
        seeds=(change("status", 6, lambda: pl.col("status").str.to_uppercase(), "upper case"),),
    ),
    Case(
        "status-recased-under-case-insensitive",
        _config(DiffRule(column_names=["status"], case_insensitive=True)),
        seeds=(
            change(
                "status",
                6,
                lambda: pl.col("status").str.to_uppercase(),
                "upper case",
                forgiven=True,
            ),
        ),
    ),
    Case(
        "region-padded",
        _config(),
        seeds=(change("region", 5, lambda: "  " + pl.col("region") + " ", "padded"),),
    ),
    Case(
        "region-padded-under-whitespace-both",
        _config(DiffRule(column_names=["region"], whitespace_mode="both")),
        seeds=(
            change("region", 5, lambda: "  " + pl.col("region") + " ", "padded", forgiven=True),
        ),
    ),
    Case(
        "region-padded-under-whitespace-left",
        _config(DiffRule(column_names=["region"], whitespace_mode="left")),
        seeds=(
            change("region", 3, lambda: "  " + pl.col("region"), "padded left", forgiven=True),
            change("region", 4, lambda: pl.col("region") + " ", "padded right"),
        ),
    ),
    Case(
        "note-sentinel",
        _config(),
        seeds=(sentinel("note", 4, "N/A", forgiven=False),),
    ),
    Case(
        "note-sentinel-under-null-values",
        _config(DiffRule(column_names=["note"], null_values=["N/A"])),
        seeds=(sentinel("note", 4, "N/A", forgiven=True),),
    ),
    Case(
        "note-nulled-in-target",
        _config(),
        seeds=(change("note", 3, lambda: pl.lit(None, dtype=pl.String()), "NULL in target"),),
    ),
    Case(
        "note-null-on-both-sides",
        _config(),
        seeds=(null_both("note", 5, forgiven=True),),
    ),
    Case(
        "note-null-on-both-sides-not-equal",
        _config(default_treat_null_as_equal=False),
        seeds=(null_both("note", 5, forgiven=False),),
    ),
    Case(
        "status-edited",
        _config(),
        seeds=(change("status", 4, lambda: pl.lit("cancelled"), "set to cancelled"),),
    ),
    Case(
        "status-edited-but-ignored",
        _config(DiffRule(column_names=["status"], ignore=True)),
        seeds=(
            change("status", 4, lambda: pl.lit("cancelled"), "set to cancelled", forgiven=True),
        ),
        ignored=frozenset({"status"}),
    ),
    Case(
        "region-renamed",
        _config(DiffRule(column_names=["region"], rename_to="area")),
        seeds=(change("region", 3, lambda: pl.lit("central"), "set to central"),),
        reshapes=(rename_target("region", "area"),),
    ),
    Case(
        "note-dropped-from-target",
        _config(schema_mode="allow_removals"),
        seeds=(change("note", 3, lambda: pl.lit("edited"), "edited", forgiven=True),),
        reshapes=(drop_target("note"),),
    ),
    pytest.param(
        Case(
            "legacy-dates-parsed",
            _config(DiffRule(column_names=["placed_at"], datetime_format=LEGACY_FORMAT)),
            seeds=(
                change(
                    "placed_at",
                    3,
                    lambda: pl.col("placed_at") + pl.duration(days=1),
                    "a day later",
                ),
            ),
            reshapes=(legacy_dates(forgiven=True),),
        ),
        marks=_REFUSED_PARSE,
        id="legacy-dates-parsed",
    ),
    Case(
        "legacy-dates-unparsed",
        _config(),
        reshapes=(legacy_dates(forgiven=False),),
        pushdown_refuses="placed_at",
    ),
    Case(
        "everything-at-once",
        _config(
            DiffRule(column_names=["amount"], absolute_tolerance=0.01),
            DiffRule(column_names=["region"], whitespace_mode="both"),
        ),
        seeds=(
            delete(3),
            insert(2),
            change("amount", 4, lambda: _amount_by(1.25), "amount + 1.25"),
            change(
                "status",
                2,
                lambda: pl.lit("cancelled"),
                "set to cancelled on two of the amount rows",
                same_rows_as=2,
            ),
            change("status", 3, lambda: pl.lit("cancelled"), "set to cancelled"),
            change("amount", 6, lambda: _amount_by(0.003), "amount + 0.003", forgiven=True),
            change("region", 5, lambda: " " + pl.col("region"), "padded", forgiven=True),
            change("quantity", 2, lambda: pl.col("quantity") * 10, "quantity * 10"),
        ),
    ),
]
"""Every graded case. Each id names the drift and the rule, if any, meant to forgive it."""


def _case_id(case: object) -> str:
    return case.name if isinstance(case, Case) else str(case)


@pytest.mark.parametrize("case", CASES, ids=_case_id)
def test_a_local_run_reports_exactly_the_seeded_drift(case: Case) -> None:
    """Ensure a local run reports every seeded difference, every key, and nothing else."""
    pair = case.build()

    result = run_local(case.config, pair.source, pair.target)

    assert grade(pair.ledger, result, every_row=True) == []


@pytest.mark.parametrize("case", CASES, ids=_case_id)
def test_pushdown_reports_exactly_the_seeded_drift(case: Case) -> None:
    """Ensure pushdown reports the seeded counts, per column, and the verdict.

    Text against a timestamp is a pair only a local run converts, so pushdown
    refuses it before reading a row, naming the column.
    """
    pair = case.build()
    if case.pushdown_refuses is not None:
        with pytest.raises(ConfigError, match=f"'{case.pushdown_refuses}'"):
            run_pushdown(case.config, pair.source, pair.target)
        return

    result, statements = run_pushdown(case.config, pair.source, pair.target)

    assert grade(pair.ledger, result, every_row=False) == [], "\n".join(statements)


class TestRefusals:
    """Seeds no comparison may pair or schema it must refuse, in both engines."""

    def test_a_repeated_key_is_refused(self) -> None:
        """Ensure a source key that appears twice raises instead of pairing twice."""
        pair = Case("repeated-key", _config(), seeds=(duplicate_key(),)).build()

        with pytest.raises(DataIntegrityError):
            run_local(_config(), pair.source, pair.target)
        with pytest.raises(DataIntegrityError):
            run_pushdown(_config(), pair.source, pair.target)

    def test_a_dropped_column_fails_an_exact_schema(self) -> None:
        """Ensure `exact` refuses a target that lost a column, before reading rows."""
        config = _config(schema_mode="exact")
        pair = Case("dropped", config, reshapes=(drop_target("note"),)).build()

        with pytest.raises(ConfigError, match="note"):
            run_local(config, pair.source, pair.target)
        with pytest.raises(ConfigError, match="note"):
            run_pushdown(config, pair.source, pair.target)


@pytest.mark.only_on("duckdb", reason="The command line reads Parquet files, on any backend.")
class TestExitCodes:
    """The command line turns each kind of seeded pair into the exit code CI reads."""

    @pytest.mark.parametrize(
        ("case", "expected"),
        [
            (Case("clean", _config()), EXIT_MATCH),
            (Case("deleted", _config(), seeds=(delete(2),)), EXIT_MISMATCH),
            (
                Case(
                    "forgiven",
                    _config(DiffRule(column_names=["status"], case_insensitive=True)),
                    seeds=(
                        change(
                            "status",
                            3,
                            lambda: pl.col("status").str.to_uppercase(),
                            "upper case",
                            forgiven=True,
                        ),
                    ),
                ),
                EXIT_MATCH,
            ),
            (Case("repeated-key", _config(), seeds=(duplicate_key(),)), EXIT_ERROR),
        ],
        ids=lambda value: value.name if isinstance(value, Case) else str(value),
    )
    def test_run_exits_as_the_ledger_says(
        self, tmp_path: Path, case: Case, expected: int, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `veridelta run` exits 0 on a match, 1 on drift, and 3 on a refused pair."""
        pair = case.build()
        pair.source.write_parquet(tmp_path / "source.parquet")
        pair.target.write_parquet(tmp_path / "target.parquet")
        settings = case.config.model_dump(mode="json", exclude_none=True, exclude_defaults=True)
        config = tmp_path / "veridelta.yaml"
        config.write_text(
            yaml.safe_dump(
                {
                    "source": {"path": str(tmp_path / "source.parquet")},
                    "target": {"path": str(tmp_path / "target.parquet")},
                    **settings,
                }
            ),
            encoding="utf-8",
        )

        code = run(build_parser().parse_args(["run", "-c", str(config), "--quiet"]))

        capsys.readouterr()
        assert code == expected


class TestTheGraderCanFail:
    """The suite proves nothing unless a wrong ledger or a wrong verdict turns it red."""

    def test_a_ledger_that_misses_a_change_is_caught(self) -> None:
        """Ensure grading against a ledger with one change left out reports it."""
        case = Case(
            "edited",
            _config(),
            seeds=(change("status", 3, lambda: pl.lit("cancelled"), "set to cancelled"),),
        )
        pair = case.build()
        result = run_local(case.config, pair.source, pair.target)
        dropped = sorted(pair.ledger.changed)[0]
        short = Ledger(
            added=pair.ledger.added,
            removed=pair.ledger.removed,
            changed={key: cols for key, cols in pair.ledger.changed.items() if key != dropped},
            compared=pair.ledger.compared,
        )

        problems = grade(short, result, every_row=True)

        assert "changed_count: expected 2, got 3" in problems
        assert any(f"unexpected [{dropped}]" in problem for problem in problems)

    def test_a_tolerance_too_wide_is_caught(self) -> None:
        """Ensure a rule that forgives a change the ledger records turns the grade red."""
        case = Case(
            "too-wide",
            _config(),
            seeds=(change("amount", 4, lambda: _amount_by(0.5), "amount + 0.5"),),
        )
        pair = case.build()
        loose = _config(DiffRule(column_names=["amount"], absolute_tolerance=1.0))

        problems = grade(pair.ledger, run_local(loose, pair.source, pair.target), every_row=True)

        assert "changed_count: expected 4, got 0" in problems
        assert "is_match: expected False, got True" in problems
