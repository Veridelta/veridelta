# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests that hold the pushdown compiler to its golden SQL, dialect by dialect."""

import difflib

import pytest

from tests.unit.sql_corpus import CASES, FIXTURES, STATEMENTS, Case, escape, fixture, render
from veridelta.connectors.sql import SQLDialect, SQLPushdownCompiler

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_REGENERATE = "uv run python -m tests.unit.sql_corpus"


class TestGoldenSql:
    """Validate that every case compiles to exactly the SQL its golden file holds."""

    @pytest.mark.parametrize("dialect", list(SQLDialect), ids=lambda dialect: dialect.value)
    @pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
    def test_the_compiler_writes_the_golden_sql(self, case: Case, dialect: SQLDialect) -> None:
        """Ensure a compiler change shows as a diff of the SQL, rewritten on purpose or not at all."""
        path = fixture(case, dialect)
        expected = path.read_text(encoding="utf-8") if path.exists() else ""
        actual = render(case, dialect)

        if actual != expected:
            diff = "\n".join(
                difflib.unified_diff(
                    expected.splitlines(),
                    actual.splitlines(),
                    fromfile=str(path.relative_to(FIXTURES.parent.parent.parent)),
                    tofile="compiled now",
                    lineterm="",
                )
            )
            pytest.fail(
                f"The compiled SQL differs. If the change is meant, run {_REGENERATE}.\n{diff}"
            )


class TestCorpusCoverage:
    """Validate that the golden files cover every dialect, case, and compiler method."""

    def test_each_dialect_has_one_file_per_case_and_no_other(self) -> None:
        """Ensure a new dialect gets its files and a removed case leaves none behind."""
        folders = {path.name for path in FIXTURES.iterdir() if path.is_dir()}
        assert folders == {dialect.value for dialect in SQLDialect}
        for dialect in SQLDialect:
            files = {path.name for path in (FIXTURES / dialect.value).glob("*")}
            assert files == {f"{case.name}.sql" for case in CASES}, dialect.value

    def test_every_compiler_method_has_a_statement_label(self) -> None:
        """Ensure a new compile method is named here, so its SQL joins the golden files."""
        methods = {name for name in dir(SQLPushdownCompiler) if name.startswith("compile_")}

        assert methods == set(STATEMENTS)

    @pytest.mark.parametrize("dialect", list(SQLDialect), ids=lambda dialect: dialect.value)
    def test_every_statement_kind_is_compiled_or_refused(self, dialect: SQLDialect) -> None:
        """Ensure each statement a run can write appears in at least one golden file."""
        text = "\n".join(render(case, dialect) for case in CASES)

        for label in set(STATEMENTS.values()):
            assert f"-- statement: {label}" in text, label

    def test_case_names_are_unique(self) -> None:
        """Ensure no two cases write the same file."""
        names = [case.name for case in CASES]

        assert len(names) == len(set(names))


class TestEscape:
    """Validate how a statement is written so that editors and git leave it alone."""

    def test_it_writes_whitespace_and_unprintable_characters_by_code_point(self) -> None:
        """Ensure tabs, line breaks, and Unicode spaces become visible, and spaces stay."""
        assert (
            escape("a\tb\r\nc\u00a0d e\u0007") == "a⟨U+0009⟩b⟨U+000D⟩⟨U+000A⟩c⟨U+00A0⟩d e⟨U+0007⟩"
        )

    def test_it_leaves_printable_text_alone(self) -> None:
        """Ensure quotes, backslashes, and accented letters read as they are."""
        assert escape("'O''Brien' \\1 é 🚀") == "'O''Brien' \\1 é 🚀"
