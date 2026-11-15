# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for how text from the data is written for a terminal or a CI log."""

from datetime import date, datetime
from decimal import Decimal

import pytest
import yaml

from veridelta._terminal import printable, printable_text, printable_yaml

pytestmark = [pytest.mark.unit, pytest.mark.fast]


def _acted_on(text: str) -> list[str]:
    """Return the lines a terminal or a GitHub runner would act on."""
    return [
        line
        for line in text.split("\n")
        if "##[" in line
        or line.lstrip().startswith("::")
        or any(not char.isprintable() and char != "\t" for char in line)
    ]


class TestPrintable:
    """Validate how a column name or a value from the data is written."""

    @pytest.mark.parametrize(
        "name", ["amount", "Amount total", "_id", "2024 sales", "prix (€)", "naïve", "a::b"]
    )
    def test_it_writes_a_plain_name_as_it_is(self, name: str) -> None:
        """Ensure an ordinary header reads as it always has."""
        assert printable(name) == name

    @pytest.mark.parametrize(
        ("name", "written"),
        [
            pytest.param("a\nb", "'a\\nb'", id="line-break"),
            pytest.param("a\x1b[31m", "'a\\x1b[31m'", id="escape"),
            pytest.param("a\tb", "'a\\tb'", id="tab"),
            pytest.param("a\x9bb", "'a\\x9bb'", id="c1-control"),
            pytest.param("a\u2028b", "'a\\u2028b'", id="line-separator"),
            pytest.param("::error::x", "'::error::x'", id="workflow-command"),
            pytest.param(" ::x", "' ::x'", id="leading-space"),
            pytest.param("'a'", "\"'a'\"", id="quoted"),
            pytest.param("", "''", id="empty"),
            pytest.param("amt ##[error]x", "'amt ##\\x5berror]x'", id="legacy-command"),
            pytest.param("a###[b##[c", "'a###\\x5bb##\\x5bc'", id="legacy-command-twice"),
            pytest.param("a\n##[x", "'a\\n##\\x5bx'", id="legacy-command-on-a-new-line"),
        ],
    )
    def test_it_quotes_any_other_name(self, name: str, written: str) -> None:
        """Ensure a name that could act on a terminal or a CI log is written as its repr."""
        assert printable(name) == written
        assert not _acted_on(written)

    @pytest.mark.parametrize(
        ("name", "written"),
        [
            pytest.param("amount", "'amount'", id="plain"),
            pytest.param("a\nb", "'a\\nb'", id="line-break"),
            pytest.param("x ##[add-mask]y", "'x ##\\x5badd-mask]y'", id="legacy-command"),
        ],
    )
    def test_it_quotes_a_plain_name_when_asked(self, name: str, written: str) -> None:
        """Ensure a note that quotes every name writes each one the same way."""
        assert printable(name, quote=True) == written

    def test_it_keeps_a_name_that_spells_the_escape_apart(self) -> None:
        r"""Ensure a name holding the text `\x5b` reads differently from a broken `##[`."""
        assert printable("a ##\\x5bb") == "a ##\\x5bb"
        assert printable("a ##\\x5bb", quote=True) == "'a ##\\\\x5bb'"
        assert printable("a ##[b", quote=True) == "'a ##\\x5bb'"

    @pytest.mark.parametrize(
        ("value", "written"),
        [
            pytest.param(3, "3", id="integer"),
            pytest.param(None, "None", id="null"),
            pytest.param(Decimal("1.50"), "Decimal('1.50')", id="decimal"),
            pytest.param(date(2026, 1, 2), "datetime.date(2026, 1, 2)", id="date"),
            pytest.param(
                datetime(2026, 1, 2, 3, 4), "datetime.datetime(2026, 1, 2, 3, 4)", id="datetime"
            ),
            pytest.param(b"k ##[error]y", "b'k ##\\x5berror]y'", id="binary"),
            pytest.param(b"k\n::error::y\x1b", "b'k\\n::error::y\\x1b'", id="binary-break"),
            pytest.param(["k ##[error]y"], "['k ##\\x5berror]y']", id="list"),
            pytest.param({"a": "k\n##[x"}, "{'a': 'k\\n##\\x5bx'}", id="struct"),
        ],
    )
    def test_it_writes_a_value_of_any_type_as_its_repr(self, value: object, written: str) -> None:
        """Ensure a key of any dtype is written as its repr, with no `##[` left in it."""
        assert printable(value) == written
        assert printable(value, quote=True) == written
        assert not _acted_on(written)

    def test_it_escapes_a_repr_that_holds_a_control_character(self) -> None:
        """Ensure a value whose repr is not printable cannot print a line of its own."""

        class Odd:
            def __repr__(self) -> str:
                return "odd\n::error::x\x1b"

        assert printable(Odd()) == "odd\\n::error::x\\x1b"


class TestPrintableText:
    """Validate how a message that can quote the data, such as an error, is written."""

    def test_it_keeps_a_plain_message_as_it_is(self) -> None:
        """Ensure an ordinary error reads as it always has, line breaks and tabs included."""
        message = "\nConfiguration Error\nThe key 'id' is missing.\n\tSee the docs: x::y."
        assert printable_text(message) == message

    @pytest.mark.parametrize(
        ("message", "written"),
        [
            pytest.param("a ##[error]b", "a ##\\x5berror]b", id="legacy-command"),
            pytest.param("a\x1b[31mb\rc", "a\\x1b[31mb\\rc", id="escape-and-return"),
            pytest.param("a\u2028b\x85c", "a\\u2028b\\x85c", id="unicode-breaks"),
            pytest.param("a\n::error::b", "a\n\\x3a:error::b", id="workflow-command"),
            pytest.param("a\n  \t::warning::b", "a\n  \t\\x3a:warning::b", id="indented"),
            pytest.param("::error::b", "\\x3a:error::b", id="first-line"),
        ],
    )
    def test_it_escapes_what_a_terminal_or_a_runner_acts_on(
        self, message: str, written: str
    ) -> None:
        """Ensure a message keeps its lines but loses every command and control character."""
        assert printable_text(message) == written
        assert not _acted_on(written)

    def test_it_leaves_text_printable_already_wrote_alone(self) -> None:
        """Ensure a name `printable` wrote reads the same once the line it sits on is printed."""
        name = "a ##[b\n::c"
        line = f"- {printable(name)}: 1 mismatch"
        assert printable_text(line) == line


class TestPrintableYaml:
    """Validate the YAML `crosswalk` and `suggest` print, which a person pastes."""

    def test_it_dumps_plain_rules_as_yaml_does(self) -> None:
        """Ensure an ordinary rule prints as before, codes like Y and 1 quoted."""
        data = {"rules": [{"column_names": ["gender"], "value_map": {"M": "Male", "Y": "1"}}]}
        assert printable_yaml(data) == yaml.safe_dump(data, sort_keys=False)

    @pytest.mark.parametrize(
        "text",
        [
            pytest.param("::error::x", id="workflow-command"),
            pytest.param("a\n::error::x", id="line-break"),
            pytest.param("amt ##[error]x", id="legacy-command"),
            pytest.param("é ##[x\x1b[31m", id="unicode-and-escape"),
            pytest.param("x" * 70 + " ::error::y", id="long-line"),
            pytest.param("a ##\\x5bb", id="spelled-escape"),
        ],
    )
    def test_it_reads_back_and_holds_nothing_a_runner_acts_on(self, text: str) -> None:
        """Ensure a name or value from the data, as key or value, loads back as it was."""
        data = {"rules": [{"column_names": [text], "value_map": {text: text, "M": text}}]}

        dumped = printable_yaml(data)

        assert yaml.safe_load(dumped) == data
        assert not _acted_on(dumped.removesuffix("\n"))
