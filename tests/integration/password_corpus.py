# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Wrong passwords of every character class, and a check that finds any piece of one in an answer.

`veridelta mcp` takes a database password, a Snowflake password, or a
Snowflake key's passphrase from its environment as free text, and masks it in
every answer only as written and in a few encodings. That is safe only while
no reader repeats a password in another form: with its quotes doubled, its
backslashes escaped, inside ODBC braces, as JSON, or cut. The tests that use
this module drive each reader with these passwords and hold every answer to
`holds_piece`.

Each password mixes letters in runs of two with the characters that a quoting
or an encoding changes, so an escaped echo keeps no run of 4 characters as
written, and the check looks for each escaped form whole. No password holds an
English word, or a run of 4 characters from the fixed text that Veridelta or a
driver writes.
"""

from __future__ import annotations

import base64
import html
import json
import re
import unicodedata
from typing import TYPE_CHECKING, Final
from urllib.parse import unquote

if TYPE_CHECKING:
    from collections.abc import Callable

_SEPARATORS: Final = (
    "'",
    '"',
    "\\",
    "%",
    "%41",
    "@",
    ":",
    "/",
    "?",
    "#",
    "&",
    "=",
    ";",
    "{",
    "}",
    "[",
    "]",
    "+",
    "~",
    "!",
    "$",
    "*",
    "(",
    ")",
    " ",
    "\t",
    "\n",
    "é",
    "日本",
    "Ω",
    "\U0001f600",
)
"""Every class of character a password can hold that a reader could quote or encode."""

_LETTERS: Final = "QZXJVKWqzxjvkw"
"""Letters that no English word in fixed text runs together, two at a time."""


def _pair(index: int) -> str:
    """Return the pair of letters that comes before the separator at `index`."""
    return _LETTERS[index % 14] + _LETTERS[(5 * index + 3) % 14]


def _joined(separators: tuple[str, ...], length: int | None = None) -> str:
    """Put a pair of letters around each separator, once, or cycling until `length`."""
    count = len(separators) if length is None else length
    text = "".join(_pair(i) + separators[i % len(separators)] for i in range(count))
    return text[:length] if length is not None else text + _pair(count)


CORPUS: Final[tuple[tuple[str, str], ...]] = (
    ("every-class", _joined(_SEPARATORS)),
    ("long", _joined(_SEPARATORS, 300)),
    ("backslash-heavy", "Qz\\Xv\\\\Kj\\nWq\\tZp\\'Jx\\\"Vb\\u00e9Qk\\x41Zw\\0Xj\\"),
)
"""Each wrong password a reader is driven with, by id.

`long` runs to 300 characters, so a reader that cuts a password would leave a
piece of it."""

_JSON_ESCAPES: Final = re.compile(r'(?:\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))+')
"""A run of JSON escapes, decoded together so a surrogate pair reads as one character."""

_WINDOW: Final = 4
"""The length of the shortest run of a password that counts as a piece of it."""


def _json_unescaped(text: str) -> str:
    """Decode each run of JSON escapes in `text`, and leave the rest as written."""

    def decoded(match: re.Match[str]) -> str:
        try:
            return str(json.loads(f'"{match.group()}"'))
        except ValueError:
            return match.group()

    return _JSON_ESCAPES.sub(decoded, text)


def _backslashed(password: str) -> str:
    """Escape each quote and backslash with a backslash, as C, MySQL, and JSON do."""
    return re.sub(r"(['\"\\])", r"\\\1", password)


_ESCAPES: Final[tuple[tuple[str, Callable[[str], str]], ...]] = (
    ("single quotes doubled", lambda value: value.replace("'", "''")),
    ("double quotes doubled", lambda value: value.replace('"', '""')),
    ("backslashes doubled", lambda value: value.replace("\\", "\\\\")),
    ("backslash-escaped", _backslashed),
    ("libpq quoted", lambda value: "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"),
    ("ODBC braced", lambda value: "{" + value.replace("}", "}}") + "}"),
    ("JSON in ASCII", lambda value: json.dumps(value)[1:-1]),
    ("JSON in UTF-8", lambda value: json.dumps(value, ensure_ascii=False)[1:-1]),
    ("repr", lambda value: repr(value)[1:-1]),
    ("NFD", lambda value: unicodedata.normalize("NFD", value)),
    ("UTF-8 read as Latin-1", lambda value: value.encode().decode("latin-1")),
    ("UTF-16LE in hex", lambda value: value.encode("utf-16-le").hex()),
    ("HTML-escaped", html.escape),
    ("XML-escaped", lambda value: html.escape(value, quote=False)),
    ("basic auth", lambda value: base64.b64encode(f"u:{value}".encode()).decode()),
)
"""Each whole escaped form of a password a reader could write, by name."""


def escaped_forms(password: str) -> dict[str, str]:
    """Return each whole escaped form of `password`, by name, as `holds_piece` looks for it."""
    return {name: escape(password) for name, escape in _ESCAPES}


def holds_piece(answer: str, password: str) -> str | None:
    """Say what piece of `password` an answer holds, or None when it holds none.

    The answer is read as written, casefolded, percent-decoded, and with its
    JSON escapes decoded. A piece is any run of 4 characters of the password,
    in any case, or a whole escaped form of it, such as the password with its
    quotes doubled or inside ODBC braces.

    Args:
        answer (str): Everything a tool returned.
        password (str): The password the reader was given.

    Returns:
        str | None: What was found, and in which view, or None.
    """
    views = {"as written": answer, "decoded": unquote(answer), "JSON": _json_unescaped(answer)}
    for view, text in views.items():
        folded, wanted = text.casefold(), password.casefold()
        for name, form in escaped_forms(password).items():
            if form != password and form.casefold() in folded:
                return f"the password {name}, {view}"
        for start in range(len(wanted) - _WINDOW + 1):
            window = wanted[start : start + _WINDOW]
            if window in folded:
                return f"the run {window!r}, {view}"
    return None
