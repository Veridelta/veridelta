# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

r"""Write text from the data so a terminal or a CI runner shows it and does not act on it.

A column name, a key value, or an error message can carry text from the data,
and a terminal or a CI runner acts on some of it: a line break that starts a
line of its own, an escape sequence, a line that opens with `::`, or `##[`
anywhere in a line, which GitHub Actions reads as a command. Every plain-text
line the command line prints from the data goes through `printable`,
`printable_text`, or `printable_yaml`, so no branch prints such text as it is.
"""

import math

import yaml

_LEGACY_COMMAND = "##["
"""What GitHub Actions reads as a command anywhere in a line."""

_LEGACY_COMMAND_BROKEN = "##\\x5b"
"""`##[` with its `[` written as an escape, which no runner reads as a command."""


def _escaped(text: str, *, keep: str = "") -> str:
    """Escape each character that is not printable, as `repr` does, and break each `##[`.

    Args:
        text (str): The text to write.
        keep (str): Characters to leave as they are though not printable.

    Returns:
        str: The text with no control character but those in `keep`, and no `##[`.
    """
    shown = "".join(
        char if char.isprintable() or char in keep else repr(char)[1:-1] for char in text
    )
    return shown.replace(_LEGACY_COMMAND, _LEGACY_COMMAND_BROKEN)


def printable(value: object, *, quote: bool = False) -> str:
    r"""Write a column name or a value from the data for a terminal or a CI log.

    A text that starts with a letter, a digit, or an underscore, holds only
    printable characters, and holds no `##[` is written as it is. Any other
    value, of any type, is written as Python's `repr`, with the `[` of each
    `##[` written as `\x5b`. `repr` doubles a backslash, so a value that
    spells `\x5b` reads differently from one that held `##[`.

    Args:
        value (object): A column name, or a value from the data, such as a key.
        quote (bool): Write a plain text as its `repr` too.

    Returns:
        str: The text as it is, or the value's `repr` with nothing a terminal
            or a CI runner acts on.
    """
    if isinstance(value, str) and not quote and _plain(value):
        return value
    return _escaped(repr(value))


def _plain(text: str) -> bool:
    """Say whether a text prints as it is: no control character, no `##[`, no odd start."""
    return (
        (text[:1].isalnum() or text[:1] == "_")
        and text.isprintable()
        and _LEGACY_COMMAND not in text
    )


def printable_text(text: str) -> str:
    r"""Write a message that can quote the data, such as an error, for a terminal or a CI log.

    Line breaks and tabs stay as they are, so the message reads as written.
    Any other control character is escaped as `repr` writes it, the `[` of
    each `##[` reads `\x5b`, and a line that opens with `::` after its
    indentation has its first `:` written as `\x3a`.

    Args:
        text (str): The message.

    Returns:
        str: The message with nothing a terminal or a CI runner acts on.
    """
    return "\n".join(_no_workflow_command(line) for line in _escaped(text, keep="\n\t").split("\n"))


def _no_workflow_command(line: str) -> str:
    """Break the `::` a line opens with, after its indentation, which GitHub Actions runs."""
    body = line.lstrip(" \t")
    if not body.startswith("::"):
        return line
    return f"{line[: len(line) - len(body)]}\\x3a{body[1:]}"


class _PrintableDumper(yaml.SafeDumper):
    """Dump YAML that writes a text from the data in double quotes unless it is plain."""


def _represent_text(dumper: yaml.SafeDumper, text: str) -> yaml.ScalarNode:
    """Double-quote a text that is not plain, so its escapes keep it on one line."""
    style = None if _plain(text) else '"'
    return dumper.represent_scalar("tag:yaml.org,2002:str", text, style=style)  # pyright: ignore[reportUnknownMemberType]  # the PyYAML stub leaves `value` untyped


_PrintableDumper.add_representer(str, _represent_text)


def printable_yaml(data: object) -> str:
    r"""Dump YAML to print, which reads back as `data` and holds nothing a CI runner acts on.

    A text that is not plain, as `printable` decides, is double-quoted, where
    YAML escapes a line break or an escape sequence and reads `\x5b` as `[`,
    so the `[` of each `##[` is written that way. No line is folded, so no
    line opens with `::`.

    Args:
        data (object): What to dump, such as `{"rules": [...]}`.

    Returns:
        str: The YAML, keys in the order given.
    """
    dumped = yaml.dump(data, Dumper=_PrintableDumper, sort_keys=False, width=math.inf)
    # Every `##[` sits inside a double-quoted text, where `\x5b` reads as `[`.
    return dumped.replace(_LEGACY_COMMAND, _LEGACY_COMMAND_BROKEN)
