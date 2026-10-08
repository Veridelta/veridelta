# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the errors Veridelta raises and the messages they share."""

from pathlib import Path

import pytest

from veridelta.exceptions import missing_extra

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_SOURCE = Path(__file__).resolve().parents[2] / "src" / "veridelta"


def test_a_missing_extra_names_what_needs_it_and_how_to_install_it() -> None:
    """Ensure the message says what stopped, which extra, and the command that installs it."""
    assert missing_extra("snowflake", "Connecting to Snowflake") == (
        "Connecting to Snowflake needs the optional 'snowflake' extra, which is not "
        "installed. Install it with: uv add 'veridelta[snowflake]'"
    )


def test_every_module_takes_the_message_from_one_place() -> None:
    """Ensure no module writes its own message for a missing extra, so they all read alike."""
    written = [
        path.relative_to(_SOURCE).as_posix()
        for path in sorted(_SOURCE.rglob("*.py"))
        if path.name != "exceptions.py" and "Install it with" in path.read_text(encoding="utf-8")
    ]

    assert written == []
