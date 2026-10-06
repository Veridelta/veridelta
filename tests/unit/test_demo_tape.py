# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the recorded quick start honest.

`demo/veridelta.tape` is the script `make demo` renders into the GIF the README
and the docs home embed. These tests hold the tape to the CLI, the data it runs
on to the CI fixtures, the GIF to the pages that show it, and the recording's
text in `demo/transcript.txt` to what the commands print, so the recording
cannot type a command that does not exist, show data the suite does not run, or
show output the CLI no longer prints.
"""

import re
import shlex
import sys
from pathlib import Path

import pytest

from veridelta.cli import build_parser, main

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_DEMO = _ROOT / "demo"
_TAPE = _DEMO / "veridelta.tape"
_TRANSCRIPT = _DEMO / "transcript.txt"
_GIF = _ROOT / "docs" / "assets" / "demo.gif"
_GIF_URL = "https://veridelta.github.io/veridelta/assets/demo.gif"
_PROMPT = "> "
"""The prompt vhs shows before each command."""

_TYPED = re.compile(r"^Type ([\"'])(.+)\1$", re.MULTILINE)
_OUTPUT = re.compile(r"^Output (.+)$", re.MULTILINE)


def _tape() -> str:
    """Return the tape."""
    return _TAPE.read_text(encoding="utf-8")


def _typed() -> list[str]:
    """Return every line the tape types, in order, whichever quotes the tape used."""
    return [command for _, command in _TYPED.findall(_tape())]


def _transcript(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> str:
    """Run the tape's commands as the recording does, and return what the terminal shows."""
    monkeypatch.chdir(_DEMO)
    shown = ""
    code = 0
    for command in _typed():
        shown += _PROMPT + command + "\n"
        if command.startswith("cat "):
            shown += Path(command[4:]).read_text(encoding="utf-8")
        elif command.startswith("veridelta "):
            monkeypatch.setattr(sys, "argv", shlex.split(command))
            with pytest.raises(SystemExit) as exited:
                main()
            assert isinstance(exited.value.code, int)
            code = exited.value.code
            captured = capsys.readouterr()
            # The progress lines reach stderr before the summary reaches stdout.
            shown += captured.err + captured.out
        elif command.startswith("echo "):
            words = shlex.split(command)[1:]
            shown += " ".join(words).replace("$?", str(code)) + "\n"
        else:
            raise AssertionError(f"This test cannot run {command!r}.")
    return shown


class TestDemoTape:
    """Hold the tape, its data, and its output together."""

    def test_it_types_the_quick_start(self) -> None:
        """Ensure the recording shows the configuration and the data, validates, runs, and shows the exit code."""
        assert _typed() == [
            "cat veridelta.yaml",
            "cat legacy.csv",
            "cat modern.csv",
            "veridelta validate -c veridelta.yaml",
            "veridelta run -c veridelta.yaml",
            'echo "exit code for CI: $? (0 match, 1 drift, 3 error)"',
        ]

    def test_every_veridelta_line_parses_with_the_cli(self) -> None:
        """Ensure a renamed command or flag fails here before the recording lies."""
        refused = []
        for line in _typed():
            if not line.startswith("veridelta "):
                continue
            try:
                build_parser().parse_args(shlex.split(line)[1:])
            except SystemExit:
                refused.append(line)

        assert not refused, "The CLI refuses these lines of the tape:\n" + "\n".join(refused)

    def test_it_runs_on_the_ci_fixtures(self) -> None:
        """Ensure the data on screen is the data the CI job compares, byte for byte."""
        fixtures = _ROOT / "tests" / "fixtures" / "ci"

        assert (_DEMO / "legacy.csv").read_bytes() == (fixtures / "legacy.csv").read_bytes()
        assert (_DEMO / "modern.csv").read_bytes() == (fixtures / "modern_drift.csv").read_bytes()
        assert (_DEMO / "veridelta.yaml").read_text(encoding="utf-8") == (
            "primary_keys: [id]\nsource:\n  path: legacy.csv\ntarget:\n  path: modern.csv\n"
        )

    def test_it_renders_the_gif_both_pages_embed(self) -> None:
        """Ensure the tape writes the one asset the README and the docs home show."""
        outputs = _OUTPUT.findall(_tape())
        readme = (_ROOT / "README.md").read_text(encoding="utf-8")
        home = (_ROOT / "docs" / "index.md").read_text(encoding="utf-8")

        assert outputs == ["../docs/assets/demo.gif"]
        assert _GIF.is_file()
        assert _GIF.read_bytes()[:6] in (b"GIF89a", b"GIF87a")
        assert f"]({_GIF_URL})" in readme
        assert "](assets/demo.gif)" in home

    def test_the_transcript_is_what_the_commands_print(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure the recording's text, kept beside the tape, is what a terminal shows today."""
        assert _TRANSCRIPT.read_text(encoding="utf-8") == _transcript(monkeypatch, capsys)
