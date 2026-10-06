# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the recorded quick start honest.

`demo/veridelta.tape` is the script `make demo` renders into the GIF the README
embeds and the video the docs home plays with captions. These tests hold the
tape to the CLI, the data it runs on to the CI fixtures, the GIF and the video
to the pages that show them, the captions to the tape's timing, and the
transcript to what the commands print, so the recording cannot type a command
that does not exist, show data the suite does not run, or describe output the
CLI no longer prints.
"""

import re
import shlex
import struct
import sys
from pathlib import Path

import pytest

from veridelta.cli import build_parser, main

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_DEMO = _ROOT / "demo"
_TAPE = _DEMO / "veridelta.tape"
_ASSETS = _ROOT / "docs" / "assets"
_GIF = _ASSETS / "demo.gif"
_VIDEO = _ASSETS / "demo.mp4"
_CAPTIONS = _ASSETS / "demo.vtt"
_HOME = _ROOT / "docs" / "index.md"
_GIF_URL = "https://veridelta.github.io/veridelta/assets/demo.gif"
_PROMPT = "> "
"""The prompt vhs shows before each command."""

_TYPED = re.compile(r"^Type ([\"'])(.+)\1$", re.MULTILINE)
_OUTPUT = re.compile(r"^Output (.+)$", re.MULTILINE)
_TYPING_SPEED = re.compile(r"^Set TypingSpeed (\d+)ms$", re.MULTILINE)
_SLEEP = re.compile(r"^Sleep ([\d.]+)s$", re.MULTILINE)
_CUE = re.compile(r"^(\d\d):(\d\d\.\d{3}) --> (\d\d):(\d\d\.\d{3})\n(.+?)$", re.MULTILINE)
_TRANSCRIPT = re.compile(r"^```text\n(> cat veridelta\.yaml\n.*?)```$", re.MULTILINE | re.DOTALL)
_VIDEO_TAG = re.compile(r"^<video (.+?)>$", re.MULTILINE)


def _tape() -> str:
    """Return the tape."""
    return _TAPE.read_text(encoding="utf-8")


def _typed() -> list[str]:
    """Return every line the tape types, in order, whichever quotes the tape used."""
    return [command for _, command in _TYPED.findall(_tape())]


def _timing() -> list[tuple[float, float]]:
    """Return when each command starts and when its pause ends, as vhs plays the tape.

    vhs types each character after `TypingSpeed`, and `Enter` takes no time of its own.
    """
    tape = _tape()
    speed = int(_TYPING_SPEED.search(tape).group(1)) / 1000  # type: ignore[union-attr]
    pauses = [float(seconds) for seconds in _SLEEP.findall(tape)]
    assert len(pauses) == len(_typed()), "Each typed command needs one Sleep after it."
    spans: list[tuple[float, float]] = []
    start = 0.0
    for command, pause in zip(_typed(), pauses, strict=True):
        end = start + len(command) * speed + pause
        spans.append((start, end))
        start = end
    return spans


def _video_seconds(data: bytes) -> float:
    """Read a video's length from its `mvhd` box, as a player does."""
    at = data.index(b"mvhd") + 4
    if data[at] == 1:
        timescale, duration = struct.unpack(">IQ", data[at + 20 : at + 32])
    else:
        timescale, duration = struct.unpack(">II", data[at + 12 : at + 20])
    return duration / timescale


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
    """Hold the tape, its data, and its outputs together."""

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

    def test_it_renders_the_gif_the_readme_embeds(self) -> None:
        """Ensure the tape writes the asset the README and the site serve."""
        outputs = _OUTPUT.findall(_tape())
        readme = (_ROOT / "README.md").read_text(encoding="utf-8")

        assert outputs == ["../docs/assets/demo.gif", "../docs/assets/demo.mp4"]
        assert _GIF.is_file()
        assert _GIF.read_bytes()[:6] in (b"GIF89a", b"GIF87a")
        assert f"]({_GIF_URL})" in readme


class TestDocsHome:
    """Hold the video, its captions, and its transcript to the tape and the CLI."""

    def test_it_renders_the_video_the_home_page_plays(self) -> None:
        """Ensure the tape writes an MP4 as long as the tape, with the captions track beside it."""
        data = _VIDEO.read_bytes()
        home = _HOME.read_text(encoding="utf-8")

        assert data[4:8] == b"ftyp"
        assert _video_seconds(data) == pytest.approx(_timing()[-1][1], abs=0.5)
        assert '<source src="assets/demo.mp4" type="video/mp4">' in home
        assert '<track kind="captions" src="assets/demo.vtt" srclang="en"' in home
        assert _CAPTIONS.is_file()

    def test_the_video_plays_only_when_asked(self) -> None:
        """Ensure the player has controls and never starts on its own, as WCAG 2.2.2 asks."""
        match = _VIDEO_TAG.search(_HOME.read_text(encoding="utf-8"))
        assert match, "The home page embeds no video."
        attributes = match.group(1).split()

        assert {"controls", "muted", "playsinline"} <= set(attributes)
        assert "autoplay" not in attributes
        assert "loop" not in attributes

    def test_the_captions_follow_the_tape(self) -> None:
        """Ensure one cue per command, timed as the tape types and waits, each naming its command."""
        text = _CAPTIONS.read_text(encoding="utf-8")
        cues = [
            (int(m1) * 60 + float(s1), int(m2) * 60 + float(s2), body)
            for m1, s1, m2, s2, body in _CUE.findall(text)
        ]

        assert text.startswith("WEBVTT\n\n")
        assert len(cues) == len(_typed())
        for (start, end, body), command, (expected_start, expected_end) in zip(
            cues, _typed(), _timing(), strict=True
        ):
            assert start == pytest.approx(expected_start, abs=0.001), command
            assert end == pytest.approx(expected_end, abs=0.001), command
            assert body.startswith(command), command

    def test_the_transcript_is_what_the_commands_print(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure the code block under the video shows the run as the terminal shows it."""
        match = _TRANSCRIPT.search(_HOME.read_text(encoding="utf-8"))
        assert match, "The home page shows no transcript."

        assert match.group(1) == _transcript(monkeypatch, capsys)
