# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the recorded quick start honest.

`demo/veridelta.tape` is the script `make demo` renders into the GIF the README
embeds. These tests hold the tape to the CLI, the data it runs on to the CI
fixtures, and the GIF to the URL the README shows, so the recording cannot type
a command that does not exist or show data the suite does not run.
"""

import re
import shlex
from pathlib import Path

import pytest

from veridelta.cli import build_parser

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_DEMO = _ROOT / "demo"
_TAPE = _DEMO / "veridelta.tape"
_GIF = _ROOT / "docs" / "assets" / "demo.gif"
_GIF_URL = "https://veridelta.github.io/veridelta/assets/demo.gif"
_TYPED = re.compile(r'^Type "(.+)"$', re.MULTILINE)
_OUTPUT = re.compile(r"^Output (.+)$", re.MULTILINE)


def _typed() -> list[str]:
    """Return every line the tape types, in order."""
    return _TYPED.findall(_TAPE.read_text(encoding="utf-8"))


class TestDemoTape:
    """Hold the tape, its data, and its output together."""

    def test_it_types_the_quick_start(self) -> None:
        """Ensure the recording shows the file, validates it, runs it, and shows the exit code."""
        assert _typed() == [
            "cat veridelta.yaml",
            "veridelta validate -c veridelta.yaml",
            "veridelta run -c veridelta.yaml",
            "echo $?",
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
        outputs = _OUTPUT.findall(_TAPE.read_text(encoding="utf-8"))
        readme = (_ROOT / "README.md").read_text(encoding="utf-8")

        assert outputs == ["../docs/assets/demo.gif"]
        assert _GIF.is_file()
        assert _GIF.read_bytes()[:6] in (b"GIF89a", b"GIF87a")
        assert f"]({_GIF_URL})" in readme
