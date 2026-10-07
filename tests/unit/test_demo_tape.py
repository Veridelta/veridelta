# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the recorded demos honest.

Each tape in `demo/` is a script `make demo` renders into a GIF under
`docs/assets/`. These tests hold every tape to the CLI, the GIF to the page
that shows it, and the recording's text in `demo/<tape>.txt` to what the
commands print, so a recording cannot type a command that does not exist or
show output the CLI no longer prints. The quick start's data is also held to
the CI fixtures.
"""

import contextlib
import io
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from veridelta.cli import build_parser, main
from veridelta.engine import DiffEngine

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_DEMO = _ROOT / "demo"
_SETTINGS = _DEMO / "settings.tape"
_TAPES = sorted(path for path in _DEMO.glob("*.tape") if path != _SETTINGS)
_QUICK_START = _DEMO / "veridelta.tape"
_GIF_URL = "https://veridelta.github.io/veridelta/assets/demo.gif"
_PROMPT = "> "
"""The prompt vhs shows before each command."""

_TYPED = re.compile(r"^Type ([\"'])(.+)\1$", re.MULTILINE)
_OUTPUT = re.compile(r"^Output (.+)$", re.MULTILINE)
_HEAD = re.compile(r"^head -n (\d+) (\S+)$")

_EMBEDDED_IN = {
    "veridelta": "docs/index.md",
    "validate": "docs/cli.md",
    "crosswalk": "docs/cli.md",
    "mcp": "docs/agents.md",
}
"""The docs page that shows each recording, by tape."""


def _typed(tape: Path) -> list[str]:
    """Return every line a tape types, in order, whichever quotes the tape used."""
    return [command for _, command in _TYPED.findall(tape.read_text(encoding="utf-8"))]


def _gif(tape: Path) -> Path:
    """Return the GIF a tape writes, from its one `Output` line."""
    outputs = _OUTPUT.findall(tape.read_text(encoding="utf-8"))
    assert len(outputs) == 1, f"{tape.name} should write one GIF, not {outputs}."
    return (_DEMO / outputs[0]).resolve()


def _transcript(tape: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Run a tape's commands as the recording does, and return what the terminal shows."""
    monkeypatch.chdir(_DEMO)
    shown = ""
    code = 0
    for command in _typed(tape):
        shown += _PROMPT + command + "\n"
        head = _HEAD.match(command)
        if command.startswith("#"):
            # A comment labels the recording, and the shell prints nothing for it.
            continue
        if command.startswith("cat "):
            shown += Path(command[4:]).read_text(encoding="utf-8")
        elif command.startswith("python "):
            # A demo script, such as the MCP client, which starts `veridelta mcp` itself,
            # from this environment's scripts, as `uv run vhs` puts them on PATH.
            scripts = str(Path(sys.executable).parent)
            ran = subprocess.run(
                [sys.executable, *shlex.split(command)[1:]],
                capture_output=True,
                text=True,
                check=False,
                timeout=120,
                env={**os.environ, "PATH": scripts + os.pathsep + os.environ.get("PATH", "")},
            )
            assert ran.returncode == 0, ran.stderr
            shown += ran.stdout
        elif head:
            lines = Path(head.group(2)).read_text(encoding="utf-8").splitlines(keepends=True)
            shown += "".join(lines[: int(head.group(1))])
        elif command.startswith("veridelta "):
            monkeypatch.setattr(sys, "argv", shlex.split(command))
            # A terminal shows stdout and stderr in the order they are written.
            terminal = io.StringIO()
            with (
                contextlib.redirect_stdout(terminal),
                contextlib.redirect_stderr(terminal),
                pytest.raises(SystemExit) as exited,
            ):
                main()
            assert isinstance(exited.value.code, int)
            code = exited.value.code
            shown += terminal.getvalue()
        elif command.startswith("echo "):
            words = shlex.split(command)[1:]
            shown += " ".join(words).replace("$?", str(code)) + "\n"
        else:
            raise AssertionError(f"This test cannot run {command!r}.")
    return shown


def test_it_finds_every_tape() -> None:
    """Ensure a moved demo folder cannot silently skip every check, and each tape has a page."""
    assert {tape.stem for tape in _TAPES} == set(_EMBEDDED_IN)


@pytest.mark.parametrize("tape", _TAPES, ids=[tape.stem for tape in _TAPES])
class TestEveryTape:
    """Hold each tape, its output, and the page that shows it together."""

    def test_it_requires_veridelta_and_shares_the_settings(self, tape: Path) -> None:
        """Ensure the tape stops without `veridelta`, and draws as every other tape does."""
        lines = tape.read_text(encoding="utf-8").splitlines()
        settings = _SETTINGS.read_text(encoding="utf-8").splitlines()

        assert lines.index("Require veridelta") < lines.index("Source settings.tape")
        assert all(not line or line.startswith(("#", "Set ")) for line in settings)

    def test_every_veridelta_line_parses_with_the_cli(self, tape: Path) -> None:
        """Ensure a renamed command or flag fails here before the recording lies."""
        refused = []
        for line in _typed(tape):
            if not line.startswith("veridelta "):
                continue
            try:
                build_parser().parse_args(shlex.split(line)[1:])
            except SystemExit:
                refused.append(line)

        assert not refused, f"The CLI refuses these lines of {tape.name}:\n" + "\n".join(refused)

    def test_the_transcript_is_what_the_commands_print(
        self, tape: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure the recording's text, kept beside the tape, is what a terminal shows today."""
        transcript = tape.with_suffix(".txt")

        assert transcript.read_text(encoding="utf-8") == _transcript(tape, monkeypatch)

    def test_its_gif_is_on_its_page(self, tape: Path) -> None:
        """Ensure the tape writes a GIF under `docs/assets/` that its docs page embeds."""
        gif = _gif(tape)
        page = _ROOT / _EMBEDDED_IN[tape.stem]

        assert gif.parent == _ROOT / "docs" / "assets"
        assert gif.read_bytes()[:6] in (b"GIF89a", b"GIF87a")
        assert f"](assets/{gif.name})" in page.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "tape", [tape for tape in _TAPES if tape != _QUICK_START], ids=lambda tape: tape.stem
)
def test_a_feature_page_shows_the_transcript_beside_the_recording(tape: Path) -> None:
    """Ensure a feature recording sits in a closed `<details>`, with its text as a code block.

    Nothing new plays on its own, and a reader who cannot watch the GIF reads
    what it shows.
    """
    page = (_ROOT / _EMBEDDED_IN[tape.stem]).read_text(encoding="utf-8")
    gif = _gif(tape).name
    block = re.search(
        rf"<details markdown>\n<summary>.+</summary>\n\n!\[.+\]\(assets/{re.escape(gif)}\)\n\n"
        r"```text\n(.*?)```\n\n</details>",
        page,
        re.DOTALL,
    )

    assert block, f"{_EMBEDDED_IN[tape.stem]} does not show {gif} in a closed <details> block."
    assert block.group(1) == tape.with_suffix(".txt").read_text(encoding="utf-8")


class TestQuickStart:
    """Hold the quick start, the one recording the README and the docs home embed."""

    def test_it_types_the_quick_start(self) -> None:
        """Ensure the recording shows the configuration and the data, validates, runs, and shows the exit code."""
        assert _typed(_QUICK_START) == [
            "cat veridelta.yaml",
            "cat legacy.csv",
            "cat modern.csv",
            "veridelta validate -c veridelta.yaml",
            "veridelta run -c veridelta.yaml",
            'echo "exit code for CI: $? (0 match, 1 drift, 3 error)"',
        ]

    def test_make_demo_pins_the_recorder_contributors_install(self) -> None:
        """Ensure the vhs release `make demo` asks for is the one CONTRIBUTING.md installs."""
        makefile = (_ROOT / "Makefile").read_text(encoding="utf-8")
        pinned = re.search(r"^VHS_VERSION := (v[0-9.]+)$", makefile, re.MULTILINE)
        contributing = (_ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")

        assert pinned, "The Makefile no longer pins the vhs release."
        assert f"go install github.com/charmbracelet/vhs@{pinned.group(1)}" in contributing

    def test_it_runs_on_the_ci_fixtures(self) -> None:
        """Ensure the data on screen is the data the CI job compares, byte for byte."""
        fixtures = _ROOT / "tests" / "fixtures" / "ci"

        assert (_DEMO / "legacy.csv").read_bytes() == (fixtures / "legacy.csv").read_bytes()
        assert (_DEMO / "modern.csv").read_bytes() == (fixtures / "modern_drift.csv").read_bytes()
        assert (_DEMO / "veridelta.yaml").read_text(encoding="utf-8") == (
            "primary_keys: [id]\nsource:\n  path: legacy.csv\ntarget:\n  path: modern.csv\n"
        )

    def test_the_readme_embeds_it_from_the_site(self) -> None:
        """Ensure the README, which PyPI shows too, loads the GIF from the docs site."""
        readme = (_ROOT / "README.md").read_text(encoding="utf-8")

        assert _gif(_QUICK_START).name == "demo.gif"
        assert f"]({_GIF_URL})" in readme


class TestAgentKit:
    """Hold the kit for recording a real agent to the server and the files it registers."""

    def test_it_registers_a_server_the_cli_starts(self) -> None:
        """Ensure the kit's command starts `veridelta mcp` with flags the CLI accepts, rows allowed."""
        readme = (_DEMO / "agent" / "README.md").read_text(encoding="utf-8")
        added = re.search(r"^ *(claude mcp add veridelta -- .+)$", readme, re.MULTILINE)
        assert added, "The kit's README no longer registers the server."
        args = shlex.split(added.group(1))
        command = args[args.index("mcp", args.index("--")) :]

        parsed = build_parser().parse_args(command)

        assert parsed.allow_row_values is True
        assert command[command.index("--root") + 1] == "."

    def test_its_configuration_is_valid_and_finds_drift(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure the agent finds something to report: a valid file over two drifting exports."""
        monkeypatch.chdir(_DEMO / "agent")

        findings = DiffEngine.check_config_file("veridelta.yaml", schemas=True)
        monkeypatch.setattr(sys, "argv", ["veridelta", "run", "-c", "veridelta.yaml", "--quiet"])
        with contextlib.redirect_stdout(io.StringIO()), pytest.raises(SystemExit) as exited:
            main()

        assert findings == []
        assert exited.value.code == 1
