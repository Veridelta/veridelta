# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Execute the tutorial notebooks and hold them to the output they document."""

import difflib
import shutil
from pathlib import Path

import nbformat
import pytest
from nbclient import NotebookClient

REPO_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS = sorted((REPO_ROOT / "docs" / "examples").glob("*.ipynb"))
OUTPUT_MARKER = "# Output:"


def _recorded_output(source: str) -> list[str] | None:
    """Return the lines a cell documents after its `# Output:` marker, if it has one."""
    lines = source.rstrip("\n").split("\n")
    if OUTPUT_MARKER not in lines:
        return None
    block = lines[lines.index(OUTPUT_MARKER) + 1 :]
    return _trimmed([line[2:] if line.startswith("# ") else line.lstrip("#") for line in block])


def _trimmed(lines: list[str]) -> list[str]:
    """Drop trailing whitespace and trailing blank lines, which a reader cannot see."""
    stripped = [line.rstrip() for line in lines]
    while stripped and not stripped[-1]:
        stripped.pop()
    return stripped


def _printed_output(cell: nbformat.NotebookNode) -> list[str]:
    """Return what a cell printed to stdout, with shell line endings normalized."""
    text = "".join(
        output.get("text", "")
        for output in cell.get("outputs", [])
        if output.get("output_type") == "stream" and output.get("name") == "stdout"
    )
    return _trimmed(text.replace("\r\n", "\n").split("\n"))


@pytest.mark.e2e
@pytest.mark.fast
def test_it_finds_the_tutorials() -> None:
    """Ensure a moved or renamed examples folder cannot silently skip every notebook."""
    assert NOTEBOOKS


@pytest.mark.e2e
@pytest.mark.slow
@pytest.mark.parametrize("notebook", NOTEBOOKS, ids=[path.stem for path in NOTEBOOKS])
def test_it_prints_what_the_tutorial_documents(
    notebook: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure every recorded `# Output:` block matches a fresh run of its cell.

    The notebook runs in a scratch directory, so the files it writes land
    there. `load_nyc_taxi` is served from the copy in the repository through
    a scratch home directory, so the run needs no network access.
    """
    cache = tmp_path / "home" / ".cache" / "veridelta" / "datasets"
    cache.mkdir(parents=True)
    shutil.copy(REPO_ROOT / "docs" / "assets" / "data" / "sample_taxi_data.parquet", cache)
    # `Path.home()` reads USERPROFILE on Windows and HOME everywhere else.
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("USERPROFILE", str(tmp_path / "home"))
    workdir = tmp_path / "run"
    workdir.mkdir()

    document = nbformat.read(notebook, as_version=4)
    NotebookClient(
        document, timeout=300, kernel_name="python3", resources={"metadata": {"path": str(workdir)}}
    ).execute()

    checked = 0
    for index, cell in enumerate(document.cells):
        recorded = _recorded_output(cell.source) if cell.cell_type == "code" else None
        if recorded is None:
            continue
        printed = _printed_output(cell)
        diff = "\n".join(
            difflib.unified_diff(recorded, printed, "recorded", "printed", lineterm="")
        )
        assert printed == recorded, f"{notebook.name} cell {index}:\n{diff}"
        checked += 1
    assert checked, f"{notebook.name} documents no output to check"
