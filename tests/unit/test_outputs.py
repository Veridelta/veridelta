# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold what each command prints with `--json` to the schema published for it."""

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest

from veridelta.cli import main
from veridelta.outputs import OUTPUTS, output_json_schema

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_SCHEMAS = _ROOT / "docs" / "schema"


def _check(name: str, document: Any) -> None:
    """Fail with every way `document` breaks the published schema for `name`."""
    validator = jsonschema.Draft202012Validator(output_json_schema(name))
    errors = [error.message for error in validator.iter_errors(document)]

    assert errors == [], f"{name} breaks its schema:\n" + "\n".join(errors)


def _printed(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], *argv: str
) -> tuple[int | str | None, Any]:
    """Run the command line on `argv`, and return its exit code and the JSON it printed."""
    monkeypatch.setattr("veridelta.cli.sys.argv", ["veridelta", *argv])
    with pytest.raises(SystemExit) as exited:
        main()
    return exited.value.code, json.loads(capsys.readouterr().out)


def _pair(folder: Path, source: str, target: str) -> None:
    """Write two CSV files and a configuration that compares them on `id`."""
    (folder / "a.csv").write_text(source)
    (folder / "b.csv").write_text(target)
    (folder / "veridelta.yaml").write_text(
        "primary_keys: [id]\nsource:\n  path: a.csv\ntarget:\n  path: b.csv\n"
    )


class TestPublishedSchemas:
    """Keep the schemas the docs site serves current, and each one a valid schema."""

    @pytest.mark.parametrize("name", OUTPUTS)
    def test_the_published_file_is_current(self, name: str) -> None:
        """Ensure the file under `docs/schema/` is the one the package generates."""
        published = json.loads((_SCHEMAS / f"{name}.schema.json").read_text(encoding="utf-8"))

        assert published == output_json_schema(name), (
            f"docs/schema/{name}.schema.json is stale. Regenerate it with `make schema`."
        )

    @pytest.mark.parametrize("name", OUTPUTS)
    def test_it_is_a_schema_served_where_its_id_says(self, name: str) -> None:
        """Ensure each schema is valid Draft 2020-12, and its `$id` is its URL on the site."""
        schema = output_json_schema(name)

        jsonschema.Draft202012Validator.check_schema(schema)
        assert schema["$id"] == f"https://veridelta.github.io/veridelta/schema/{name}.schema.json"

    def test_the_schema_command_prints_each_output(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure `veridelta schema NAME` prints that output's schema and exits 0."""
        for name in OUTPUTS:
            code, printed = _printed(monkeypatch, capsys, "schema", name)

            assert code == 0
            assert printed == output_json_schema(name)

    def test_the_schema_command_refuses_an_unknown_output(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a name that is not an output fails as an invalid argument."""
        monkeypatch.setattr("veridelta.cli.sys.argv", ["veridelta", "schema", "summary"])
        with pytest.raises(SystemExit) as exited:
            main()

        assert exited.value.code == 2
        assert "invalid choice: 'summary'" in capsys.readouterr().err


class TestOutputsMatchTheirSchemas:
    """Validate real output from each command against its published schema."""

    def test_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a run with drift prints a summary the run schema accepts."""
        _pair(tmp_path, "id,status\n1,open\n2,shut\n", "id,status\n1,open\n3,shut\n")
        monkeypatch.chdir(tmp_path)

        code, printed = _printed(monkeypatch, capsys, "run", "--json", "--quiet")

        assert code == 1
        assert printed["added_count"] == 1
        _check("run", printed)

    @pytest.mark.parametrize(
        ("target", "valid"),
        [
            pytest.param("  path: b.csv\n", True, id="valid"),
            pytest.param("  path: b.csv\n  format: unknown\n", False, id="with-an-error"),
        ],
    )
    def test_validate(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        target: str,
        valid: bool,
    ) -> None:
        """Ensure a check prints a report the validate schema accepts, valid or not."""
        _pair(tmp_path, "id\n1\n", "id\n1\n")
        (tmp_path / "veridelta.yaml").write_text(
            f"primary_keys: [id]\nsource:\n  path: a.csv\ntarget:\n{target}"
        )
        monkeypatch.chdir(tmp_path)

        code, printed = _printed(monkeypatch, capsys, "validate", "--json", "--quiet")

        assert code == (0 if valid else 1)
        assert printed["valid"] is valid
        _check("validate", printed)

    def test_crosswalk(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a proposal prints as the crosswalk schema says."""
        ids = range(1, 11)
        source = "id,flag\n" + "".join(f"{i},{'Y' if i % 2 else 'N'}\n" for i in ids)
        target = "id,flag\n" + "".join(f"{i},{'true' if i % 2 else 'false'}\n" for i in ids)
        _pair(tmp_path, source, target)
        monkeypatch.chdir(tmp_path)

        code, printed = _printed(monkeypatch, capsys, "crosswalk", "--json", "--quiet")

        assert code == 0
        assert printed[0]["value_map"] == {"N": "false", "Y": "true"}
        _check("crosswalk", printed)

    def test_suggest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a suggestion, its rule and example keys included, prints as the schema says."""
        _pair(tmp_path, "id,fare\n1,10.00\n2,20.00\n", "id,fare\n1,10.004\n2,20.004\n")
        monkeypatch.chdir(tmp_path)

        code, printed = _printed(monkeypatch, capsys, "suggest", "--json", "--quiet")

        assert code == 0
        assert printed[0]["rule"]["absolute_tolerance"] == 0.005
        _check("suggest", printed)

    def test_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Ensure a command that cannot finish prints the one object the error schema names."""
        monkeypatch.chdir(tmp_path)

        code, printed = _printed(monkeypatch, capsys, "run", "-c", "missing.yaml", "--json")

        assert code == 3
        assert printed["error"]["type"] == "ConfigError"
        _check("error", printed)
