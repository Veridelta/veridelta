# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the JSON Schema that editors validate configuration files with."""

import re
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator

from veridelta.config import config_json_schema, load_config
from veridelta.exceptions import ConfigError

_ROOT = Path(__file__).resolve().parents[2]

_SNOWFLAKE = (
    "  table: {table}\n  account: xy12345\n  user: analyst\n  warehouse: COMPUTE_WH\n"
    "  database: ANALYTICS\n  schema_name: PUBLIC\n"
)


def _validator() -> Draft202012Validator:
    """Build a validator over the generated schema."""
    return Draft202012Validator(config_json_schema())


def _schema_accepts(text: str) -> bool:
    """Return whether the schema accepts a YAML document."""
    return _validator().is_valid(yaml.safe_load(text))


def _loader_accepts(tmp_path: Path, text: str) -> bool:
    """Return whether `load_config` accepts the same document."""
    path = tmp_path / "config.yaml"
    path.write_text(text, encoding="utf-8")
    try:
        load_config(path)
    except ConfigError:
        return False
    return True


def _documented_configs() -> list[Any]:
    """Collect every complete YAML configuration shown in the guide and README."""
    params = []
    for path in (_ROOT / "docs" / "configuration.md", _ROOT / "README.md"):
        blocks = re.findall(r"```yaml\n(.*?)```", path.read_text(encoding="utf-8"), re.DOTALL)
        for index, block in enumerate(blocks):
            if all(key in block for key in ("primary_keys", "source:", "target:")):
                params.append(pytest.param(block, id=f"{path.name}-{index}"))
    return params


@pytest.mark.unit
@pytest.mark.fast
class TestConfigJsonSchema:
    """Validate the generated schema against the loader it describes."""

    def test_it_is_a_valid_draft_2020_12_schema(self) -> None:
        """Ensure editors and validators accept the schema itself."""
        schema = config_json_schema()

        Draft202012Validator.check_schema(schema)
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["$id"].endswith("/schema/veridelta.schema.json")
        assert schema["title"] == "Veridelta configuration"

    def test_it_requires_primary_keys_and_both_sides(self) -> None:
        """Ensure an empty file is flagged for each missing root key."""
        missing = {error.message for error in _validator().iter_errors({})}

        assert missing == {
            "'primary_keys' is a required property",
            "'source' is a required property",
            "'target' is a required property",
        }

    @pytest.mark.parametrize(
        ("text", "valid"),
        [
            pytest.param(
                "primary_keys: [id]\nsource:\n  path: a.csv\ntarget:\n  path: b.parquet\n"
                "  format: parquet\n",
                True,
                id="file-pair",
            ),
            pytest.param(
                "primary_keys: [id]\nsource:\n  type: snowflake\n"
                + _SNOWFLAKE.format(table="SRC")
                + "target:\n  type: snowflake\n"
                + _SNOWFLAKE.format(table="TGT"),
                True,
                id="snowflake-pair",
            ),
            pytest.param(
                "primary_keys: [id]\nsource:\n"
                + _SNOWFLAKE.format(table="SRC")
                + "target:\n  path: b.csv\n",
                False,
                id="warehouse-block-without-type",
            ),
            pytest.param(
                "primary_keys: [id]\nsource:\n  type: snowflake\n"
                + _SNOWFLAKE.format(table="${VD_SCHEMA_TABLE}")
                + "target:\n  type: snowflake\n"
                + _SNOWFLAKE.format(table="TGT"),
                True,
                id="table-from-environment",
            ),
            pytest.param(
                "primary_keys: [id]\nsource:\n  type: database\n  uri: sqlite:///x.db\n"
                "  table: orders\ntarget:\n  path: b.csv\n",
                True,
                id="database-source",
            ),
            pytest.param(
                "primary_key: [id]\nsource:\n  path: a.csv\ntarget:\n  path: b.csv\n",
                False,
                id="root-typo",
            ),
            pytest.param(
                "primary_keys: [id]\nsource:\n  path: a.csv\ntarget:\n  path: b.csv\n"
                "rules:\n  - column_names: [amount]\n    absolute_tolerence: 0.1\n",
                False,
                id="rule-typo",
            ),
            pytest.param(
                "primary_keys: [id]\nsource:\n  path: a.xls\n  format: xls\ntarget:\n  path: b.csv\n",
                False,
                id="unknown-format",
            ),
        ],
    )
    def test_it_agrees_with_the_loader(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str, valid: bool
    ) -> None:
        """Ensure the schema and `load_config` reach the same verdict.

        A warehouse block without `type` loads as a file source and fails, so
        the schema must require `type` on every non-file branch. A patterned
        field such as `table` must accept a `${NAME}` reference, which the
        loader expands before validating.
        """
        monkeypatch.setenv("VD_SCHEMA_TABLE", "SRC")

        assert _loader_accepts(tmp_path, text) is valid
        assert _schema_accepts(text) is valid

    @pytest.mark.parametrize("text", _documented_configs())
    def test_it_accepts_every_documented_configuration(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str
    ) -> None:
        """Ensure no example in the docs is one the schema would flag, or the loader reject."""
        for name in set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)", text)):
            monkeypatch.setenv(name, "placeholder")
        document: Any = yaml.safe_load(text)

        assert list(_validator().iter_errors(document)) == []
        assert _loader_accepts(tmp_path, text)
