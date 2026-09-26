# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for YAML configuration parsing and validation."""

from pathlib import Path

import pytest

from veridelta.config import load_config
from veridelta.exceptions import ConfigError
from veridelta.models import (
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
)


@pytest.mark.unit
@pytest.mark.fast
class TestYAMLConfigurationParsing:
    """Validate YAML file reading, structural integrity, and Pydantic schema mapping."""

    def test_it_raises_config_error_when_file_does_not_exist(self, tmp_path: Path) -> None:
        """Ensure the parser safely catches missing or invalid file paths."""
        fake_path = tmp_path / "does_not_exist.yaml"

        with pytest.raises(ConfigError, match="Configuration file not found"):
            load_config(fake_path)

    def test_it_raises_config_error_when_path_is_a_directory_instead_of_a_file(
        self, tmp_path: Path
    ) -> None:
        """Ensure the parser rejects directories passed by mistake via the CLI."""
        with pytest.raises(ConfigError, match="not found or is not a file"):
            load_config(tmp_path)

    def test_it_raises_config_error_for_malformed_yaml_syntax(self, tmp_path: Path) -> None:
        """Ensure invalid YAML formatting triggers a graceful ConfigError."""
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text("source: [this is unclosed and invalid yaml: \n - target:")

        with pytest.raises(ConfigError, match="Failed to parse YAML file"):
            load_config(bad_yaml)

    def test_it_raises_config_error_when_file_is_completely_empty(self, tmp_path: Path) -> None:
        """Ensure completely blank files are caught and gracefully rejected."""
        empty_yaml = tmp_path / "empty.yaml"
        empty_yaml.touch()

        with pytest.raises(ConfigError, match="Root element must be a dictionary"):
            load_config(empty_yaml)

    def test_it_raises_config_error_when_root_is_not_a_dictionary(self, tmp_path: Path) -> None:
        """Ensure the parser rejects YAML files that evaluate to a list instead of a dict."""
        list_yaml = tmp_path / "list.yaml"
        list_yaml.write_text("- item1\n- item2")

        with pytest.raises(ConfigError, match="Root element must be a dictionary"):
            load_config(list_yaml)

    def test_it_raises_config_error_when_missing_source_or_target_blocks(
        self, tmp_path: Path
    ) -> None:
        """Ensure the mandatory 'source' and 'target' definitions are strictly enforced."""
        missing_yaml = tmp_path / "missing.yaml"
        # Providing source, but omitting target
        missing_yaml.write_text("source:\n  path: data.csv\nprimary_keys:\n  - id\n")

        with pytest.raises(ConfigError, match="must contain both 'source' and 'target' blocks"):
            load_config(missing_yaml)

    def test_it_raises_config_error_with_formatted_message_on_validation_failure(
        self, tmp_path: Path
    ) -> None:
        """Ensure Pydantic ValidationErrors are cleanly intercepted and formatted for the user."""
        invalid_schema_yaml = tmp_path / "invalid.yaml"
        # Missing the required 'primary_keys' root attribute, and adding an illegal extra field
        invalid_schema_yaml.write_text(
            "source:\n  path: src.csv\ntarget:\n  path: tgt.csv\nunsupported_field: true\n"
        )

        with pytest.raises(ConfigError) as exc_info:
            load_config(invalid_schema_yaml)

        error_msg = str(exc_info.value)
        assert "Configuration Validation Failed" in error_msg
        assert "primary_keys" in error_msg
        assert "unsupported_field" in error_msg

    @pytest.mark.parametrize(
        "blocks",
        [
            pytest.param(
                "source:\n  type: snowflake\n  table: SRC\n  user: u\n  warehouse: w\n"
                "  database: d\n  schema_name: s\n  password: hunter2-do-not-print\n"
                "target:\n  path: b.csv\n",
                id="missing-field",
            ),
            pytest.param(
                "source:\n  path: a.csv\n"
                "target:\n  type: snowfake\n  password: hunter2-do-not-print\n",
                id="unknown-type",
            ),
        ],
    )
    def test_it_keeps_connection_secrets_out_of_validation_errors(
        self, tmp_path: Path, blocks: str
    ) -> None:
        """Ensure neither the message nor the chained Pydantic error repeats a credential.

        The message lists only each error's summary, but the chained error
        quoted the whole block, so a traceback or `logging.exception` printed
        the password.
        """
        config = tmp_path / "leaky.yaml"
        config.write_text(blocks + "primary_keys:\n  - id\n")

        with pytest.raises(ConfigError) as exc_info:
            load_config(config)

        assert "hunter2-do-not-print" not in str(exc_info.value)
        assert "hunter2-do-not-print" not in str(exc_info.value.__cause__)

    def test_it_accepts_both_string_and_path_objects_seamlessly(self, tmp_path: Path) -> None:
        """Ensure the parser natively handles raw strings since CLI arguments arrive as strings."""
        valid_yaml = tmp_path / "valid_string.yaml"
        valid_yaml.write_text(
            "source:\n  path: a.csv\ntarget:\n  path: b.csv\nprimary_keys:\n  - id\n"
        )

        diff_cfg, src_cfg, tgt_cfg = load_config(str(valid_yaml))

        assert isinstance(src_cfg, SourceConfig)
        assert isinstance(tgt_cfg, SourceConfig)
        assert diff_cfg.primary_keys == ["id"]
        assert src_cfg.path == "a.csv"
        assert src_cfg.type == "file"
        assert tgt_cfg.path == "b.csv"

    def test_it_successfully_loads_and_validates_a_complete_configuration_file(
        self, tmp_path: Path
    ) -> None:
        """Ensure a fully valid YAML file maps correctly to all three Pydantic models."""
        valid_yaml = tmp_path / "valid.yaml"
        valid_yaml.write_text(
            "source:\n"
            "  path: source_data.parquet\n"
            "  format: parquet\n"
            "target:\n"
            "  path: target_data.csv\n"
            "  format: csv\n"
            "primary_keys:\n"
            "  - invoice_id\n"
            "  - line_item_id\n"
            "schema_mode: allow_additions\n"
            "strict_types: true\n"
        )

        diff_cfg, src_cfg, tgt_cfg = load_config(valid_yaml)

        assert isinstance(src_cfg, SourceConfig)
        assert isinstance(tgt_cfg, SourceConfig)
        assert src_cfg.path == "source_data.parquet"
        assert src_cfg.format == "parquet"
        assert tgt_cfg.path == "target_data.csv"
        assert tgt_cfg.format == "csv"

        assert diff_cfg.primary_keys == ["invoice_id", "line_item_id"]
        assert diff_cfg.schema_mode == "allow_additions"
        assert diff_cfg.strict_types is True


def _load_yaml(tmp_path: Path, text: str) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Write a configuration file and load it."""
    path = tmp_path / "config.yaml"
    path.write_text(text)
    return load_config(path)


_SNOWFLAKE_BLOCK = (
    "  type: snowflake\n  table: SRC\n  account: xy12345\n  user: analyst\n"
    "  warehouse: COMPUTE_WH\n  database: ANALYTICS\n  schema_name: PUBLIC\n"
)
"""Required Snowflake fields, indented as a `source` or `target` block."""


@pytest.mark.unit
@pytest.mark.fast
class TestEnvironmentExpansion:
    """Validate `${NAME}` references in the `source` and `target` blocks."""

    def test_it_expands_credentials_from_the_environment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a password and a token can stay out of the file."""
        monkeypatch.setenv("VD_SF_PASSWORD", "s3cr3t")
        monkeypatch.setenv("VD_DBX_TOKEN", "dapi-123")

        _, source, target = _load_yaml(
            tmp_path,
            "source:\n" + _SNOWFLAKE_BLOCK + "  password: ${VD_SF_PASSWORD}\n"
            "target:\n  type: databricks\n  table: tgt\n  server_hostname: adb.net\n"
            "  http_path: /sql\n  access_token: ${VD_DBX_TOKEN}\nprimary_keys: [id]\n",
        )

        assert isinstance(source, SnowflakeConfig)
        assert isinstance(target, DatabricksConfig)
        assert source.password == "s3cr3t"
        assert target.access_token == "dapi-123"

    def test_it_expands_references_inside_text_and_nested_values(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a reference can sit inside a longer string, a mapping, or a list."""
        monkeypatch.setenv("VD_BUCKET", "lake")
        monkeypatch.setenv("VD_AWS_SECRET", "aws-key")
        monkeypatch.setenv("VD_DATA_DIR", "/data")
        monkeypatch.setenv("VD_NULL", "NA")

        _, source, target = _load_yaml(
            tmp_path,
            "source:\n  type: delta\n  table_uri: s3://${VD_BUCKET}/events\n"
            "  storage_options:\n    AWS_SECRET_ACCESS_KEY: ${VD_AWS_SECRET}\n"
            "target:\n  path: ${VD_DATA_DIR}/target.csv\n"
            "  options:\n    null_values: ['${VD_NULL}', missing]\nprimary_keys: [id]\n",
        )

        assert isinstance(source, DeltaLakeConfig)
        assert isinstance(target, SourceConfig)
        assert source.table_uri == "s3://lake/events"
        assert source.storage_options == {"AWS_SECRET_ACCESS_KEY": "aws-key"}
        assert target.path == "/data/target.csv"
        assert target.options == {"null_values": ["NA", "missing"]}

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param(None, "ANALYST", id="unset"),
            pytest.param("", "ANALYST", id="empty"),
            pytest.param("ADMIN", "ADMIN", id="set"),
        ],
    )
    def test_it_falls_back_to_a_default_when_the_variable_is_unset_or_empty(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        value: str | None,
        expected: str,
    ) -> None:
        """Ensure `${NAME:-default}` prefers a non-empty variable over its default."""
        if value is None:
            monkeypatch.delenv("VD_SF_ROLE", raising=False)
        else:
            monkeypatch.setenv("VD_SF_ROLE", value)

        _, source, _ = _load_yaml(
            tmp_path,
            "source:\n" + _SNOWFLAKE_BLOCK + "  role: ${VD_SF_ROLE:-ANALYST}\n"
            "target:\n  path: b.csv\nprimary_keys: [id]\n",
        )

        assert isinstance(source, SnowflakeConfig)
        assert source.role == expected

    def test_it_keeps_a_variable_that_is_set_but_empty(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure an empty variable without a default expands to empty text."""
        monkeypatch.setenv("VD_SUFFIX", "")

        _, source, _ = _load_yaml(
            tmp_path,
            "source:\n  path: a${VD_SUFFIX}.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n",
        )

        assert isinstance(source, SourceConfig)
        assert source.path == "a.csv"

    def test_it_names_an_unset_variable_and_where_it_is_used(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a missing credential fails the load instead of sending the reference as text."""
        monkeypatch.delenv("VD_MISSING_SECRET", raising=False)

        with pytest.raises(
            ConfigError,
            match=r"'VD_MISSING_SECRET' is not set.*target -> storage_options -> AWS_SECRET",
        ):
            _load_yaml(
                tmp_path,
                "source:\n  path: a.csv\ntarget:\n  type: iceberg\n  table_uri: s3://t\n"
                "  storage_options:\n    AWS_SECRET: ${VD_MISSING_SECRET}\nprimary_keys: [id]\n",
            )

    @pytest.mark.parametrize(
        "reference",
        [
            pytest.param("${VD_UNCLOSED", id="unclosed"),
            pytest.param("${1}", id="leading-digit"),
            pytest.param("${VD-DASH}", id="invalid-character"),
            pytest.param("${}", id="empty-name"),
            pytest.param("${VD_A:-${VD_B}}", id="nested-default"),
        ],
    )
    def test_it_rejects_a_malformed_reference(self, tmp_path: Path, reference: str) -> None:
        """Ensure a typo in a reference fails loudly rather than reaching a driver as text."""
        with pytest.raises(ConfigError, match=r"Malformed environment reference in source -> path"):
            _load_yaml(
                tmp_path,
                f"source:\n  path: '{reference}'\ntarget:\n  path: b.csv\nprimary_keys: [id]\n",
            )

    def test_it_writes_a_literal_reference_for_a_doubled_dollar(self, tmp_path: Path) -> None:
        """Ensure `$${` escapes the reference syntax."""
        _, source, _ = _load_yaml(
            tmp_path,
            "source:\n  path: $${VD_NOT_EXPANDED}/a.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n",
        )

        assert isinstance(source, SourceConfig)
        assert source.path == "${VD_NOT_EXPANDED}/a.csv"

    def test_it_never_expands_a_substituted_value_again(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a secret that happens to contain `${` arrives intact."""
        monkeypatch.setenv("VD_ODD_PASSWORD", "p${VD_OTHER}$$")

        _, source, _ = _load_yaml(
            tmp_path,
            "source:\n" + _SNOWFLAKE_BLOCK + "  password: ${VD_ODD_PASSWORD}\n"
            "target:\n  path: b.csv\nprimary_keys: [id]\n",
        )

        assert isinstance(source, SnowflakeConfig)
        assert source.password == "p${VD_OTHER}$$"

    def test_it_expands_a_block_shared_through_a_yaml_anchor_on_each_side(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure both sides expand the file's text, not each other's output.

        A merge key hands the target the same `storage_options` mapping as the
        source, so editing the parsed YAML in place would expand the secret twice.
        """
        monkeypatch.setenv("VD_LAKE_SECRET", "k${VD_OTHER}")
        monkeypatch.delenv("VD_OTHER", raising=False)

        _, source, target = _load_yaml(
            tmp_path,
            "source: &lake\n  type: delta\n  table_uri: s3://lake/a\n"
            "  storage_options:\n    AWS_SECRET_ACCESS_KEY: ${VD_LAKE_SECRET}\n"
            "target:\n  <<: *lake\n  table_uri: s3://lake/b\nprimary_keys: [id]\n",
        )

        assert isinstance(source, DeltaLakeConfig)
        assert isinstance(target, DeltaLakeConfig)
        assert source.storage_options == {"AWS_SECRET_ACCESS_KEY": "k${VD_OTHER}"}
        assert target.storage_options == source.storage_options
        assert target.table_uri == "s3://lake/b"

    @pytest.mark.parametrize(
        ("options", "location"),
        [
            pytest.param("&loop\n    nested: *loop", "options -> nested", id="mapping"),
            pytest.param("\n    nested: &loop [*loop]", "options -> nested -> 0", id="list"),
        ],
    )
    def test_it_rejects_a_yaml_alias_that_contains_itself(
        self, tmp_path: Path, options: str, location: str
    ) -> None:
        """Ensure a looping alias is reported as a configuration error, not a crash."""
        with pytest.raises(ConfigError, match=rf"alias at source -> {location} refers to"):
            _load_yaml(
                tmp_path,
                f"source:\n  path: a.csv\n  options: {options}\n"
                "target:\n  path: b.csv\nprimary_keys: [id]\n",
            )

    def test_it_leaves_keys_non_text_values_and_other_settings_alone(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure only text inside `source` and `target` is expanded.

        Rules and root settings keep `${...}` verbatim, so a regex replacement
        can still refer to a capture group as `${1}`.
        """
        monkeypatch.setenv("VD_KEY", "expanded")

        diff, source, _ = _load_yaml(
            tmp_path,
            "source:\n  path: a.csv\n  options:\n    ${VD_KEY}: ${VD_KEY}\n    skip_rows: 2\n"
            "    has_header: true\ntarget:\n  path: b.csv\nprimary_keys: [id]\n"
            "output_path: ${VD_KEY}\nrules:\n  - column_names: [code]\n"
            "    regex_replace:\n      '(\\d+)': '${1}'\n",
        )

        assert isinstance(source, SourceConfig)
        assert source.options == {"${VD_KEY}": "expanded", "skip_rows": 2, "has_header": True}
        assert diff.output_path == "${VD_KEY}"
        assert diff.rules[0].regex_replace == {"(\\d+)": "${1}"}

    def test_it_keeps_an_expanded_secret_out_of_validation_errors(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a block that fails validation after expansion never repeats the secret."""
        monkeypatch.setenv("VD_SF_PASSWORD", "hunter2-do-not-print")

        with pytest.raises(ConfigError) as exc_info:
            _load_yaml(
                tmp_path,
                "source:\n  type: snowflake\n  table: SRC\n  password: ${VD_SF_PASSWORD}\n"
                "target:\n  path: b.csv\nprimary_keys: [id]\n",
            )

        assert "hunter2-do-not-print" not in str(exc_info.value)
        assert "hunter2-do-not-print" not in str(exc_info.value.__cause__)
