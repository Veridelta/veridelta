# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for YAML configuration parsing and validation."""

from pathlib import Path

import pytest

from veridelta.config import load_config
from veridelta.exceptions import ConfigError


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

    def test_it_accepts_both_string_and_path_objects_seamlessly(self, tmp_path: Path) -> None:
        """Ensure the parser natively handles raw strings since CLI arguments arrive as strings."""
        valid_yaml = tmp_path / "valid_string.yaml"
        valid_yaml.write_text(
            "source:\n  path: a.csv\ntarget:\n  path: b.csv\nprimary_keys:\n  - id\n"
        )

        diff_cfg, src_cfg, tgt_cfg = load_config(str(valid_yaml))

        assert diff_cfg.primary_keys == ["id"]
        assert src_cfg.path == "a.csv"
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

        assert src_cfg.path == "source_data.parquet"
        assert src_cfg.format == "parquet"
        assert tgt_cfg.path == "target_data.csv"
        assert tgt_cfg.format == "csv"

        assert diff_cfg.primary_keys == ["invoice_id", "line_item_id"]
        assert diff_cfg.schema_mode == "allow_additions"
        assert diff_cfg.strict_types is True
