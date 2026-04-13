# Copyright 2026 Nicholas Harder
# SPDX-License-Identifier: Apache-2.0

"""Configuration parsing and validation from YAML files."""

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from veridelta.exceptions import ConfigError
from veridelta.models import DiffConfig, SourceConfig


def load_config(path: str | Path) -> tuple[DiffConfig, SourceConfig, SourceConfig]:
    """Loads a Veridelta configuration from a YAML file.

    Args:
        path: The file path to the YAML configuration.

    Returns:
        A tuple of (DiffConfig, SourceConfig, SourceConfig).

    Raises:
        ConfigError: If the file is missing, invalid YAML, or fails schema validation.
    """
    file_path = Path(path)

    if not file_path.exists():
        raise ConfigError(f"Configuration file not found: {file_path.absolute()}")

    try:
        with file_path.open("r", encoding="utf-8") as f:
            raw_config: Any = yaml.safe_load(f)
    except yaml.YAMLError as yaml_err:
        raise ConfigError(f"Failed to parse YAML file:\n{yaml_err}") from yaml_err

    if not isinstance(raw_config, dict):
        raise ConfigError("Invalid YAML structure: Root element must be a dictionary.")

    if "source" not in raw_config or "target" not in raw_config:
        raise ConfigError("Configuration must contain both 'source' and 'target' blocks.")

    try:
        raw_source = raw_config.pop("source")
        raw_target = raw_config.pop("target")

        source_cfg = SourceConfig.model_validate(raw_source)
        target_cfg = SourceConfig.model_validate(raw_target)
        diff_cfg = DiffConfig.model_validate(raw_config)

        return diff_cfg, source_cfg, target_cfg

    except ValidationError as e:
        error_msg = "Configuration Validation Failed:\n"
        for validation_error in e.errors():
            location = " -> ".join(str(loc) for loc in validation_error["loc"])
            error_msg += f"  - [{location}]: {validation_error['msg']}\n"
        raise ConfigError(error_msg) from e
