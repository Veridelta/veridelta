# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Configuration parsing and validation from YAML files.

This module acts as the bridge between user-defined YAML configurations
and the strict Pydantic models required by the execution engine.
"""

from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import ValidationError

from veridelta.exceptions import ConfigError
from veridelta.models import DiffConfig, SourceConfig


def load_config(path: str | Path) -> tuple[DiffConfig, SourceConfig, SourceConfig]:
    """Loads and validates a Veridelta configuration from a YAML file.

    Extracts the `source` and `target` blocks and evaluates the root
    parameters as the master `DiffConfig`.

    Args:
        path (str | Path): The file system path to the YAML configuration.

    Returns:
        tuple[DiffConfig, SourceConfig, SourceConfig]: The validated master,
            source, and target configurations.

    Raises:
        ConfigError: If the file cannot be located, contains invalid YAML syntax,
            lacks the mandatory source/target blocks, or violates the strict
            Pydantic schema definitions.

    Example:
        A standard pipeline configuration (`veridelta.yaml`):

        ```yaml
        primary_keys: ["id", "region"]
        output_format: "parquet"

        source:
          path: "s3://raw-data/*.csv"
          format: "csv"
          options: { has_header: true, infer_schema_length: 10000 }

        target:
          path: "s3://clean-data/"
          format: "parquet"
          options: { use_pyarrow: true }
        ```

        Executing the configuration via the Python API:

        ```python
        from veridelta.config import load_config
        from veridelta.engine import DiffEngine

        diff_cfg, source, target = load_config("veridelta.yaml")
        summary = DiffEngine(diff_cfg).execute(source, target)
        ```
    """
    file_path = Path(path)

    if not file_path.is_file():
        raise ConfigError(f"Configuration file not found or is not a file: {file_path.absolute()}")

    try:
        with file_path.open("r", encoding="utf-8") as f:
            parsed_yaml: Any = yaml.safe_load(f)
    except yaml.YAMLError as yaml_err:
        raise ConfigError(f"Failed to parse YAML file:\n{yaml_err}") from yaml_err

    if not isinstance(parsed_yaml, dict):
        raise ConfigError("Invalid YAML structure: Root element must be a dictionary.")

    raw_config = cast("dict[str, Any]", parsed_yaml)
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
