# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Guard the configuration guide against drifting from the Pydantic models."""

from pathlib import Path

import pytest

from veridelta.models import (
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffRule,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
)

_GUIDE = Path(__file__).resolve().parents[2] / "docs" / "configuration.md"

_CONFIG_MODELS = (
    DiffConfig,
    DiffRule,
    SourceConfig,
    SnowflakeConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    IcebergConfig,
)


@pytest.mark.unit
@pytest.mark.fast
class TestConfigurationGuideCoverage:
    """Keep `docs/configuration.md` in lockstep with the config models."""

    def test_it_documents_every_config_field(self) -> None:
        """Ensure a new field cannot ship without appearing in the guide.

        A one-time audit goes stale the next time someone adds a field. Binding
        the guide to the models makes the gap fail the suite instead. Warehouse
        and lakehouse connection models are held to the same standard, since a
        credential or time-travel field nobody documents is one nobody can use.
        """
        guide = _GUIDE.read_text(encoding="utf-8")
        missing = [
            f"{model.__name__}.{field}"
            for model in _CONFIG_MODELS
            for field in model.model_fields
            if field not in guide
        ]

        assert missing == [], (
            "These configuration fields are missing from docs/configuration.md: "
            + ", ".join(missing)
        )
