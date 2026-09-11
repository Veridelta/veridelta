# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Guard the configuration guide against drifting from the Pydantic models."""

from pathlib import Path

import pytest

from veridelta.models import DiffConfig, DiffRule, SourceConfig

_GUIDE = Path(__file__).resolve().parents[2] / "docs" / "configuration.md"


@pytest.mark.unit
@pytest.mark.fast
class TestConfigurationGuideCoverage:
    """Keep `docs/configuration.md` in lockstep with the config models."""

    def test_it_documents_every_config_field(self) -> None:
        """Ensure a new field cannot ship without appearing in the guide.

        A one-time audit goes stale the next time someone adds a field. Binding
        the guide to the models makes the gap fail the suite instead.
        """
        guide = _GUIDE.read_text(encoding="utf-8")
        missing = [
            f"{model.__name__}.{field}"
            for model in (DiffConfig, DiffRule, SourceConfig)
            for field in model.model_fields
            if field not in guide
        ]

        assert missing == [], (
            "These configuration fields are missing from docs/configuration.md: "
            + ", ".join(missing)
        )
