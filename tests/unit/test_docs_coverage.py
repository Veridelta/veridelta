# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Guard the user guide against drifting from the Pydantic models."""

from pathlib import Path

import pytest
from pydantic import BaseModel

from veridelta.models import (
    BigQueryConfig,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffRule,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
)

_DOCS = Path(__file__).resolve().parents[2] / "docs"

_PAGES: dict[str, tuple[type[BaseModel], ...]] = {
    "configuration.md": (DiffConfig,),
    "rules.md": (DiffRule,),
    "sources.md": (
        SourceConfig,
        SnowflakeConfig,
        DatabricksConfig,
        BigQueryConfig,
        DeltaLakeConfig,
        IcebergConfig,
        DatabaseConfig,
    ),
}
"""Each guide page and the models whose every field it must name."""


@pytest.mark.unit
@pytest.mark.fast
class TestUserGuideCoverage:
    """Keep the user guide in lockstep with the config models."""

    @pytest.mark.parametrize("page", sorted(_PAGES))
    def test_it_documents_every_config_field(self, page: str) -> None:
        """Ensure a new field cannot ship without appearing on its guide page.

        A one-time audit goes stale the next time someone adds a field. Binding
        each page to its models makes the gap fail the suite instead. Warehouse,
        lakehouse, and database connection models are held to the same standard,
        since a credential or time-travel field nobody documents is one nobody
        can use.
        """
        text = (_DOCS / page).read_text(encoding="utf-8")
        missing = [
            f"{model.__name__}.{field}"
            for model in _PAGES[page]
            for field in model.model_fields
            if field not in text
        ]

        assert missing == [], f"These fields are missing from docs/{page}: " + ", ".join(missing)
