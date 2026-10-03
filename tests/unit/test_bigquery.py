# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for BigQuery: its connection model, SQL dialect, and connector."""

from typing import Any

import pytest
from pydantic import ValidationError

from veridelta.models import BigQueryConfig

_BASE: dict[str, Any] = {"project": "analytics-prod", "table": "sales.orders"}
"""The fields every BigQuery config needs."""


@pytest.mark.unit
@pytest.mark.fast
class TestBigQueryConfig:
    """Validate BigQuery connection settings."""

    def test_it_accepts_a_dataset_qualified_table(self) -> None:
        """Ensure the common shape loads, with every optional field unset."""
        config = BigQueryConfig(**_BASE)

        assert config.type == "bigquery"
        assert (config.dataset, config.location, config.credentials_path) == (None, None, None)
        assert config.maximum_bytes_billed is None

    def test_it_reads_a_bare_table_from_the_default_dataset(self) -> None:
        """Ensure a one-segment table is allowed once a dataset is named."""
        config = BigQueryConfig(project="analytics-prod", table="orders", dataset="sales")

        assert (config.dataset, config.table) == ("sales", "orders")

    def test_it_needs_a_dataset_for_a_bare_table(self) -> None:
        """Ensure a table BigQuery could not resolve fails when the config loads."""
        with pytest.raises(ValidationError, match="needs 'dataset'"):
            BigQueryConfig(project="analytics-prod", table="orders")

    @pytest.mark.parametrize(
        "project",
        [
            pytest.param("Analytics-prod", id="uppercase"),
            pytest.param("short", id="too-short"),
            pytest.param("analytics-prod-", id="trailing-hyphen"),
            pytest.param("1analytics", id="leading-digit"),
            pytest.param("example.com:analytics", id="domain-scoped"),
        ],
    )
    def test_it_refuses_a_project_id_google_would_not_issue(self, project: str) -> None:
        """Ensure only lowercase project ids of six to thirty characters pass."""
        with pytest.raises(ValidationError, match="project"):
            BigQueryConfig(**{**_BASE, "project": project})

    @pytest.mark.parametrize(
        "table",
        [
            pytest.param("project.sales.orders", id="three-segments"),
            pytest.param("sales.order-lines", id="hyphen"),
            pytest.param("1sales.orders", id="leading-digit"),
        ],
    )
    def test_it_refuses_tables_outside_the_identifier_allowlist(self, table: str) -> None:
        """Ensure the table is one or two plain identifiers, so it can be quoted safely."""
        with pytest.raises(ValidationError, match="table"):
            BigQueryConfig(**{**_BASE, "table": table})

    @pytest.mark.parametrize("limit", [0, "1000", True, 1.5])
    def test_it_takes_a_byte_cap_only_as_a_positive_integer(self, limit: object) -> None:
        """Ensure the cost cap cannot be coerced from text, a bool, or a float."""
        with pytest.raises(ValidationError, match="maximum_bytes_billed"):
            BigQueryConfig(**{**_BASE, "maximum_bytes_billed": limit})

    def test_it_keeps_the_key_file_path_out_of_its_repr(self) -> None:
        """Ensure a printed config does not reveal where the service account key lives."""
        config = BigQueryConfig(**_BASE, credentials_path="/secrets/bq-key.json")

        assert "bq-key" not in repr(config)
        assert config.model_dump()["credentials_path"] == "/secrets/bq-key.json"
