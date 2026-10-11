# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Pin which tables the live warehouse harness may drop.

The harness drops leftover tables in a maintainer's own warehouse accounts, so
it must recognize only names it made itself, and only once they are a day old.
"""

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from tests.integration import warehouse_harness
from tests.integration.warehouse_harness import STALE_AFTER, _table_names, is_stale
from veridelta.models import DatabaseConfig

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_NOW = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


class TestStaleTables:
    """Validate the names `drop_stale_tables` drops."""

    def test_it_recognizes_the_names_the_harness_makes(self) -> None:
        """Ensure a table the harness makes is dropped once it is older than a day."""
        made = _NOW - STALE_AFTER - timedelta(minutes=1)

        assert [is_stale(name, _NOW) for name in _table_names(made)] == [True, True]

    def test_it_keeps_a_table_a_running_suite_may_still_use(self) -> None:
        """Ensure a run never drops the tables of another run still under way."""
        made = _NOW - STALE_AFTER + timedelta(minutes=1)

        assert [is_stale(name, _NOW) for name in _table_names(made)] == [False, False]

    @pytest.mark.parametrize(
        "name",
        [
            "orders",
            "vd_keep_me",
            "vd_202610010000_source",
            "vd_202610010000_1a2b3c4d_backup",
            "VD_202610010000_1A2B3C4D_SOURCE",
            "vd_202610010000_1a2b3c4d_source_copy",
        ],
    )
    def test_it_never_drops_a_table_it_did_not_make(self, name: str) -> None:
        """Ensure an old table of any other name survives, `vd_` prefix or not."""
        assert not is_stale(name, _NOW)

    def test_it_names_one_backend_for_each_live_warehouse(self) -> None:
        """Ensure each service answers to the value of `VERIDELTA_PARITY_BACKEND` it is keyed by."""
        assert {name: service.name for name, service in warehouse_harness.SERVICES.items()} == {
            name: name for name in ("bigquery", "databricks", "motherduck", "redshift", "snowflake")
        }


class TestRedshiftService:
    """Validate how the Redshift service describes and stores its tables, with no cluster."""

    @pytest.fixture
    def service(self, monkeypatch: pytest.MonkeyPatch) -> warehouse_harness._Redshift:
        """Build the service from settings a workflow would pass."""
        for name, value in {
            "REDSHIFT_HOST": "default.123.us-east-1.redshift-serverless.amazonaws.com",
            "REDSHIFT_DATABASE": "dev",
            "REDSHIFT_USER": "veridelta@live",
            "REDSHIFT_PASSWORD": "p@ss:w/rd",
        }.items():
            monkeypatch.setenv(name, value)
        monkeypatch.delenv("REDSHIFT_PORT", raising=False)
        return warehouse_harness._Redshift()

    def test_a_table_is_a_redshift_source_that_sets_pushdown(
        self, service: warehouse_harness._Redshift
    ) -> None:
        """Ensure the pair routes to the Redshift session, with the password kept apart."""
        source = service.source("vd_202610010000_1a2b3c4d_source")

        assert isinstance(source, DatabaseConfig)
        assert source.uri == (
            "redshift://veridelta%40live@"
            "default.123.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
        )
        assert source.password == "p@ss:w/rd"
        assert source.pushdown is True

    def test_text_is_stored_without_a_length_limit(
        self, service: warehouse_harness._Redshift
    ) -> None:
        """Ensure no column is TEXT, which Redshift would cut to 256 bytes."""
        assert service._column_type("name", pl.String()) == "VARCHAR(MAX)"
        assert service._column_type("at", pl.Datetime("us", "UTC")) == "TIMESTAMPTZ"
        assert service._column_type("amount", pl.Decimal(12, 2)) == "DECIMAL(12, 2)"

    def test_a_dtype_redshift_cannot_hold_exactly_is_refused(
        self, service: warehouse_harness._Redshift
    ) -> None:
        """Ensure nanoseconds, which Redshift would round, fail the case instead."""
        with pytest.raises(NotImplementedError, match="Redshift has no column type"):
            service._column_type("at", pl.Datetime("ns"))
