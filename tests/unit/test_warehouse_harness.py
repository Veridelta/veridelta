# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Pin which tables the live warehouse harness may drop.

The harness drops leftover tables in a maintainer's own warehouse accounts, so
it must recognize only names it made itself, and only once they are a day old.
"""

from datetime import UTC, datetime, timedelta

import pytest

from tests.integration import warehouse_harness
from tests.integration.warehouse_harness import STALE_AFTER, _table_names, is_stale

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
            name: name for name in ("bigquery", "databricks", "motherduck", "snowflake")
        }
