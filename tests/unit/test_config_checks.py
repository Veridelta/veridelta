# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for `DiffEngine.check_configs`, the offline half of `veridelta validate`."""

from typing import Any

import pytest
from pytest_mock import MockerFixture

from veridelta.engine import DiffEngine
from veridelta.models import (
    ConfigFinding,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffRule,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
)


def _snowflake(table: str, **overrides: Any) -> SnowflakeConfig:
    """Build a Snowflake side on one shared connection."""
    fields: dict[str, Any] = {
        "account": "xy12345",
        "user": "analyst",
        "warehouse": "COMPUTE_WH",
        "database": "ANALYTICS",
        "schema_name": "PUBLIC",
        "table": table,
        **overrides,
    }
    return SnowflakeConfig(**fields)


def _databricks(table: str) -> DatabricksConfig:
    """Build a Databricks side."""
    return DatabricksConfig(
        server_hostname="adb.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc",
        catalog="main",
        schema_name="default",
        table=table,
    )


_CSV = SourceConfig(path="a.csv")
_PARQUET = SourceConfig(path="b.parquet", format="parquet")


def _check(source: SourceRef, target: SourceRef, **diff: Any) -> list[tuple[str, str]]:
    """Run the checks and flatten each finding to (severity, message)."""
    config = DiffConfig(primary_keys=["id"], **diff)
    return [
        (finding.severity, finding.message)
        for finding in DiffEngine.check_configs(config, source, target)
    ]


@pytest.fixture
def drivers(mocker: MockerFixture) -> None:
    """Install stand-ins for every optional driver, so only the check under test fires."""
    mocker.patch("veridelta.connectors.warehouse.snowflake_connector", object())
    mocker.patch("veridelta.connectors.warehouse.databricks_sql", object())
    mocker.patch("veridelta.connectors.database.connectorx", object())
    mocker.patch("veridelta.engine.fastexcel", object())
    mocker.patch("veridelta.engine.rapidfuzz_distance", object())
    mocker.patch("veridelta.engine.find_spec", return_value=object())


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.usefixtures("drivers")
class TestConfigChecks:
    """Validate what `check_configs` reports, and that it never connects."""

    def test_it_finds_nothing_wrong_with_a_plain_file_pair(self) -> None:
        """Ensure a configuration that will run reports no findings."""
        assert _check(_CSV, _PARQUET) == []

    def test_it_reports_a_pair_no_engine_can_compare(self, mocker: MockerFixture) -> None:
        """Ensure pairing errors surface as findings, before any connector is built."""
        connector = mocker.patch("veridelta.engine.SnowflakeConnector")

        findings = _check(_snowflake("SRC"), _CSV)

        assert findings == [
            ("error", "Mixed file/lakehouse/database and warehouse backends are unsupported.")
        ]
        connector.assert_not_called()

    def test_it_reports_a_table_compared_with_itself(self) -> None:
        """Ensure the same-table refusal reads the same as it does in a run."""
        [(severity, message)] = _check(_snowflake("SRC"), _snowflake("SRC"))

        assert severity == "error"
        assert "both name the same table 'SRC'" in message

    @pytest.mark.parametrize(
        ("source", "patched", "extra"),
        [
            pytest.param(
                SourceConfig(path="a.xlsx", format="excel"),
                "veridelta.engine.fastexcel",
                "excel",
                id="excel",
            ),
            pytest.param(
                DatabaseConfig(uri="sqlite:///legacy.db", table="orders"),
                "veridelta.connectors.database.connectorx",
                "database",
                id="database",
            ),
        ],
    )
    def test_it_reports_a_missing_extra(
        self, mocker: MockerFixture, source: SourceRef, patched: str, extra: str
    ) -> None:
        """Ensure a side whose reader is not installed fails here instead of mid-run."""
        mocker.patch(patched, None)

        assert _check(source, _CSV) == [
            (
                "error",
                f"Reading the source needs the optional '{extra}' extra, which is not "
                f"installed. Install it with: uv add 'veridelta[{extra}]'",
            )
        ]

    @pytest.mark.parametrize(
        ("source", "module", "extra"),
        [
            pytest.param(
                DeltaLakeConfig(table_uri="s3://lake/a"), "deltalake", "delta", id="delta"
            ),
            pytest.param(
                IcebergConfig(table_uri="s3://lake/a/metadata.json"),
                "pyiceberg",
                "iceberg",
                id="iceberg",
            ),
        ],
    )
    def test_it_looks_up_lakehouse_extras_without_importing_them(
        self, mocker: MockerFixture, source: SourceRef, module: str, extra: str
    ) -> None:
        """Ensure a lakehouse reader is found by name, so checking never imports it."""
        find_spec = mocker.patch("veridelta.engine.find_spec", return_value=None)

        [(severity, message)] = _check(source, _CSV)

        assert severity == "error"
        assert f"'veridelta[{extra}]'" in message
        find_spec.assert_called_once_with(module)

    @pytest.mark.parametrize(
        ("source", "target", "patched", "extra"),
        [
            pytest.param(
                _snowflake("SRC"),
                _snowflake("TGT"),
                "veridelta.connectors.warehouse.snowflake_connector",
                "snowflake",
                id="snowflake",
            ),
            pytest.param(
                _databricks("main.default.src"),
                _databricks("main.default.tgt"),
                "veridelta.connectors.warehouse.databricks_sql",
                "databricks",
                id="databricks",
            ),
        ],
    )
    def test_it_reports_a_missing_warehouse_extra_once(
        self,
        mocker: MockerFixture,
        source: SourceRef,
        target: SourceRef,
        patched: str,
        extra: str,
    ) -> None:
        """Ensure both sides needing one extra make one finding that names both."""
        mocker.patch(patched, None)

        assert _check(source, target) == [
            (
                "error",
                f"Reading the source and target needs the optional '{extra}' extra, which "
                f"is not installed. Install it with: uv add 'veridelta[{extra}]'",
            )
        ]

    def test_it_reports_a_similarity_rule_without_the_fuzzy_extra(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a local run's scorer is checked for each rule that needs it."""
        mocker.patch("veridelta.engine.rapidfuzz_distance", None)
        rules = [
            DiffRule(column_names=["amount"], absolute_tolerance=0.1),
            DiffRule(column_names=["name"], max_levenshtein_distance=1),
            DiffRule(column_names=["city"], min_jaro_winkler_similarity=0.9),
        ]

        findings = _check(_CSV, _PARQUET, rules=rules)

        assert [severity for severity, _ in findings] == ["error", "error"]
        assert findings[0][1].startswith("rules[1] sets a similarity limit")
        assert findings[1][1].startswith("rules[2] sets a similarity limit")
        assert "uv add 'veridelta[fuzzy]'" in findings[0][1]

    def test_it_needs_no_scorer_where_the_warehouse_computes_edit_distance(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a pushdown run's Levenshtein limit, which compiles to SQL, needs no extra."""
        mocker.patch("veridelta.engine.rapidfuzz_distance", None)
        rules = [DiffRule(column_names=["NAME"], max_levenshtein_distance=1)]

        assert _check(_snowflake("SRC"), _snowflake("TGT"), rules=rules) == []

    def test_it_reports_a_regex_polars_cannot_compile_on_a_local_run(self) -> None:
        """Ensure a pattern Python accepts but Polars rejects fails here, not mid-run."""
        rules = [DiffRule(column_names=["name"], regex_replace={"(?<=Mr)\\.": "", "-": ""})]

        [(severity, message)] = _check(_CSV, _PARQUET, rules=rules)

        assert severity == "error"
        assert message.startswith("rules[0] regex_replace pattern '(?<=Mr)\\\\.' does not")
        assert "look-around" in message

    def test_it_warns_about_that_regex_on_a_warehouse_run(self) -> None:
        """Ensure pushdown only warns: the warehouse's own regex engine may accept it."""
        rules = [DiffRule(column_names=["NAME"], regex_replace={"(?<=Mr)\\.": ""})]

        [(severity, message)] = _check(_databricks("a"), _databricks("b"), rules=rules)

        assert severity == "warning"
        assert "warehouse's own regular expression engine" in message

    def test_it_warns_about_settings_a_warehouse_refuses_by_column_type(self) -> None:
        """Ensure refusals that depend on stored names and types are warnings, in order."""
        rules = [
            DiffRule(column_names=["NAME"], min_jaro_winkler_similarity=0.9),
            DiffRule(column_names=["SEEN_AT"], datetime_format="%d %b %Y"),
            DiffRule(column_names=["BORN_ON"], datetime_format="%Y-%m-%d"),
        ]

        findings = _check(
            _snowflake("SRC"), _snowflake("TGT"), rules=rules, normalize_column_names=True
        )

        assert [severity for severity, _ in findings] == ["warning"] * 3
        assert findings[0][1].startswith("normalize_column_names is on.")
        assert findings[1][1].startswith("rules[0] sets min_jaro_winkler_similarity")
        assert findings[2][1].startswith("rules[1] datetime_format")
        assert "%b" in findings[2][1]
        assert "Snowflake" in findings[2][1]

    def test_it_reports_a_database_table_it_cannot_quote(self) -> None:
        """Ensure an unknown scheme with `table` fails here, and `query` is left alone."""
        findings = _check(
            DatabaseConfig(uri="exasol://analyst@db.internal/sales", table="orders"),
            DatabaseConfig(uri="exasol://analyst@db.internal/sales", query="SELECT 1"),
        )

        [(severity, message)] = findings
        assert severity == "error"
        assert message.startswith("source: 'table' needs a URI scheme")
        assert "'exasol'" in message

    def test_it_returns_findings_as_models(self) -> None:
        """Ensure the Python API hands back the public model, ready to serialize."""
        [finding] = DiffEngine.check_configs(
            DiffConfig(primary_keys=["id"]), _snowflake("SRC"), _CSV
        )

        assert isinstance(finding, ConfigFinding)
        assert finding.model_dump() == {
            "severity": "error",
            "message": "Mixed file/lakehouse/database and warehouse backends are unsupported.",
        }
