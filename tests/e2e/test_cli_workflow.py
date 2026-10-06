# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-End integration tests for the Veridelta CLI."""

import contextlib
import json
import os
import sqlite3
import subprocess
from pathlib import Path
from urllib.parse import quote

import duckdb
import polars as pl
import pytest
from google.protobuf import json_format
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)

from tests.otlp_collector import running_collector

pytestmark = [pytest.mark.e2e]


def _sqlite_comparison(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    """Write a SQLite table and a CSV file that differ in one value, and a config comparing them.

    Returns:
        tuple[Path, dict[str, str]]: The configuration file, and the environment
            whose `LEGACY_DB_URI` names the database.
    """
    database = tmp_path / "legacy.db"
    with contextlib.closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("CREATE TABLE orders (id INTEGER, status TEXT)")
        connection.executemany("INSERT INTO orders VALUES (?, ?)", [(1, "open"), (2, "closed")])
    modern = tmp_path / "modern.csv"
    pl.DataFrame({"id": [1, 2], "status": ["open", "shipped"]}).write_csv(modern)

    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
source:
  type: database
  uri: ${{LEGACY_DB_URI}}
  table: orders
target:
  path: {modern}
primary_keys: [id]
""")
    return config_file, {**os.environ, "LEGACY_DB_URI": "sqlite://" + quote(str(database))}


class TestEndToEndCLIWorkflow:
    """Validate the entire Veridelta pipeline from YAML to artifact generation via subprocess."""

    def test_e2e_semantic_match_with_messy_data_returns_exit_code_zero(
        self, tmp_path: Path
    ) -> None:
        """Ensure the engine correctly resolves aliases, tolerances, and typing to find a match."""
        src_file = tmp_path / "source.csv"
        pl.DataFrame(
            {"legacy_id": [1, 2], "cost": ["$10.00", "$20.50"], "status": ["Active", "N/A"]}
        ).write_csv(src_file)

        tgt_file = tmp_path / "target.csv"
        pl.DataFrame(
            {"user_id": [1, 2], "cost": [10.02, 20.48], "status": ["Active", None]}
        ).write_csv(tgt_file)

        config_file = tmp_path / "config.yaml"
        config_file.write_text(f"""
source:
  path: {src_file}
  format: csv
target:
  path: {tgt_file}
  format: csv
primary_keys:
  - user_id
rules:
  - column_names: [legacy_id]
    rename_to: user_id
  - column_names: [cost]
    regex_replace:
      '\\$': ''
    cast_to: Float64
    absolute_tolerance: 0.05
  - column_names: [status]
    null_values: ["N/A"]
    treat_null_as_equal: true
""")

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0
        assert "Veridelta Execution Summary" in result.stdout
        assert "Total Issues:  0" in result.stdout

    def test_e2e_discrepancy_run_generates_correct_parquet_artifacts(self, tmp_path: Path) -> None:
        """Ensure failed comparisons exit with 1 and write the exact diffs to disk."""
        out_dir = tmp_path / "diff_output"

        src_file = tmp_path / "source.csv"
        pl.DataFrame({"id": [1, 2, 3], "val": ["A", "B", "C"]}).write_csv(src_file)

        tgt_file = tmp_path / "target.csv"
        pl.DataFrame({"id": [1, 2, 4], "val": ["A", "CHANGED", "D"]}).write_csv(tgt_file)

        config_file = tmp_path / "config.yaml"
        config_file.write_text(f"""
source:
  path: {src_file}
  format: csv
target:
  path: {tgt_file}
  format: csv
primary_keys:
  - id
output_path: {out_dir}
output_format: parquet
""")

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 1
        assert "Veridelta Execution Summary" in result.stdout
        assert "Artifacts saved to:" not in result.stdout
        assert "Artifacts saved to:" in result.stderr

        added_df = pl.read_parquet(out_dir / "added_rows.parquet")
        assert added_df.height == 1
        assert added_df.item(0, "id") == 4
        assert added_df.item(0, "val") == "D"

        removed_df = pl.read_parquet(out_dir / "removed_rows.parquet")
        assert removed_df.height == 1
        assert removed_df.item(0, "id") == 3
        assert removed_df.item(0, "val") == "C"

        changed_df = pl.read_parquet(out_dir / "changed_rows.parquet")
        assert changed_df.height == 1
        assert changed_df.item(0, "id") == 2
        assert changed_df.item(0, "val_source") == "B"
        assert changed_df.item(0, "val_target") == "CHANGED"
        assert changed_df.item(0, "val_is_match") is False

    def test_e2e_otel_metrics_describe_the_run(self, tmp_path: Path) -> None:
        """Ensure `--otel` writes an OTLP export a collector accepts, tagged from the environment."""
        src_file = tmp_path / "source.csv"
        pl.DataFrame({"id": [1, 2, 3], "val": ["A", "B", "C"]}).write_csv(src_file)
        tgt_file = tmp_path / "target.csv"
        pl.DataFrame({"id": [1, 2, 4], "val": ["A", "CHANGED", "D"]}).write_csv(tgt_file)
        config_file = tmp_path / "config.yaml"
        config_file.write_text(f"""
source:
  path: {src_file}
target:
  path: {tgt_file}
primary_keys: [id]
""")
        metrics_file = tmp_path / "telemetry" / "otel-metrics.json"

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file), "--json", "--otel", str(metrics_file)],
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                "OTEL_SERVICE_NAME": "orders-migration",
                "OTEL_RESOURCE_ATTRIBUTES": "deployment.environment=ci,team=data%20platform",
            },
        )

        assert result.returncode == 1, result.stderr
        assert '"is_match": false' in result.stdout
        assert "OpenTelemetry metrics saved to:" in result.stderr
        request = json_format.Parse(
            metrics_file.read_text(encoding="utf-8"), ExportMetricsServiceRequest()
        )
        (resource_metrics,) = request.resource_metrics
        attributes = {a.key: a.value.string_value for a in resource_metrics.resource.attributes}
        assert attributes["service.name"] == "orders-migration"
        assert attributes["deployment.environment"] == "ci"
        assert attributes["team"] == "data platform"
        assert attributes["veridelta.config.path"] == str(config_file)
        assert attributes["veridelta.source.name"] == str(src_file)
        metrics = {m.name: m for m in resource_metrics.scope_metrics[0].metrics}
        kinds = {
            point.attributes[0].value.string_value: point.as_int
            for point in metrics["veridelta.diff.rows"].gauge.data_points
        }
        assert kinds == {"added": 1, "removed": 1, "changed": 1}
        assert metrics["veridelta.diff.match"].gauge.data_points[0].as_int == 0

    def test_e2e_otel_send_posts_the_run_to_a_collector(self, tmp_path: Path) -> None:
        """Ensure `--otel-send` posts an export a collector accepts, with its headers, unprinted."""
        src_file = tmp_path / "source.csv"
        pl.DataFrame({"id": [1, 2], "val": ["A", "B"]}).write_csv(src_file)
        tgt_file = tmp_path / "target.csv"
        pl.DataFrame({"id": [1, 2], "val": ["A", "X"]}).write_csv(tgt_file)
        config_file = tmp_path / "config.yaml"
        config_file.write_text(f"""
source:
  path: {src_file}
target:
  path: {tgt_file}
primary_keys: [id]
""")
        env = {name: value for name, value in os.environ.items() if not name.startswith("OTEL_")}

        with running_collector() as collector:
            result = subprocess.run(
                ["veridelta", "run", "-c", str(config_file), "--json", "--otel-send"],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **env,
                    "OTEL_EXPORTER_OTLP_ENDPOINT": collector.url,
                    "OTEL_EXPORTER_OTLP_HEADERS": "authorization=Bearer%20s3cret-token",
                    "NO_PROXY": "127.0.0.1,localhost",
                },
            )

        assert result.returncode == 1, result.stderr
        assert f"OpenTelemetry metrics sent to: {collector.url}/v1/metrics" in result.stderr
        (request,) = collector.received
        assert request.headers["authorization"] == "Bearer s3cret-token"
        json_format.Parse(request.body.decode(), ExportMetricsServiceRequest())
        assert "s3cret" not in result.stdout + result.stderr

    def test_e2e_config_reads_references_from_the_environment(self, tmp_path: Path) -> None:
        """Ensure `${NAME}` in the source and target blocks resolves from the CLI's environment."""
        frame = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        frame.write_csv(tmp_path / "source.csv")
        frame.write_csv(tmp_path / "target.csv")

        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
source:
  path: ${VERIDELTA_E2E_DIR}/source.csv
target:
  path: ${VERIDELTA_E2E_DIR}/target.csv
primary_keys: [id]
""")

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "VERIDELTA_E2E_DIR": str(tmp_path)},
        )

        assert result.returncode == 0
        assert "Total Issues:  0" in result.stdout

    def test_e2e_database_source_reads_its_uri_from_the_environment(self, tmp_path: Path) -> None:
        """Ensure a database source connects through a URI the environment supplies."""
        config_file, env = _sqlite_comparison(tmp_path)

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )

        assert result.returncode == 1, result.stderr
        assert "Changed:       1" in result.stdout
        assert "veridelta.connectors" not in result.stderr

    def test_e2e_verbose_logs_each_read_on_stderr(self, tmp_path: Path) -> None:
        """Ensure `--verbose` prints the read's log line on stderr, and stdout keeps the JSON."""
        config_file, env = _sqlite_comparison(tmp_path)

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file), "--json", "--verbose"],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )

        assert result.returncode == 1, result.stderr
        assert json.loads(result.stdout)["is_match"] is False
        assert "INFO veridelta.connectors.database: Read 2 rows of table 'orders'" in result.stderr

    def test_e2e_duckdb_source_reads_a_file_named_in_the_configuration(
        self, tmp_path: Path
    ) -> None:
        """Ensure `veridelta run` reads a DuckDB file's table and compares it locally."""
        database = tmp_path / "legacy.duckdb"
        with duckdb.connect(str(database)) as connection:
            connection.execute(
                "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'open'), (2, 'closed')) "
                "AS t(id, status)"
            )
        modern = tmp_path / "modern.csv"
        pl.DataFrame({"id": [1, 2], "status": ["open", "shipped"]}).write_csv(modern)

        config_file = tmp_path / "config.yaml"
        config_file.write_text(f"""
source:
  type: duckdb
  database: {database}
  table: orders
target:
  path: {modern}
primary_keys: [id]
""")

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 1, result.stderr
        assert "Changed:       1" in result.stdout

    def test_e2e_delta_table_is_compared_with_a_csv_file(self, tmp_path: Path) -> None:
        """Ensure `veridelta run` reads a Delta table named in the configuration."""
        legacy = tmp_path / "legacy_orders"
        pl.DataFrame({"id": [1, 2], "status": ["open", "closed"]}).write_delta(legacy)
        modern = tmp_path / "modern.csv"
        pl.DataFrame({"id": [1, 2], "status": ["open", "shipped"]}).write_csv(modern)
        config_file = tmp_path / "config.yaml"
        config_file.write_text(f"""
source:
  type: delta
  table_uri: {legacy}
target:
  path: {modern}
primary_keys: [id]
""")

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 1, result.stderr
        assert "Changed:       1" in result.stdout

    def test_e2e_missing_delta_table_is_named_not_reported_as_a_bug(self, tmp_path: Path) -> None:
        """Ensure a table that does not exist fails as a scan, with its URI.

        Polars defers the scan, so the error used to surface mid-run, and the
        CLI asked the user to report it as a bug in Veridelta.
        """
        modern = tmp_path / "modern.csv"
        pl.DataFrame({"id": [1], "status": ["open"]}).write_csv(modern)
        config_file = tmp_path / "config.yaml"
        config_file.write_text(f"""
source:
  type: delta
  table_uri: {tmp_path / "missing"}
target:
  path: {modern}
primary_keys: [id]
""")

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 3
        assert "ConnectorError" in result.stderr
        assert "Delta Lake scan of" in result.stderr
        assert "Unexpected System Error" not in result.stderr

    def test_e2e_missing_key_names_the_format_the_file_was_read_as(self, tmp_path: Path) -> None:
        """Ensure a `.parquet` path with no `format` fails naming the cause: it was read as CSV.

        `format` defaults to `csv`, so the key is not among the columns read. The message
        used to name only the missing key, which sent the user to the wrong setting.
        """
        legacy, modern = tmp_path / "legacy.parquet", tmp_path / "modern.parquet"
        pl.DataFrame({"id": [1], "status": ["open"]}).write_parquet(legacy)
        pl.DataFrame({"id": [1], "status": ["open"]}).write_parquet(modern)
        config_file = tmp_path / "config.yaml"
        config_file.write_text(f"""
source:
  path: {legacy}
target:
  path: {modern}
primary_keys: [id]
""")

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 3
        assert "Configuration Error" in result.stderr
        assert f"`{legacy}` read as csv since `format` is not set" in result.stderr

    def test_e2e_unset_environment_variable_is_a_configuration_error(self, tmp_path: Path) -> None:
        """Ensure a missing variable stops the run with a configuration error that names it."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
source:
  path: ${VERIDELTA_E2E_UNSET}/source.csv
target:
  path: target.csv
primary_keys: [id]
""")

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
            env={key: value for key, value in os.environ.items() if key != "VERIDELTA_E2E_UNSET"},
        )

        assert result.returncode == 3
        assert "Configuration Error" in result.stderr
        assert "'VERIDELTA_E2E_UNSET' is not set, but source -> path references it" in result.stderr

    def test_e2e_a_failure_under_json_is_one_json_object_on_stdout(self, tmp_path: Path) -> None:
        """Ensure a script reading `run --json` parses a failure, and tells it from drift.

        The error goes to stdout as one object, the explanation stays on stderr,
        and the exit code is 3, never drift's 1.
        """
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
source:
  path: ${VERIDELTA_E2E_UNSET}/source.csv
target:
  path: target.csv
primary_keys: [id]
""")

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file), "--json", "--quiet"],
            capture_output=True,
            text=True,
            check=False,
            env={key: value for key, value in os.environ.items() if key != "VERIDELTA_E2E_UNSET"},
        )

        assert result.returncode == 3
        payload = json.loads(result.stdout)
        assert list(payload) == ["error"]
        assert payload["error"]["type"] == "ConfigError"
        assert "'VERIDELTA_E2E_UNSET' is not set" in payload["error"]["message"]
        assert "Configuration Error" in result.stderr

    def test_e2e_crosswalk_proposals_make_the_comparison_pass(self, tmp_path: Path) -> None:
        """Ensure the rules `crosswalk` prints, pasted into the config, clear the drift."""
        ids = list(range(12))
        pl.DataFrame({"id": ids, "gender": ["M"] * 6 + ["F"] * 6}).write_csv(
            tmp_path / "source.csv"
        )
        pl.DataFrame({"id": ids, "gender": ["Male"] * 6 + ["Female"] * 6}).write_csv(
            tmp_path / "target.csv"
        )
        config = (
            f"source:\n  path: {tmp_path / 'source.csv'}\n"
            f"target:\n  path: {tmp_path / 'target.csv'}\n"
            "primary_keys: [id]\n"
        )
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config)

        proposed = subprocess.run(
            ["veridelta", "crosswalk", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
        )
        mapped_file = tmp_path / "mapped.yaml"
        mapped_file.write_text(config + proposed.stdout)
        result = subprocess.run(
            ["veridelta", "run", "-c", str(mapped_file)],
            capture_output=True,
            text=True,
            check=False,
        )

        assert proposed.returncode == 0
        assert "'M' -> 'Male': 6 of 6 rows (100.0%)" in proposed.stderr
        assert result.returncode == 0
        assert "Total Issues:  0" in result.stdout

    def test_e2e_validate_checks_a_configuration_without_its_secrets(self, tmp_path: Path) -> None:
        """Ensure `validate` exits 0 for a sound file, and 1 once a rule cannot run."""
        config_file = tmp_path / "config.yaml"
        config = (
            "source:\n  path: ${VERIDELTA_E2E_UNSET}/source.csv\n"
            "target:\n  path: target.csv\n"
            "primary_keys: [id]\n"
        )
        config_file.write_text(config)
        env = {key: value for key, value in os.environ.items() if key != "VERIDELTA_E2E_UNSET"}

        strict = subprocess.run(
            ["veridelta", "validate", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
        lenient = subprocess.run(
            ["veridelta", "validate", "-c", str(config_file), "--allow-missing-env"],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
        config_file.write_text(
            config + "rules:\n  - column_names: [name]\n    regex_replace: {'(?<=Mr)\\.': ''}\n"
        )
        broken = subprocess.run(
            ["veridelta", "validate", "-c", str(config_file), "--allow-missing-env", "-q"],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )

        assert strict.returncode == 1
        assert "error: Environment variable 'VERIDELTA_E2E_UNSET' is not set" in strict.stdout
        assert lenient.returncode == 0
        assert lenient.stdout.startswith("warning: Environment variable 'VERIDELTA_E2E_UNSET'")
        assert lenient.stderr.endswith("valid, with 1 warning.\n")
        assert broken.returncode == 1
        assert "error: rules[0] regex_replace pattern" in broken.stdout
        assert broken.stderr == ""
