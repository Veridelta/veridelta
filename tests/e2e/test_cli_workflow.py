# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-End integration tests for the Veridelta CLI."""

import contextlib
import os
import sqlite3
import subprocess
from pathlib import Path
from urllib.parse import quote

import polars as pl
import pytest


@pytest.mark.e2e
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

        result = subprocess.run(
            ["veridelta", "run", "-c", str(config_file)],
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "LEGACY_DB_URI": "sqlite://" + quote(str(database))},
        )

        assert result.returncode == 1, result.stderr
        assert "Changed:       1" in result.stdout

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

        assert result.returncode == 1
        assert "Configuration Error" in result.stderr
        assert "'VERIDELTA_E2E_UNSET' is not set, but source -> path references it" in result.stderr

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
