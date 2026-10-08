# API reference

The public Python interface, generated from its docstrings.

## Configuration models

Pydantic models for a comparison and its sources. Build them in Python, or read them from YAML with `load_config`.

::: veridelta.models
    options:
      show_root_heading: false
      show_source: true

## Engine

`DiffEngine` loads, aligns, and compares two datasets, on Polars or inside a warehouse.

::: veridelta.engine
    options:
      show_root_heading: false
      show_source: true

## Configuration loading

Functions that read and check a YAML file, or build a configuration from two files, and return configuration models. The models themselves live in `veridelta.models`, which [Configuration models](#configuration-models) documents.

::: veridelta.config
    options:
      show_root_heading: false
      show_source: true
      members:
        - SCHEMA_URL
        - config_json_schema
        - files_config
        - load_config
        - referenced_variables

## Exceptions

The errors Veridelta raises. Each derives from `VerideltaError`, so one `except` clause catches them all without hiding Python's own errors.

::: veridelta.exceptions
    options:
      show_root_heading: false
      show_source: true

## Connectors

Warehouse sessions, lakehouse scanners, and the database and DuckDB readers. `VerideltaConnector` is the lifecycle they share, `ReaderConnector` and `PushdownSession` are the two kinds the engine drives, and `SQLPushdownCompiler` writes each dialect's comparison SQL.

::: veridelta.connectors
    options:
      show_root_heading: false
      show_source: true

## MCP server

The tools `veridelta mcp` serves, and the folder guard each one goes through. The SDK they run on comes with the `mcp` extra, and this module imports without it.

::: veridelta.mcp_server
    options:
      show_root_heading: false
      show_source: true

## Reports

Standalone HTML reports and Markdown summaries, rendered from a `DiffResult`.

::: veridelta.report
    options:
      show_root_heading: false
      show_source: true

## OpenTelemetry metrics

A run's counts, column drift, and verdict as OTLP/JSON metrics. See [OpenTelemetry metrics](results.md#opentelemetry-metrics).

::: veridelta.telemetry
    options:
      show_root_heading: false
      show_source: true

## Datasets

Sample datasets for the tutorials, downloaded once and cached.

::: veridelta.datasets
    options:
      show_root_heading: false
      show_source: true