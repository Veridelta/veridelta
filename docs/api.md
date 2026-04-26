# API Reference

The building blocks of Veridelta. Explore the configuration schemas, core diffing engine, and operational utilities.

## Configuration Models
Strict Pydantic models defining how Veridelta comparisons are structured. These can be instantiated programmatically or driven by declarative YAML.

::: veridelta.models
    options:
      show_root_heading: false
      show_source: true

## The Engine
The core mathematical evaluation engine and I/O orchestration, powered by the Rust-based Polars backend.

::: veridelta.engine
    options:
      show_root_heading: false
      show_source: true

## Configuration Parser
Utilities for loading, parsing, and validating YAML files into strictly typed configuration objects.

::: veridelta.config
    options:
      show_root_heading: false
      show_source: true

## Exceptions
The custom exception hierarchy. Consumers of the Python API should handle these specific errors to manage pipeline failures gracefully without silencing native Python runtime panics.

::: veridelta.exceptions
    options:
      show_root_heading: false
      show_source: true

## Datasets
Built-in data utilities with network-resilient caching for testing, onboarding, and tutorials.

::: veridelta.datasets
    options:
      show_root_heading: false
      show_source: true