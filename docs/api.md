# API Reference

The building blocks of Veridelta. Explore the configuration schemas, column rules, and the core diffing engine.

## Configuration Models
These models define how you configure a Veridelta comparison. You can pass these directly via Python, or define them in a YAML file.

::: veridelta.models
    options:
      show_root_heading: false
      show_source: true

## The Engine
The core mathematical diffing engine, powered by Polars.

::: veridelta.engine
    options:
      show_root_heading: false
      show_source: true