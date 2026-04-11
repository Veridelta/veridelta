# Configuration Guide

Veridelta is driven by a configuration object (or YAML file) that defines how two datasets should be aligned and compared.

## Core Settings

The following fields are required for every comparison:

| Field | Description |
| :--- | :--- |
| `primary_keys` | A list of columns used to join and align the datasets (e.g., `['id']`). |
| `source` | Configuration for the "Legacy" or "Left" dataset. |
| `target` | Configuration for the "Modern" or "Right" dataset. |

## Schema Modes

The `schema_mode` determines how Veridelta handles columns that don't match between datasets.

- `intersection` (Default): Only compare columns present in both datasets.
- `exact`: Fail if columns or their order do not match perfectly.
- `allow_additions`: Allow the Target to have new columns not found in the Source.
- `allow_removals`: Allow the Target to drop columns found in the Source.

## Column Rules

Rules allow you to define tolerances for specific columns or patterns.

### Numeric Tolerances
Use these to ignore floating-point jitter in financial or scientific data.

```yaml
rules:
  - column_names: ["total_amount"]
    absolute_tolerance: 0.01
    relative_tolerance: 0.005
```

### String Normalization

Handle messy text data by cleaning it before the comparison.

```yaml
rules:
  - column_names: ["user_email"]
    case_insensitive: true
    whitespace_mode: "both"
    regex_replace:
      "\\.com$": ".net"  # Example regex sanitization
```

### Value Mapping (Crosswalks)

Translate legacy Enums to modern values.

```yaml
rules:
  - column_names: ["status_code"]
    value_map:
      "0": "INACTIVE"
      "1": "ACTIVE"
```