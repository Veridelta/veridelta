# Troubleshooting

Each section starts from what you see, then gives the cause and the change that fixes it. A command that cannot finish exits `3`. It prints the error's type and message on stderr, and with `--json`, as one object on stdout; see [Exit codes](../cli.md#exit-codes).

## Find the cause first

- `veridelta validate -c veridelta.yaml` checks the file without reading a row. With `--schemas`, it also reads each side's columns and checks every rule against them.
- The error's type says where to look. A `ConfigError` is in the configuration, a `ConnectorError` comes from a source, and a `DataIntegrityError` comes from the data. Any other type is a bug: please [report it](https://github.com/Veridelta/veridelta/issues).
- `-v` logs each file opened, each connection, and each statement on stderr.

## An extra is not installed

```text
Reading the source needs the optional 'snowflake' extra, which is not installed. Install it with: uv add 'veridelta[snowflake]'
```

The core package reads files. Every other kind of source, and Excel files, reads through an optional extra. Install the one the message names, with `uv add 'veridelta[snowflake]'` or `pip install 'veridelta[snowflake]'`. `veridelta validate` reports every missing extra without connecting. [Sources](../sources.md) names the extra for each kind of source.

## A primary key repeats

```text
DataIntegrityError
Primary keys ['id'] are not unique in SOURCE dataset. Found 2 duplicate rows. Clean your data before diffing.
```

The keys together must name one row on each side. Two causes are common:

- The key is incomplete. An order line needs `order_id` and `line_no`, not `order_id` alone. List every key column in `primary_keys`.
- A rule on a key column made two keys one. `case_insensitive` reads `A1` and `a1` as the same key, and the check runs after the rules.

To see the repeated keys, read the side with Polars:

```python
import polars as pl

frame = pl.read_csv("legacy.csv")
print(frame.filter(frame["id"].is_duplicated()))
```

## Every row is added and removed

```text
Added:         2
Removed:       2
Changed:       0
```

No key pairs, since each side spells its keys another way, such as `A1` against `a1`, or `7` against `007` in text. Rules on a key column apply before rows are paired. Give the key the rule that makes both sides spell it one way, such as `case_insensitive: true`, `whitespace_mode: both`, or `pad_zeros: 3`. See [Primary keys](../configuration.md#primary-keys).

The match rate reads below zero in this case. It is one minus the mismatch ratio, and the ratio counts added and removed rows over the source rows, so it can pass 1.

## A key holds two types

```text
Configuration Error
Rows pair only on keys of one type, or of two integer or two float types, and these primary keys hold two other types: 'id' (String in the source, Int64 in the target). Give each a rule with cast_to, such as cast_to: Int64, so both sides hold one type.
```

Two exports often type one key differently. A CSV with padded keys, such as `" 1"`, reads them as text, where a Parquet file stored integers. Give the key a rule with `cast_to`, and trim it first if it is padded:

```yaml
rules:
  - column_names: [id]
    whitespace_mode: both
    cast_to: Int64
```

Before 0.33.2, a run failed on such a key with an unexpected error from inside the join.

## Most values in a column differ

One systematic difference is the usual cause: rounding, case, padding, a placeholder for NULL, or a code for each value. `veridelta suggest` and `veridelta crosswalk` find these, with the evidence for each rule they propose. [From drift to rules](from-drift-to-rules.ipynb) walks through both.

A column stored as two types compares as [Column types](../configuration.md#column-types) says: two numbers by value, and any other pair by casting the target to the source type.

## Timestamps differ by whole hours

When one side stores a time zone and the other does not, the side without one is read as UTC. A `09:00` without a zone matches `09:00` UTC, and differs from `09:00` in New York, which is `14:00` UTC.

If the side without a zone holds local time, export it with its zone. Where it is text, parse it with a `datetime_format` that reads the offset, such as `%Y-%m-%dT%H:%M:%S%z`. A `timezone` rule converts zoned timestamps only, and refuses a column without a zone; see [Dates and timezones](../rules.md#dates-and-timezones).

## A SQLite read fails, or its numbers drift

- `sqlite:///legacy.db` names `/legacy.db`, at the root of the file system. Write the full path after `sqlite://`, such as `sqlite:///srv/data/legacy.db`.
- `Cannot infer type from null for SQLite` names a column declared without a type, whose first rows are NULL. Declare its type, or select it with a `CAST` in a `query`.
- A column declared `NUMERIC` arrives as `Float64`. A value a float cannot hold exactly, such as one computed as `0.1 + 0.2`, then differs from the same decimal on the other side. A small `absolute_tolerance`, such as `0.000001`, forgives that and nothing a person would see.
- `validate --schemas` reads no rows, so it shows such a `NUMERIC` column as `String`, where a run reads `Float64`.

[Databases](../sources.md#databases) lists how each declared type arrives.

## A rule does nothing

```text
warning: Neither side has a column named 'vv', so the rules that name it do nothing. Check the spelling.
```

`veridelta validate --schemas` warns about a rule that names a column neither side has. Without `--schemas`, it reads no columns, so it cannot tell. With `normalize_column_names`, write the names in lower case.

## Pushdown refuses a column of two types

A comparison inside a database refuses a column whose two sides hold two types, unless both are numeric, since the database would convert one side by its own rules. Give the column a `cast_to`, or set `strict_types`; [Upgrading](../upgrading.md#0330) shows both.
