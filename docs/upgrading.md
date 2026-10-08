# Upgrading

Upgrading moves a project to a newer Veridelta release. Most releases ask nothing of you. This page lists each release that does, newest first, and what to change.

Veridelta is below 1.0, so a release that breaks something raises the minor version, such as 0.27.1 to 0.28.0. A patch release, such as 0.31.0 to 0.31.1, needs no change to your configuration or code, though a fix can change a verdict that was wrong. The [changelog](https://github.com/Veridelta/veridelta/blob/main/CHANGELOG.md) lists every change in every release.

## How to upgrade

Read the section below for each release between yours and the new one. Then move the version in a uv project:

```bash
uv lock --upgrade-package veridelta
uv sync
```

Check each configuration file against the new release, which reports any setting it now refuses:

```bash
veridelta validate -c veridelta.yaml
```

Then run a comparison whose verdict you already know, and confirm the new release agrees. In CI, move the GitHub Action's `@v` ref, and the tag in the GitLab template's `remote` URL, to the new release, as [CI integrations](ci.md) shows.

## 0.28.0

The `compile_` methods of `SQLPushdownCompiler` that read both sides no longer take `source_alias` or `target_alias`. They are `compile_query`, `compile_column_predicate`, `compile_changed_sample_query`, `compile_missing_query`, `compile_added_query`, `compile_column_mismatch_query`, and `compile_value_map_query`. A call that passes either raises `TypeError`. Drop the argument: the aliases were always `src` and `tgt`.

Only code that calls the compiler itself is affected. Configuration files and the command line are not.

## 0.15.0

`DataIngestor` no longer exists. `DiffEngine.run_from_configs` loads, aligns, and compares both sides in its place:

```python
from veridelta import DiffEngine, load_config

diff, source, target = load_config("veridelta.yaml")
result = DiffEngine.run_from_configs(diff, source, target)
```

`fetch_schema()` no longer exists on a connector. Read a reader's schema with `lazyframe().collect_schema()`. Probe a warehouse table with `compile_schema_probe_query(table)`, run through `execute_pushdown(sql, query_type="schema")`.

A connector of your own now subclasses one of two bases. One the local engine reads subclasses `ReaderConnector`. One that runs compiled SQL subclasses `PushdownSession`, and defines `compiler` and `execute_pushdown()`. A reader has no `execute_pushdown`.

## 0.14.0

`run`, `validate`, and `crosswalk` exit with `3`, not `1`, when they cannot finish, such as when a file is missing. With `--json`, such a failure prints `{"error": {"type": ..., "message": ...}}` on stdout, where it printed nothing.

A CI step that read every nonzero exit as drift still fails, which is right. A script that tells drift from a failure reads `1` as drift and `3` as a comparison that did not run. The [command line](cli.md#exit-codes) page lists every exit code.

## 0.12.0

Veridelta requires Python 3.11 or later.

## 0.11.0

Four comparisons became stricter. A run can report a difference that the release before it missed:

- `%f` in `datetime_format` reads `.5` as 500 milliseconds, not 5 nanoseconds.
- A local integer tolerance uses the true difference, so a pair whose difference overflowed into a match now mismatches.
- `strict_types: true` fails a warehouse column whose type differs after normalization.
- The crosswalk thresholds must be an `int` or a `float`, and not a boolean. `min_support` must be an `int`.

## 0.10.0

- A literal `${` inside `source` or `target` is written `$${`. A single `${` names an environment variable.
- Integer and float columns compare by value, so a pair that differs only in the fraction now mismatches in a local run.
- `load_nyc_taxi` raises `DatasetError`, which `except RuntimeError` no longer catches. Catch `DatasetError`, or `VerideltaError` for every error Veridelta raises.
- The tutorial notebooks were renamed, so their old URLs return 404.

## 0.7.0

`DiffEngine.run()` and `DiffEngine.run_from_configs()` return a `DiffResult`, where they returned a `DiffSummary`. The summary is on its `summary` attribute:

```python
summary = DiffEngine(config, source_frame, target_frame).run().summary
```

`SourceType` and `output_format` accept only the formats Veridelta reads and writes. A removed name, such as `sql` or `xml`, never worked, and now fails when the configuration loads.

## 0.6.0

`cast_to` accepts only `Int64`, `Float64`, `String`, `Boolean`, `Date`, and `Datetime`. Any other name fails when the configuration loads. Before, an unknown name left the column uncast, so a typo could hide a difference.
