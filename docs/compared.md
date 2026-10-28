# Compared with other tools

This page sets Veridelta beside four tools that also check data: datacompy, reladiff, dbt `audit_helper`, and Great Expectations. Each description of another tool comes from that project's own README or documentation, read on 2026-10-28, and links to it. If a description is wrong or out of date, open an issue to correct it.

## At a glance

| Topic | Veridelta | datacompy | reladiff | dbt `audit_helper` | Great Expectations |
| :--- | :--- | :--- | :--- | :--- | :--- |
| What it checks | Two datasets, row by row, paired on primary keys | Two DataFrames or tables, paired on join columns or the index | Two tables, in one database or across two | Two queries or relations in a dbt project, paired on primary key columns | One dataset against Expectations a team writes, or two query results for identical rows |
| Where the comparison runs | Polars on your machine, or SQL inside the warehouse or database that holds both tables | pandas, Polars, Spark, or Snowflake | Inside the databases | Inside the warehouse, as dbt macros | Against the data sources its compatibility reference lists |
| What decides a match | Rules per column in a configuration file | An absolute and a relative tolerance | The values of the columns compared | The values of the columns compared | Each Expectation's own condition |
| Command line | `veridelta run`: `0` on a match, `1` on drift | `datacompy compare`: `0` on a match, `1` on a difference | `reladiff` | Through `dbt` | Its README starts from Python |
| Output | A summary, an HTML report, Markdown for a pull request, JSON, and discrepancy files | A text report, typed report data, HTML, and a dictionary | JSON, or a diff with `+` and `-` lines | A result table with each row's status, counts, and samples | Validation results, and documentation built from them |

## What Veridelta adds

None of the four tools' documentation, read on 2026-10-28, describes these:

- **A run record.** `veridelta run` records the SHA-256 of the configuration and the baseline, the commit, and the CI run, so an approval traces to the files it judged. See [Tracing a verdict](traceability.md).
- **What the rules forgave.** With [`count_forgiven_values`](configuration.md#counting-forgiven-values), a run counts, per column, the values its rules made match although the originals differ.
- **Rules beyond tolerance.** Value maps, null sentinels, date formats, and fuzzy text matching apply per column; see [Rules](rules.md).
- **A baseline of named drift.** A [baseline](cli.md#accepting-drift) accepts drift row by row and column by column, where a threshold accepts a count.
- **A report built for sign-off.** The HTML report states the rules each column ran under, groups mismatched values into patterns, filters them by column, key, or pattern, and is checked with axe-core; see [Results](results.md#html-report).
- **A maintained GitHub Action.** It comments the summary on the pull request, and can attest its reports; see [CI integrations](ci.md).
- **An MCP server.** `veridelta mcp` lets an AI agent run comparisons; see [AI agents](agents.md). datacompy and GX Core ship agent skills instead.

Each tool also does something Veridelta does not; the sections below say what.

## datacompy

[datacompy](https://github.com/capitalone/datacompy) compares two DataFrames or tables in pandas, Polars, Spark, or Snowflake. Its README says it began as a replacement for SAS's `PROC COMPARE`. It pairs rows on join columns or on the index, and takes an absolute tolerance and a relative one, `abs_tol` and `rel_tol` ([Pandas usage](https://capitalone.github.io/datacompy/pandas_usage.html)). Its `datacompy compare` command sets a tolerance per column, such as `--abs-tol balance=0.01`, and exits `2` on an error.

Where the two differ:

- **Repeated keys.** datacompy pairs rows that repeat in the join columns by numbering them within each group, after sorting by the other columns ([Duplicate rows](https://capitalone.github.io/datacompy/pandas_usage.html#duplicate-rows)). Veridelta raises `DataIntegrityError` on a key that repeats, since which row pairs with which would be a guess. See [Primary keys](configuration.md#primary-keys).
- **What may differ.** datacompy takes an absolute and a relative tolerance, its command line can ignore spaces and case with `--ignore-spaces --ignore-case`, and custom comparators in Python handle more ([Command line](https://capitalone.github.io/datacompy/cli.html)). Veridelta reads every allowance per column from [rules](rules.md) in a configuration file, value maps, null sentinels, date formats, and fuzzy text matching among them. `veridelta validate` checks the file before a run.
- **Known drift.** datacompy's `--max-unequal-rows` lets a count of unequal rows pass. A Veridelta [baseline](cli.md#accepting-drift) names the drift it accepts, row by row and column by column, so any other drift still fails.

Choose datacompy to compare two DataFrames inside Python code or a notebook.

## reladiff

[reladiff](https://github.com/erezsh/reladiff) diffs tables inside the databases that hold them. Across two databases, it compares hashes of segments of each table, and downloads only the data it needs to compare the segments that differ. Inside one database, it joins the two tables. Its README lists over a dozen databases, a command line and a Python API, and output as JSON or as a diff with `+` and `-` lines. It is a fork of the archived data-diff, under the MIT license.

Where the two differ:

- **Repeated keys.** reladiff's join diff checks that the key columns are unique, unless `--assume-unique-key` skips the check ([How to use](https://reladiff.readthedocs.io/en/latest/how-to-use.html)). Veridelta always refuses a key that repeats.
- **Two databases.** Veridelta compares two tables in place only when both are on one connection; see [Pushdown](pushdown.md). Two tables in two systems are read and compared on the machine that runs Veridelta. reladiff compares across two databases without moving every row.
- **The verdict.** reladiff reports which rows differ. Veridelta also applies rules per column before it decides. It reports the count of mismatches in each column, writes an HTML report, and exits with a code for CI.

Choose reladiff to diff two large tables that live in two different databases.

## dbt `audit_helper`

[`audit_helper`](https://github.com/dbt-labs/dbt-audit-helper) is a dbt package of macros for data audits. `compare_and_classify_query_results` pairs two queries on `primary_key_columns`, and labels each row added, removed, identical, or modified, with counts and sample rows. Other macros compare row counts, the values of one column or of all columns, and the schemas of two relations. On Snowflake and BigQuery, `quick_are_queries_identical` compares a hash of every row. A macro's output can back a dbt test.

Where the two differ:

- **Where it lives.** `audit_helper` runs inside a dbt project, on the warehouse that project targets. Veridelta runs from its own command line or Python, and reads files and lakehouse tables as well as warehouses; see [Sources](sources.md).
- **What may differ.** Veridelta's rules forgive drift a person declares, such as a tolerance or a value map, and [baselines](cli.md#accepting-drift) accept a change made on purpose.

Choose `audit_helper` when the comparison belongs among a dbt project's models and tests.

## Great Expectations

[Great Expectations](https://github.com/fivetran/great_expectations), whose open source package is GX Core, checks data against Expectations, which its README calls "unit tests for your data". It builds documentation from each set of validation results.

Where the two differ:

- **One dataset or two.** Most Expectations state what one dataset must hold. A [Multi-source Expectation](https://docs.greatexpectations.io/docs/core/customize_expectations/define_a_multi_source_expectation), `ExpectQueryResultsToMatchComparison`, runs a query on each of two data sources and fails when the share of identical rows falls below `mostly`. Veridelta pairs the rows on primary keys, applies rules per column, and counts and lists each difference, and it does not profile either dataset.
- **Together.** Expectations can guard what a table holds, and Veridelta can show that a rebuilt table matches the one it replaces.

Choose Great Expectations to state and test what one dataset must hold.
