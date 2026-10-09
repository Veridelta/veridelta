## v0.35.1 (2026-10-09)

Each run of the GitHub Action writes its reports to a folder of its own, so a job that
runs the action twice keeps both runs' reports. Before, the second run overwrote the
first's `summary.json`, `summary.md`, `report.html`, and `otel-metrics.json`, while the
first step's outputs still named those paths. Each artifact now holds only its own run's
files, and a run that ends before it writes a summary no longer copies the previous run's
summary into the job summary. The CI guide also shows the comment the action posts on a
pull request, and the rules page says what rules cannot do.

### Fix

- give each GitHub Action run its own reports folder (#363)

## v0.35.0 (2026-10-09)

The GitHub Action takes a `baseline` input: a baseline file, relative to
`working-directory`, whose drift the run accepts, so a pull request that changes data on
purpose fails only on drift the file does not list. Write the file once with
`veridelta run --save-baseline accepted.json`, commit it next to the configuration, and
pass `baseline: accepted.json`. The path reaches `veridelta run --baseline` as one
argument, whatever it holds.
[Accepting drift](https://veridelta.github.io/veridelta/ci/#accepting-drift) in the CI
guide shows the step. The GitLab template does not take the input, since it stays as it
is unless a GitLab user asks.

### Feat

- a baseline input on the GitHub Action (#360)

## v0.34.0 (2026-10-09)

`Baseline` and `AcceptedChange` now import from `veridelta`, as the other models a user
builds do, so a baseline built in Python no longer needs `from veridelta.models import
Baseline`. `veridelta.models` still exports both. [Accepting
drift](https://veridelta.github.io/veridelta/cli/#accepting-drift) gains a Python example:
build the file from a result with `Baseline.of`, write it, read it back, and pass it to
`run_from_configs` as `baseline`.

### Feat

- import Baseline and AcceptedChange from veridelta (#358)

## v0.33.6 (2026-10-09)

No behavior changed. `engine.py` held 3,421 lines, most of them helpers around one class.
It now holds `DiffEngine` and the names it exports, in 1,102 lines. The helpers live in
nine private modules beside it, each with one job: rule resolution, `suggest`, local
matching, reading, warehouse routing, results, pushdown, config checks, and value maps.
[A decision record](https://github.com/Veridelta/veridelta/blob/main/decisions/engine-helpers-in-private-modules.md)
says why. Every name `veridelta.engine` exports is the same, and the test suites compile
the same SQL and make the same engine calls, in the same order, as 0.33.5 did. Three
attributes left `veridelta.engine`, since the code that reads them moved: the `fastexcel`
and `rapidfuzz_distance` probes, which the API page showed, and `logger`. The logger keeps
its name, so `logging.getLogger("veridelta.engine")` returns the same logger, and
`--verbose` prints the same lines.

### Refactor

- move crosswalk's proposals out of engine.py into _value_maps.py (#355)
- move the config checks out of engine.py into _checks.py (#354)
- move pushdown planning and collecting into _pushdown.py (#353)
- check a pushdown's probes without going through DiffEngine (#352)
- move pushdown rule resolution out of engine.py into _pushdown.py (#351)
- move artifacts and baselines out of engine.py into _results.py (#350)
- move warehouse routing out of engine.py into _warehouses.py (#349)
- move the loaders out of engine.py into _reading.py (#348)
- move local matching out of engine.py into _matching.py (#347)
- move suggest's proposals out of engine.py into _suggest.py (#346)
- move rule resolution out of engine.py into _resolution.py (#345)

## v0.33.5 (2026-10-09)

No behavior changed. PyPI shows the README of the newest release, so this release brings
two README changes to the project page there. A Status section says where the project
stands: it is in alpha, its tests run on generated data, nobody but its maintainer is known
to have run it, and its pushdown SQL has not yet run in a live cloud warehouse. The version
badge now comes from shields.io, which reads PyPI directly. The old one, from badge.fury.io,
still showed 0.14.0. GitHub Releases also take their notes from this changelog now.

## v0.33.4 (2026-10-08)

A pushdown run, its check under `validate --schemas`, and its row sample each compiled the
same statements, with the same long argument lists. They now share one compile step, so
the check compiles exactly the SQL a run executes. The statements, and the order they run
in, are the same. One ordering changes: a statement that fails to compile now raises its
`ConfigError` before the duplicate key queries run, as a local run fails on a rule before it
reads a key. Before, repeated keys were reported first.

### Refactor

- compile the pushdown statements once (#337)

## v0.33.3 (2026-10-08)

No behavior changed. `build_parser` added every command's arguments in one function of 270
lines. It now names each command, its parents, and its help in one place, and one function
per command adds that command's arguments. Every command's `--help` prints the same text as
before, byte for byte. The docs also gained a
[troubleshooting guide](https://veridelta.github.io/veridelta/how-to/troubleshooting/), which
starts from what a failed or surprising run shows and gives the cause and the fix.

### Refactor

- one function per command in build_parser (#335)

## v0.33.2 (2026-10-08)

A primary key whose two sides held two types a join cannot pair failed inside the join,
with an error that named no side and called itself a bug. A CSV and a Parquet export that
type one key differently were enough: Int64 against text, Int64 against Float64, two
decimal precisions, or a date against a timestamp. Each key's two types are now checked
before rows are paired, in `run`, `validate --schemas`, `suggest`, and `crosswalk`. Such a
key raises `ConfigError`, naming it, both of its types, and the `cast_to` rule that
resolves it. A key of one type, two integer types, or two float types pairs as before.
Pushdown is unchanged, since a database converts one side of the key itself, and
[Pushdown](https://veridelta.github.io/veridelta/pushdown/#differences-from-a-local-run)
lists that difference. The docs also gained a How-to guides section, a changelog page,
and tests that hold them to the command line, the GitHub Action, and the MCP server.

### Fix

- name a primary key whose two sides cannot pair rows (#332)

## v0.33.1 (2026-10-08)

No behavior changed. The Snowflake and Databricks connectors share one cursor session for
what they did the same way: the check before each statement, a fresh cursor's Arrow
fetch, and `close()`. Each keeps only how it signs in and which fetch its driver offers,
and the statements, log lines, and errors stay the same. This is also the first release
the new pipeline publishes (#324): it builds in one job, and publishes from another that
runs no code from the release, with an attestation for each file.

### Refactor

- one cursor session for Snowflake and Databricks (#325)

## v0.33.0 (2026-10-08)

Pushdown and a local run could reach different verdicts on a column whose two sides hold
different types. A local run casts the target to the source type, but a database
converted one side by its own rules. In DuckDB, the text `"007"` against the integer `7`,
or ISO text against a timestamp, matched where a local run reported drift. A date against
a timestamp at noon went the other way, and legacy text against a timestamp failed with a
raw conversion error. Pushdown now refuses such a column before reading a row, and names
it with both of its types. Two numeric types still compare by value, and a `cast_to` that
gives both sides one type resolves the rest. The [Upgrading](https://veridelta.github.io/veridelta/upgrading/)
page shows how. A new seeded-drift suite found it, and now holds both engines to drift
whose effect is known.

### BREAKING CHANGE

- A pushdown comparison of a column whose two sides hold different types, other than two
  numeric types, raises `ConfigError`. Add `cast_to` to the column's rule, or set
  `strict_types: true`.

### Fix

- refuse a pushdown column of two types a database would convert (#317)

## v0.32.0 (2026-10-08)

`veridelta.config` no longer re-exports the seven source models. Import `BigQueryConfig`,
`DatabaseConfig`, `DatabricksConfig`, `DeltaLakeConfig`, `DuckDBConfig`, `IcebergConfig`,
and `SnowflakeConfig` from `veridelta` or `veridelta.models`, which export them too. A new
[Upgrading](https://veridelta.github.io/veridelta/upgrading/) page says what each breaking
release since 0.6.0 asks of you. This is the release's one breaking change, so under 0.x it
bumps the minor version.

### BREAKING CHANGE

- `from veridelta.config import SnowflakeConfig`, and the same import of the six other
  source models, raises `ImportError`. Import them from `veridelta` or `veridelta.models`.

### Refactor

- stop re-exporting the source models from veridelta.config (#315)

## v0.31.2 (2026-10-08)

No behavior changed. `ReaderConnector` now holds what `connect()` opened, and gives every
reader the same `lazyframe()` and `close()`. The database, DuckDB, Delta Lake, and Iceberg
readers each wrote that pair themselves, and now keep only `connect()`. Each still names
its own kind of source when read before `connect()`, in the words it used before. A reader
of your own may still define both methods.

### Refactor

- give ReaderConnector a concrete lazyframe and close (#312)

## v0.31.1 (2026-10-08)

Every message for an optional extra that is not installed now reads the same way: what
needs the extra, which extra it is, and the command that installs it. Twelve places wrote
their own, in five wordings, and now share `missing_extra` in `veridelta.exceptions`. Each
still raises the error it raised before, so exit codes do not change.

### Refactor

- write the message for a missing extra in one place (#310)

## v0.31.0 (2026-10-08)

`veridelta run legacy.csv modern.csv --key id` compares two files with no configuration
file. It runs what a file holding only `primary_keys` and the two paths would run, with the
same summary and exit codes, and each file's format still follows its suffix. Repeat
`--key` for a key of several columns. Two files without `--key`, `--key` without two
files, or two files with `-c` exit 2. In Python, `files_config` in `veridelta.config`
builds the same configuration. Each tutorial also opens in Google Colab now, where its
first cell installs this release.

### Feat

- compare two files with run and --key, no configuration file (#307)

## v0.30.0 (2026-10-08)

`veridelta mcp` serves a sixth tool, `suggest_rules`. It returns what
`veridelta suggest --json` prints: rules that would explain each column's differences,
each with its evidence. Its `max_share` argument works as the command's `--max-share`
does. Each suggestion's examples hold primary keys from the data, so the tool answers only
on a server started with `--allow-row-values`, and `--max-rows` caps those keys. It reads
both sides locally, so a pair compared in place is refused, as the command refuses it.

### Feat

- **mcp**: suggest rules through a suggest_rules tool (#305)

## v0.29.0 (2026-10-08)

`veridelta validate --schemas` now warns about a rule whose `column_names` holds a name
that neither side has, after `normalize_column_names` when it is on. Such a rule forgives
nothing, so a misspelled name used to pass unnoticed. The warning names each such column.
It works for files, databases, lakehouse tables, and warehouse pairs alike, and the MCP
server's `validate_config` reports it too. A warning leaves the exit code at 0, as before.

### Feat

- warn when a rule names a column neither side has (#303)

## v0.28.0 (2026-10-08)

`SQLPushdownCompiler`'s seven `compile_` methods that read both sides no longer take
`source_alias` and `target_alias`. No caller passed them: the engine always used the
defaults. Every statement still names the two relations `src` and `tgt`, byte for byte, so
a comparison inside a warehouse runs the same SQL as before. This is the one breaking
change of the release, so under 0.x it bumps the minor version.

### Refactor

- **sql**: remove the unused source_alias and target_alias parameters (#295)

### BREAKING CHANGE

- `compile_column_predicate`, `compile_query`, `compile_changed_sample_query`,
  `compile_missing_query`, `compile_added_query`, `compile_column_mismatch_query`, and
  `compile_value_map_query` no longer accept `source_alias` or `target_alias`. A call that
  passes either raises `TypeError`. Drop the argument, since the aliases were always `src`
  and `tgt`.

## v0.27.1 (2026-10-08)

Every extra that installs pyarrow now asks for 23.0.1 or later, where it allowed 14.0.1.
pyarrow before 23.0.1 frees memory still in use while reading an Arrow IPC file
(CVE-2026-25087). A fresh install already got 23.0.1, so this changes an install only
where another tool pinned an older pyarrow. The Snowflake extra keeps its cap below 24 on
Python 3.14.

### Fix

- **deps**: raise the pyarrow floor past CVE-2026-25087 (#293)

## v0.27.0 (2026-10-07)

`veridelta run --save-baseline accepted.json` writes the file `run --baseline` reads:
every row of drift the run finds, in key order, with each changed row's differing
columns. A change made on purpose is accepted in one step. With `--baseline` too, the new
file keeps what the old one accepted, and leaves out entries for rows that no longer
drift. In Python, `Baseline.of(result)` builds the same file. The run's verdict and exit
code stay as they are.

### Feat

- write the drift a run finds as a baseline with run --save-baseline (#283)

## v0.26.0 (2026-10-07)

`veridelta run --baseline accepted.json` accepts the drift the file lists and fails only
on drift it does not list. The file lists added and removed rows by primary key, and
changed rows with the columns whose drift is accepted on each: drift in any other column
of that row still counts. Accepted drift is left out of the counts, the verdict, the
artifacts, and the reports. The summary gains `accepted_count`, in `run --json`, in the
MCP server's `run_comparison`, and in the published run schema, and
`veridelta schema baseline` prints the file's JSON Schema. A pair compared where it is
stored refuses `--baseline`.

### Feat

- accept the drift a baseline file lists with run --baseline (#280)

## v0.25.0 (2026-10-07)

`veridelta suggest` now proposes `datetime_format` for a column with text on one side and
dates or timestamps on the other, when the text reads as those dates under one format.
The common formats are tried, with or without a time, and the one that matches the most
differing rows wins, so the dates themselves settle whether the day or the month comes
first. A timestamp with a zone is left out.

### Feat

- suggest a date format for text that reads as the dates beside it (#278)

## v0.24.0 (2026-10-07)

`veridelta suggest` now proposes `null_values` for a column of any type when a common
spelling of NULL stands on one side where the other side is NULL: an empty string, `N/A`,
`NULL`, `None`, `-`, and the like, in any case, or a number such as -999. A real value
beside NULL, such as a city, is a change, and `false` is never suggested. A suggested
sentinel joins the column's sentinels today, and a column with both rounding and a
sentinel gets one rule with both settings.

### Feat

- suggest null sentinels where one side spells NULL (#276)

## v0.23.0 (2026-10-07)

`veridelta suggest` now proposes rules for text columns too. A column gets
`whitespace_mode: both` when its differing values match once both ends are stripped,
`case_insensitive: true` when they match once lowercased, and both settings only when
some rows need both. Text that differs in anything else is a change, and no rule explains
it. A rule that would make a row that matches today differ is never suggested, such as
case folding ahead of a `value_map` whose keys are capitals.

### Feat

- suggest trimming and case folding for text that differs only in them (#274)

## v0.22.0 (2026-10-07)

`veridelta suggest` runs the comparison, then suggests rules that would explain the
differences it finds, each with its evidence. Its first kind of rule is a numeric
tolerance. A column gets one when every gap between its differing values is at most
`--max-share`, 1% by default, of the larger of the two: absolute when the gaps stay about
one size, as rounding leaves them, and relative when they grow with the values. The
tolerance is the first round value above the largest gap, and the comparison runs again
with it to count the rows it explains. The rules print as YAML on stdout, ready to paste
first in `rules`, and the evidence on stderr, or both as JSON under `--json`, with a
published schema. No model is called. In Python, `DiffEngine.suggest_rules()` returns
`RuleSuggestion` models.

The results page shows the HTML report in light and dark, and a link to the docs shows a
preview card. The AI agents page shows a client calling the MCP tools, and `demo/agent/`
holds a kit for recording an agent's session. The sdist leaves out the recordings and
screenshots, which made up about 40% of its size.

### Feat

- suggest a numeric tolerance from the pairs that differ (#272)

## v0.21.2 (2026-10-07)

The run report counts one in the singular: a column with one mismatch reads "1 mismatch",
and a target with one more row reads "+1 row", where both took the plural.

### Fix

- count one mismatch and one row in the singular (#265)

## v0.21.1 (2026-10-07)

While a run compares, `veridelta run` says "Comparing..." on stderr, where it said
"Executing semantic diff...".

The AI agents page and the agent skill give an agent the loop that fixes drift its own
pipeline change causes. Tutorial 6 compares two model evaluation runs. The command line
page shows recordings of `validate` and `crosswalk`, each beside the text it prints.

### Fix

- say "Comparing..." while a run compares (#263)

## v0.21.0 (2026-10-07)

The Markdown summary that CI posts to a pull request ends with an HTML comment, which
GitHub and GitLab hide from readers. It names the published run schema and holds the
run's summary as JSON on one line, so an agent that reads the comment through the API can
parse the result without running the comparison again. The JSON is what
`veridelta run --json` prints, except that `column_mismatches` lists only the columns the
drift table shows, and is left out when `report_top_columns_limit` is 0. `<`, `>`, and `&`
are written as JSON escapes, so a column name cannot close the comment. The 60,000 byte
budget for changed values counts the comment.

### Feat

- end the Markdown summary with its counts as JSON for agents (#256)

## v0.20.0 (2026-10-07)

`veridelta schema` takes an optional name, `run`, `validate`, `crosswalk`, or `error`, and
prints the JSON Schema of what that command prints with `--json`, or of the error object
any command prints at exit code 3. With no name, it prints the configuration schema, as
before. Each schema is published under `docs/schema/` at the URL its `$id` names, and a
test validates real output from each command against it. `typing-extensions` is now a
declared dependency; Pydantic already required it.

The README and the docs home name the MCP server, and the user guide, the product
documents, and the contributor guide now say what the code does.

### Feat

- publish a JSON Schema for what each command prints with --json (#254)

## v0.19.20 (2026-10-07)

No behavior changed. The engine opens and closes a warehouse session in one place, and the
package logger holds the one `NullHandler` that seven modules each added. A program that
adds no handler still sees no records from Veridelta.

### Refactor

- open a warehouse session in one place, and silence logging once (#248)

## v0.19.19 (2026-10-07)

No behavior changed, and every SQL statement is byte for byte the same: the unit and
integration suites compile 2,316 statements, and each is the same before and after. The
allowlist check every configured name passes before SQL is one function, the value map
query names its aliases once, and `veridelta.models.POSTGRES_SCHEMES` names the Postgres
URI schemes once.

### Refactor

- name each SQL alias, the allowlist gate, and the Postgres schemes once (#246)

## v0.19.18 (2026-10-07)

No behavior changed. The verdict and the reported `mismatch_ratio` divide by the source
rows through one function, `mismatch_ratio_of`, and five places that normalized a column
name share `normalize_column_name`. Both are in `veridelta.models`.

### Refactor

- compute the mismatch ratio and normalize a column name in one place each (#244)

## v0.19.17 (2026-10-07)

The engine writes each check once: the `timezone` guard, the null equality of stage 9, the
probe for each optional extra, and the names of the two sides. Results are unchanged. One
message changed: a `timezone` rule on a naive timestamp now gives the same advice on a
local run and in pushdown, to store the column with a zone or parse it with an offset such
as `%z`.

### Refactor

- write each engine check once (#242)

## v0.19.16 (2026-10-07)

No behavior changed. `validate --json` and the MCP server's `validate_config` build their
report through one function, `validation_report` in `veridelta.mcp_server`, and print the
same JSON as before. Two argument parsers share their whole-number parse, with every
message unchanged.

### Refactor

- build the validation report once, and parse whole numbers once (#240)

## v0.19.15 (2026-10-07)

A `schema_mode` failure lists the columns that break the mode, sorted and quoted, where it
printed a Python set in an order that changed between runs. `exact` says which columns
only the source has and which only the target has, and a run from a configuration says
what each side read. The messages keep their opening words.

### Fix

- list the columns a schema_mode refuses in a fixed order (#227)

## v0.19.14 (2026-10-07)

The HTML report and the Markdown summary say a keys-only comparison ran as pushdown,
inside the database that stores both tables. They said warehouse pushdown, which was wrong
for two Postgres or DuckDB tables that set `pushdown`.

### Fix

- say pushdown, not warehouse pushdown, in the reports (#225)

## v0.19.13 (2026-10-07)

`OTEL_EXPORTER_OTLP_TIMEOUT` must be ASCII digits, so a value such as `²` names the
variable rather than crashing the send. A send with `OTEL_EXPORTER_OTLP_HEADERS` set warns
when it goes over plain `http://` to another machine, naming the endpoint and never the
headers, since they often carry an API key.

### Fix

- refuse a non-ASCII OTLP timeout, and warn on headers over http (#223)

## v0.19.12 (2026-10-07)

`load_nyc_taxi` caches a download only when it is under 1 MiB and its SHA-256 matches the
published sample's. A cached copy that no longer matches, a corrupt one included, is
downloaded again with a warning.

### Fix

- check the sample dataset's SHA-256 and size before caching it (#221)

## v0.19.11 (2026-10-07)

The GitHub Action checks its `extras`, `version`, and `artifact-name` inputs before it
installs anything. A value outside the pattern each input takes fails the step with exit
code 2, the command line's code for invalid arguments. Before, a workflow that passed text
from a pull request into an input could add a requirement, or a second line to the step's
outputs.

### Fix

- check the Action's extras, version, and artifact name before use (#219)

## v0.19.10 (2026-10-07)

The `snowflake` extra needs `snowflake-connector-python` 4.7.3 or later, past six
advisories in earlier releases, such as logged credentials and a skipped TLS hostname
check. The `snowflake` and `databricks` extras need `pyarrow` 14.0.1 or later, past a flaw
that runs code from a crafted IPC file. An install that already has an older driver now
upgrades it.

### Fix

- **deps**: raise the floors of two drivers past their advisories (#217)

## v0.19.9 (2026-10-07)

Errors and logs name a file or table without the query of its URL, which can hold a token
or the signature of a pre-signed link, and without a login in its user part. Before, only
telemetry did: a failed read of a pre-signed `s3://` link printed its signature, and a
database URI kept a `?password=` parameter in every log line. `redacted_location` in
`veridelta.models` is the one redactor for all of them.

### Fix

- leave the query and login out of every location printed (#215)

## v0.19.8 (2026-10-07)

A Databricks connection that fails masks the access token in its error as `***`, and no
longer chains the driver's exception, whose traceback printed the message again. Snowflake
already did both. BigQuery's connection error is no longer chained either, so the three
warehouses report a failed connection alike.

### Fix

- keep the Databricks token out of a connection error (#213)

## v0.19.7 (2026-10-07)

A failure Polars reports as a panic now exits 3, as any other error does, and `--json`
prints it as one error object. Before, Python exited 1 with a traceback: the code
`veridelta run` gives for drift, so CI read a crash as drift. The MCP server fails the one
call with the panic's type and message, where it would have stopped.

### Fix

- report a Polars panic as an error, exit 3, not as drift (#211)

## v0.19.6 (2026-10-07)

A run whose differing rows hold a binary column, such as a MySQL `BIT` column, writes its
discrepancy files in every format. `csv`, `json`, and `ndjson` hold the bytes as
hexadecimal text, while `parquet` and `arrow` keep them. Before, Polars refused the column
in CSV and panicked on it in JSON, and the run stopped with exit code 1. A column that
nests binary values in a list or a struct fails with a `ConfigError` that names the
formats which hold it.

### Fix

- write a binary column to every artifact format (#209)

## v0.19.5 (2026-10-07)

`veridelta mcp` describes its tools as they behave. `propose_value_maps` now says that
`sample_fraction` is a share of source rows chosen by primary key, as `--sample-fraction`
does, not a share of joined rows. The instructions a host reads say that two tools return
row values only when the server allows them, and that the server reads files only from the
folders it was started with.

### Fix

- describe the MCP tools as they behave (#207)

## v0.19.4 (2026-10-07)

`veridelta mcp` masks every value a configuration takes from an environment variable, four
characters or longer, wherever it appears in a tool's answer. Before, `path: ${SECRET}.csv`
returned the value inside a read's error, and a column named after a variable returned it
in a schema. `veridelta.config.referenced_variables` names the variables a configuration's
text references.

### Fix

- mask values taken from the environment in MCP answers (#205)

## v0.19.3 (2026-10-07)

`veridelta mcp` runs a side's `query` only when it is started with the new
`--allow-queries`, since a query runs as written, with the configuration's credentials.
Before, a DuckDB query such as `SELECT * FROM read_text(...)` could read a file outside
the roots, and a MotherDuck query ran read-write with the server's token. Every DuckDB file
the server opens is now held to the roots: extensions stop loading on their own, files open
only under the roots, and the configuration locks before the first read. A MotherDuck
connection is not held to the roots. A DuckDB connection whose setup fails is now closed.

### Fix

- gate MCP queries behind --allow-queries and hold DuckDB to the roots (#203)

## v0.19.2 (2026-10-07)

The MCP server checks a configuration's relative data path, and its `output_path`, where
the reader and the artifact writer open it: the working directory. Before, it checked them
against the first `--root` folder, which agrees only because `veridelta mcp` runs there. A
program that called `serve()` or `build_server()` from another folder passed the check for
a file under the root, then read or wrote the one beside the program. The command line is
unchanged.

### Fix

- check a configuration's relative paths where they are opened (#201)

## v0.19.1 (2026-10-07)

Every `veridelta mcp` tool that opens a side now reads data on this machine only from under
the server's `--root` folders. Before, only `read_discrepancies` and `propose_value_maps`
checked, while `run_comparison`, `describe_schema`, and `validate_config` with `schemas`
opened a file anywhere. A column name can carry a file's text: reader options that skip
lines and split on an unused separator turn any line of any file into the header, which
`describe_schema` returned. A check without `schemas` opens no data and is unchanged.

The PyPI summary, the docs, and `veridelta --help` now describe Veridelta in one sentence,
and the pushdown guide says which services the generated SQL has run in.

### Fix

- hold every MCP tool that opens a side to the server's folders (#199)

## v0.19.0 (2026-10-07)

`veridelta mcp` gains its last two tools, which return values from the data.
`read_discrepancies` runs the comparison and returns the rows of one kind, added,
removed, or changed, with how many there are. `propose_value_maps` returns what
`veridelta crosswalk --json` prints. Both refuse unless the server is started with
`--allow-row-values`, and then return at most `--max-rows` rows or value map entries a
call, 50 by default. They read files on this machine only from under the server's
`--root` folders, with `~` expanded and links followed, and read data elsewhere, such as
an object store or a warehouse, as the command line does.

### Feat

- read rows and propose value maps from the MCP server when allowed (#195)

## v0.18.0 (2026-10-07)

`veridelta mcp` gains a third tool, `describe_schema`, which lists one side's columns and
their types and reads no rows. It returns the side and a map from each column's name, as
stored, to its type as Polars names it, such as `Int64`. A side read through a `query` is
refused, since only running the query would name its columns.

In Python, `DiffEngine.read_schema(config)` returns one side's `pl.Schema` without
returning a row. A file is read by its loader, a database or DuckDB `table` by a probe
that returns no rows, and a warehouse table, or a table with `pushdown`, by the probe a
pushdown run starts with, in a session of its own.

### Feat

- describe a side's schema from the MCP server (#193)

## v0.17.0 (2026-10-07)

`veridelta mcp` gains a second tool, `run_comparison`, which runs the comparison a
configuration file describes. It returns the summary `veridelta run --json` prints, with
three more fields: `verdict`, which is `match` or `drift`; `exit_code`, the 0 or 1 that
`veridelta run` exits with; and `artifacts_written`, which says whether the rows that
differ were written. A run writes those rows to the configuration's `output_path`, so the
tool refuses an `output_path` outside the server's `--root` folders before it reads a row.
A configuration that cannot run fails the call with its error's type and message, as
`run --json` reports a failure.

### Feat

- run a comparison from the MCP server (#191)

## v0.16.0 (2026-10-07)

`veridelta mcp` serves Veridelta's checks to an AI agent's host as Model Context
Protocol tools, over stdio. It needs the new `mcp` extra, which installs the official
MCP Python SDK and is part of `all`. Its first tool, `validate_config`, returns the
object `veridelta validate --json` prints. The person who starts the server names the
folders a tool may read with `--root`, the current directory by default, and a path
outside them fails the call. A tool returns findings, never a value from the
configuration file. The AI agents page shows how to register the server with Claude
Code or Cursor.

In Python, `DiffEngine.check_config_file(path)` loads a configuration file and returns
the findings `veridelta validate` reports, with a file that does not load as one error.

### Feat

- add `veridelta mcp`, a Model Context Protocol server with the tool `validate_config`

## v0.15.0 (2026-10-07)

A file source reads its `format` from the path's suffix when the key is absent: `.csv`;
`.parquet` or `.pq`; `.json`; `.ndjson` or `.jsonl`; `.arrow`, `.ipc`, or `.feather`;
`.avro`; and `.xlsx` or `.xls`, in any case and before a `?` query or `#` fragment. A
`format` you write always wins, and a suffix Veridelta does not know keeps the `csv`
default. The smallest configuration is now the two paths and the keys that pair their
rows, and the README, the docs home, and tutorial 2 lead with it, which is the file the
README's recording runs.

Two names deprecated in 0.14 are gone. `veridelta.DataIngestor`, which loaded and aligned
two sources for a `DiffEngine` that aligns them again, goes: call
`DiffEngine.run_from_configs(diff, source, target)`. `VerideltaConnector.fetch_schema()`,
which nothing in the package called, goes with the code that served only it: read a
reader's schema with `lazyframe().collect_schema()`, and probe a warehouse table with
`compile_schema_probe_query(table)` through `execute_pushdown(sql, query_type="schema")`.

The connector base is split in two. `VerideltaConnector` declares the lifecycle,
`connect()` and `close()`, and nothing else. `ReaderConnector` adds `lazyframe()`, which
the Delta Lake, Iceberg, database, and DuckDB connectors implement. `PushdownSession` adds
`compiler` and `execute_pushdown()`, which the Snowflake, Databricks, and BigQuery
connectors and the Postgres and DuckDB pushdown sessions implement. A reader no longer has
an `execute_pushdown` method. A subclass written against the old base now subclasses the
kind it is.

This release ships before the live warehouse suite's first run against Snowflake,
Databricks, BigQuery, and MotherDuck, by the maintainer's decision. That run comes when the
maintainer has the time, and a difference it finds ships as a 0.15 patch.

### Feat

- infer a file's format from its extension

### Refactor

- remove `DataIngestor`, deprecated in 0.14.6
- remove `fetch_schema`, deprecated in 0.14.7
- split the connector base into readers and pushdown sessions

### BREAKING CHANGE

- `veridelta.DataIngestor` no longer exists. Call
  `DiffEngine.run_from_configs(diff, source, target)`, which loads, aligns, and compares
  both sides.
- `VerideltaConnector.fetch_schema()` no longer exists. Read a reader's schema with
  `lazyframe().collect_schema()`, and probe a warehouse table with
  `compile_schema_probe_query(table)` through `execute_pushdown(sql, query_type="schema")`.
- `VerideltaConnector` declares `connect()` and `close()` only. A connector the local
  engine reads subclasses `ReaderConnector` and defines `lazyframe()`; a connector that
  runs compiled SQL subclasses `PushdownSession` and defines `compiler` and
  `execute_pushdown()`. A reader no longer has an `execute_pushdown` method.

## v0.14.10 (2026-10-06)

`veridelta.datasets` computes its Git ref in one private function, where two public names
nothing else used built it, and logs through the logger's own formatting rather than an
f-string. The notice that the NYC taxi sample is downloading moved from WARNING to INFO,
since a download a tutorial asks for is routine; a corrupted cache still logs at WARNING.
Nothing else changed.

### Refactor

- tidy `datasets.py`: one private ref constant, and the download notice logged at INFO
  without an f-string

## v0.14.9 (2026-10-06)

The connector base now refuses SQL pushdown by default, with one message, so a reader
that is compared locally no longer implements `execute_pushdown` only to raise. The Delta
Lake, Iceberg, database, and DuckDB readers lose those overrides and their three copies of
the message. A warehouse connector or a pushdown session overrides the default, as before.
The refusal's wording changed: it now says the source is compared locally and names
`connect()`, where it said the method was warehouse-only.

### Refactor

- refuse pushdown from the connector base, so no reader implements `execute_pushdown`
  only to raise

## v0.14.8 (2026-10-06)

The connectors now share one secret scrubber. Three of them each masked credentials in
driver output their own way, and two carried an identical property that names what a
side reads. `mask_secrets` and `read_subject` in `veridelta.connectors.base` replace
them, and every message, log line, and exception keeps its wording. Nothing a user sees
changed.

### Refactor

- share one secret scrubber and one read subject across the warehouse, database, and
  DuckDB connectors

## v0.14.7 (2026-10-06)

Every connector's `fetch_schema` now warns that it goes in 0.15.0. The method has nine
implementations and no caller in the package: the engine reads a side's columns from the
frame it loads. The method, its implementations, and the code that serves only it stay
until 0.15.0, where the removal ships as a breaking change. Nothing else changed.

### Refactor

- deprecate `fetch_schema` on every connector with a warning, ahead of its removal in
  0.15.0

## v0.14.6 (2026-10-06)

`DataIngestor` now warns on construction that it goes in 0.15.0. No code in the package
calls it, and its own docstring warned against the one thing it is for: feeding its
output to `DiffEngine` aligns both sides twice. `DiffEngine.run_from_configs(diff,
source, target)` loads and aligns the sides itself. The class and its export stay until
0.15.0, and nothing else changed.

### Refactor

- deprecate `DataIngestor` with a warning on construction, ahead of its removal in 0.15.0

## v0.14.5 (2026-10-06)

The package behaves as 0.14.4 did: no command, rule, connector, or report changed. One
helper, `optional_module` in `veridelta.connectors.base`, now probes every extra the
connectors and the engine read through. Each probe was a `try` block of its own, with two
coverage pragmas, eight in all, and the engine kept a private copy of the helper that the
connectors could not import. Every probe keeps its name, so a test that patches one is
unchanged.

### Refactor

- share one optional-import helper across the connectors and the engine, and drop the
  eight coverage pragmas the old probes carried

## v0.14.4 (2026-10-06)

`-v` now prints each file a local run opens, with its format and its width, on the
`veridelta.engine` logger. Only the connectors logged before, so a run over two files
printed nothing under `--verbose`, though the 0.14.0 changelog promised each read. A run
without the flag prints no record, as before.

### Fix

- log each file a local run opens under `--verbose`, with its format and column count

## v0.14.3 (2026-10-06)

A configuration failure inside `source` or `target` now names the block. A bad `format`
in the target was reported as `[file -> format]`. Pydantic opens the location with the
`type` tag of the model it chose, which names neither side. The location now opens
with `source` or `target`, so the line reads `[target -> format]`, and a block whose
`type` matches no model reads `[target]`. The comparison settings keep their locations,
such as `[rules -> 0 -> case_insensitive]`.

### Fix

- name `source` or `target` in the location of a configuration failure inside either
  block, in place of Pydantic's `type` tag

## v0.14.2 (2026-10-06)

A missing primary key now names what each side read. `format` defaults to `csv`, so a
`.parquet` path without it is scanned as text, and the run stopped with "Primary keys
missing in SOURCE", which named a symptom. The error now names the side, the file, and the
format it was read as, and says when `format` was not set. It lists the columns it found,
so a newcomer changes `format` rather than the key. A Delta Lake or Iceberg side is named
by its table URI, and a database or DuckDB side by its table or as a query, never by a
credential. `validate --schemas` reports the same message.

### Fix

- name the side, the file, the format it was read as, and the columns found when a
  primary key is missing, in `run`, `crosswalk`, and `validate --schemas`

## v0.14.1 (2026-10-06)

The package is the one 0.14.0 shipped: no command, rule, connector, or report changed.
This release proves the shorter release path. A release now writes the version into
every pin a user copies, from one list in `pyproject.toml`: the docs, the GitHub Action,
and the bug report form. A test holds those pins to the package's version between
releases, where 0.14.0 edited each by hand.

The repository now says whom Veridelta serves. `product/USERS.md` holds the personas
and use cases, and `product/KEY_METRICS.md` the north star with its drivers and
guardrails. `product/FEATURES.md` maps every feature and roadmap item to the use case it
serves, and each roadmap item names its use case or says that none asks for it yet. The
GitLab CI template is frozen as it stands, until a GitLab user asks for more. The
pushdown parity suite has still not run against the live warehouses. That run comes
before 0.15.0, and a difference it finds ships as a 0.14 patch.

### Chore

- bump every version pin from one list in `pyproject.toml`, and hold each to the
  package's version with a test
- write the personas, use cases, key metrics, and feature map under `product/`, with a
  test that ties them together
- freeze the GitLab CI template, and name a use case on every roadmap item

## v0.14.0 (2026-10-06)

`run`, `validate`, and `crosswalk` now exit with `3` when they cannot finish, such as on
a configuration error, an unreachable source, or a missing extra. `1` means drift, or a
file `validate` finds invalid, and nothing else. Under `--json`, a failure prints one
object, `{"error": {"type": ..., "message": ...}}`, on stdout, so a script reads one
stream whatever happens. The GitHub Action and the GitLab CI template still read `1` as
drift only when the summary says so, since either can pin an older release.

Snowflake signs in with a key pair, from `private_key_path` and an optional
`private_key_passphrase`, since Snowflake now requires strong sign-in for scripted
users. `-v` or `--verbose` prints each connection, read, and pushdown statement on
stderr, with their timings and never a credential. `run --otel-send` posts the run's
OpenTelemetry metrics to the OTLP/HTTP endpoint that the standard `OTEL_EXPORTER_OTLP_*`
variables name, so no separate `curl` step is needed. The GitHub Action and the GitLab
CI template gain an `otel-send` input. The GitLab template also gains the Action's
`working-directory`, `python-version`, and `upload-artifact` inputs.

Three failures that the command line called a bug now name their cause before the
comparison starts. One is a missing or unreadable file. Another is a Delta Lake or
Iceberg table, version, or snapshot that cannot be read. The third is a `cast_to` that
the column's type cannot take, which `validate --schemas` reports too. A SQL Server
`table` read now reads each `DATETIMEOFFSET` at the instant it holds, where ConnectorX
0.4.6 shifted it by its offset. A `query` still reads what ConnectorX returns, and the
docs show the workaround.

The HTML report reads without JavaScript: every row is in the page, and the script only
splits long tables into pages of 25 rows. Each pager names its table and announces its
page, and a wide table scrolls from the keyboard. Cells show Python's text for a value,
so a float reads `1.0` and a struct shows its fields. On the docs site, text reaches the
4.5:1 contrast ratio in both color schemes, links are underlined, and the search dialog
has a name. `ACCESSIBILITY.md` says what Veridelta aims for, how it is checked, and how
to report a barrier.

Reads are now tested against real systems: Delta Lake and Iceberg tables each test
writes, and MySQL 8.4 and SQL Server 2022 servers in CI. A suite started by hand runs
the pushdown parity cases inside Snowflake, Databricks, BigQuery, and MotherDuck. This
release ships before that suite's first run against those services. That run comes
before 0.15.0, and a difference it finds ships as a 0.14 patch.

For AI agents, the docs site publishes `llms.txt` and `llms-full.txt`, and a new page
says how an agent should drive the command line. `skills/veridelta/SKILL.md` carries the
same steps as an agent skill. Contributors' coding agents read one rules file,
`AGENTS.md`, and settled choices keep short records under `decisions/`. A new CI job
checks every docs page and a sample report with axe-core.

### Feat

- sign in to Snowflake with a key pair, from `private_key_path` and
  `private_key_passphrase`
- print each connection, read, and pushdown statement on stderr with `--verbose`
- send a run's OpenTelemetry metrics to an OTLP/HTTP endpoint with `run --otel-send`,
  and the `otel-send` input of the GitHub Action and the GitLab CI template
- give the GitLab CI template the Action's `working-directory`, `python-version`, and
  `upload-artifact` inputs
- exit with `3` when a command cannot finish, and print the failure as one JSON object
  under `--json`

### Fix

- report a missing or unreadable file before the comparison starts
- report an unreadable Delta Lake or Iceberg table, version, or snapshot before the
  comparison starts
- refuse a `cast_to` the column's type cannot take before reading a row, and report it
  in `validate --schemas`
- read each SQL Server `DATETIMEOFFSET` in a `table` read at the instant it holds
- render every row of the HTML report in the page, name each pager's buttons after its
  table, announce its page, and let the keyboard scroll a wide table
- show report cells as Python writes their values, such as `1.0` for a float
- reach the 4.5:1 contrast ratio on the docs site in both color schemes, underline its
  links, and name its search dialog
- correct where the docs and the code disagreed

### Test

- read real Delta Lake and Iceberg tables
- read real MySQL and SQL Server tables in CI
- run the pushdown parity suite inside live warehouses, started by hand
- check every docs page and a sample HTML report with axe-core and a keyboard in CI

### Chore

- publish `llms.txt`, `llms-full.txt`, and an AI agents page, and add AI workflows to
  the roadmap
- add an agent skill, `AGENTS.md` for contributors' coding agents, and decision records
- add an accessibility statement, an accessibility issue form, and a funding file for
  GitHub Sponsors
- run the end-to-end tests on macOS, give CI jobs a read-only token, and update
  vulnerable locked dependencies

### BREAKING CHANGE

- `run`, `validate`, and `crosswalk` exit with `3`, not `1`, when they cannot finish.
  Under `--json`, a failure prints `{"error": {"type": ..., "message": ...}}` on stdout,
  where it printed nothing.

## v0.13.0 (2026-10-05)

A `duckdb` source reads a table, or the result of a query, from a DuckDB file or a
MotherDuck database written as `md:name`. It is compared locally and pairs with any
source but a warehouse. It needs the `duckdb` extra. A file opens read-only, and every
session reads time in UTC. MotherDuck needs a token, from the `motherduck_token` field
or the `MOTHERDUCK_TOKEN` variable, and is tested only against a stand-in for its
driver.

Two tables in one DuckDB database that both set `pushdown` are compared inside DuckDB,
as two Postgres tables already can be. Only counts, keys, and an optional row sample
come back. DuckDB refuses `max_levenshtein_distance`, since its `levenshtein` counts
bytes rather than characters.

A database `table` can be read in parallel: set `partition_on` to an integer column and
`partitions` to the number of ranges. The column must hold no NULL, which ConnectorX
would leave out. Veridelta counts NULLs first and fails the read if it finds any.

`--markdown-max-rows` lists changed values in the Markdown summary, which the GitHub
Action and the GitLab CI template post on pull and merge requests. The default, `0`,
lists none, since everyone who can read the comment sees the values. OpenTelemetry
exports now read `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`, and Veridelta's own
attributes keep their values.

The rest is internal. Single-use helpers are inlined across the engine, the compiler,
and the CLI, and private docstrings are cut to one line. Public docstrings follow the
style rules, with doctested examples, and copied tests are parametrized. CI has one
check to require on `main`, a time limit on each job, and a workflow that re-runs jobs
GitHub never started.

### Feat

- read DuckDB files and MotherDuck databases as a `duckdb` source
- compare two DuckDB tables inside DuckDB when both set `pushdown`
- read a database `table` in parallel partitions with `partition_on` and `partitions`
- list changed values in the Markdown summary with `--markdown-max-rows`, and the
  `markdown-max-rows` input of the GitHub Action and the GitLab CI template
- read `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` for OpenTelemetry exports

### Refactor

- inline single-use helpers and drop repeated code in the engine
- inline single-use helpers in the SQL compiler and the connector sessions
- define `--config` once in the CLI and inline its single-use helpers

### Chore

- add one `CI Passed` check for `main` to require, stop each CI job after 15 minutes,
  and re-run CI jobs that GitHub never started
- drop stale tool configuration
- mark each test module once, parametrize copied tests, and drop exact duplicates
- cut private docstrings to one line, and write the public ones to the style rules with
  doctested examples
- start the release steps with a `cz bump` dry run

## v0.12.1 (2026-10-05)

A Postgres `table` now keeps the declared precision and scale of each `numeric` column,
with or without `pushdown`. Before, every `numeric` arrived as `Decimal(38, 10)`,
rounded to ten decimal places. A value with more than 18 digits before the decimal point
failed the read, and `strict_types` could not tell `numeric(10, 2)` from
`numeric(12, 4)`. Veridelta now reads the declarations from the `pg_attribute` catalog
before the rows, so a Postgres-compatible server without that catalog needs a `query`. A
`query`, a `numeric` declared without a precision, and one with a precision above 38 or
a negative scale keep `Decimal(38, 10)`. A stored `NaN` fails the read, which now names
its column.

Pushdown also accepts `timezone` on text that `datetime_format` parses with an offset,
as a local run does, and `cast_to: Boolean` works on BigQuery numbers that are not
integers. OpenTelemetry exports keep the container in `abfss://` source names.

The documentation follows written style rules, which a CI check enforces. The
configuration guide is split into pages for configuration, sources, rules, pushdown,
results, and the command line. A link to a section that moved lands at the top of the
configuration page.

### Fix

- read each Postgres `numeric` at its declared precision and scale, with or without
  `pushdown`
- accept `timezone` in pushdown on text that `datetime_format` parses with an offset
- compare BigQuery numbers with zero for `cast_to: Boolean`, since BigQuery casts only
  integers and text to `BOOL`
- keep the container in the `abfss://` source names of OpenTelemetry exports

### Chore

- pin workflow actions to commits, and let Dependabot keep them and the lockfile current
- deploy the documentation one commit at a time
- add a code of conduct, issue forms, and a pull request template
- hold the documentation and tutorials to written style rules, checked in CI, and split
  the configuration guide into focused pages

## v0.12.0 (2026-10-05)

Veridelta now requires Python 3.11 or later, since Python 3.10 reaches end of life this
month; pip and uv keep installing 0.11.1 on 3.10. Pushdown reports can now show values,
and any run can feed a dashboard through OpenTelemetry.

Set `pushdown_sample_rows` on a comparison that runs in a warehouse or inside Postgres,
and one more statement fetches up to that many changed rows, lowest keys first, with
both sides' values and each column's match flag, laid out like a local run's changed
rows. The HTML report shows them in place of the bare keys, `DiffResult.changed_sample`
holds them, and `output_path` writes them as `changed_rows_sample`. The counts do not
change. Values do leave the warehouse, so the default, `0`, fetches nothing, and the
Markdown summary that CI posts, the `--json` summary, and the logs never carry them.

`veridelta run --otel PATH` writes the run's metrics as one line of OTLP JSON for an
OpenTelemetry Collector or any OTLP/HTTP endpoint, with no new dependency: rows on each
side, added, removed, and changed rows, mismatched rows for every compared column, the
mismatch ratio, and the verdict, each a gauge. They carry counts and column names,
never row values, connection URIs, credentials, or SQL. The GitHub Action and the
GitLab CI template write the file into their artifact, and the Action exposes its path
as the `otel-metrics` output. A fifth tutorial takes a database source through
`veridelta validate` and into the CI integrations.

### Feat

- fetch up to `pushdown_sample_rows` changed rows with both sides' values in a pushdown
  run, shown in the HTML report and written as `changed_rows_sample`
- write OpenTelemetry metrics with `veridelta run --otel`, and from the GitHub Action
  and the GitLab CI template

### Refactor

- make `SQLDialect` a `StrEnum`, so `str()` of a dialect is its value

### Chore

- finish a release that is still waiting for approval when another change merges to
  `main`, and upload only the files PyPI does not have yet
- require Python 3.11 or later, and test 3.11 through 3.14

### BREAKING CHANGE

- Veridelta requires Python 3.11 or later

## v0.11.1 (2026-10-05)

Two Postgres tables can now be compared inside Postgres, and warehouse pushdown trims
whitespace and reads regex replacements the way a local run does. Those two settings
compared differently in a warehouse and now give the local verdict there too, so a
run can report fewer differences than it did on 0.11.0.

Set `pushdown: true` on two `type: database` sources that name tables on one Postgres
connection, and the comparison compiles to SQL and runs inside Postgres through
ConnectorX, so only counts and primary keys come back. Both sides must set it; a
database source without it is still read into memory and compared locally. Postgres
refuses `datetime_format` and `max_levenshtein_distance` with `ConfigError`, since it
has no date parse that returns NULL and its `levenshtein` needs an extension, and the
server must keep `standard_conforming_strings` on, as it is by default. CI runs the
parity suite against a live Postgres. ConnectorX reads every Postgres `numeric` as
`Decimal(38, 10)` whether or not `pushdown` is set; the configuration guide explains
what that changes.

`whitespace_mode` strips the same characters in a warehouse as in a local run: tabs,
line breaks, no-break spaces, and the rest of Unicode's whitespace, not only spaces.
Before, Snowflake, Databricks, and DuckDB pushdown counted values padded with those
characters as changed, and keys padded with them as added and removed rows.

A `regex_replace` replacement now means the same in every warehouse. Write group
references as Polars reads them, `$1` or `${1}`, with `$0` for the whole match and
`$$` for a dollar sign, and pushdown rewrites them for each warehouse: `\1` on
Snowflake, BigQuery, DuckDB, and Postgres, and `$1` on Databricks. A reference to a
group by name, which includes `$1a`, or to a group above 9 raises `ConfigError` on a
warehouse pair, and `veridelta validate` warns about it. A replacement written for
Snowflake as `\1` is now plain text in the warehouse, as it always was in a local run.

### Feat

- compare two Postgres tables inside Postgres when both `database` sources set
  `pushdown: true`

### Fix

- strip tabs, line breaks, and Unicode whitespace in pushdown `whitespace_mode`, as
  Polars does
- rewrite `regex_replace` group references for each warehouse, and refuse references
  to a group by name or above 9

### Chore

- tag the version, publish it to PyPI after approval, and create the GitHub release
  when a version bump merges to `main`

## v0.11.0 (2026-10-04)

Veridelta now reads operational databases, compares BigQuery tables in place, and
checks a configuration before it runs. Read the behavior changes first: a few
comparisons that matched on 0.10.0 because of arithmetic or parsing bugs now report
drift that is really there.

A `type: database` source reads a table, or the result of a query, from Postgres,
MySQL or MariaDB, SQL Server, Oracle, Redshift, ClickHouse, or SQLite through
ConnectorX, installed with the new `database` extra. The read is eager and the
comparison runs locally, so a database pairs with a file, a lakehouse table, or
another database, and `crosswalk` reads it too. A `query` is sent verbatim, so connect
with a read-only role. A `password` field is percent-encoded into the URI and kept out
of printed configs and error messages.

BigQuery joins Snowflake and Databricks for warehouse pushdown through the new
`bigquery` extra. It authenticates with Application Default Credentials or a key
file, and `maximum_bytes_billed` caps what each statement may scan. The dialect
follows the GoogleSQL reference and is tested against a stand-in client; it has not
yet run against a live project. `veridelta crosswalk` now also runs inside a
warehouse when both tables share one connection, counting every candidate column in
one statement. There, only columns stored as text on both sides qualify.

`veridelta validate` checks a configuration without reading any rows: backend
pairing, missing extras, and regex patterns Polars rejects, plus each side's columns
with `--schemas`. `--allow-missing-env` lets it run in CI without secrets. A JSON
Schema gives editors completion and validation as you type, and `veridelta schema`
prints it. `veridelta run --markdown` writes a summary for pull requests, and a
composite GitHub Action and a GitLab CI template run a comparison and keep one summary
comment up to date. Pin either to `v0.11.0` or later. Avro files are read with
`format: avro`, from local paths and eagerly.

Several parity fixes change results. Local integer tolerances measure the true
difference, where `Int8` `100` against `-100` used to wrap into a match. `%f` in
`datetime_format` reads a fraction of a second, so `.5` is 500 ms rather than 5 ns.
`strict_types: true` now applies in warehouse comparisons, which used to treat `10.0`
and `10` as equal. DuckDB pushdown replaces every regex match, and pushdown tolerances
widen integer operands, so narrow and extreme values no longer overflow. A Hypothesis
property test compares local and pushdown verdicts on generated data in every CI run.

`veridelta[all]` now includes ConnectorX, which publishes no wheels for musllinux or
Windows on ARM, so install only the extras you need there.

### Feat

- read a table or query from Postgres, MySQL, SQL Server, Oracle, Redshift,
  ClickHouse, or SQLite with `type: database`, through the new `database` extra
- compare two BigQuery tables in place with `type: bigquery`, through the new
  `bigquery` extra, with an optional `maximum_bytes_billed` cap
- propose `value_map` entries inside the warehouse when both tables share one
  connection
- check a configuration without reading rows with `veridelta validate`, adding each
  side's columns with `--schemas`
- generate a JSON Schema for configuration files, print it with `veridelta schema`,
  and publish it with the docs for editors
- write a Markdown summary with `veridelta run --markdown`
- run a comparison in CI with a composite GitHub Action or a GitLab CI template, each
  keeping one summary comment on the pull or merge request
- read Avro files with `format: avro`

### Fix

- widen integer pairs before applying a local tolerance, so a difference no longer
  wraps into a match
- read `%f` in `datetime_format` as a fraction of a second, without Polars'
  `ChronoFormatWarning`
- replace every regex match in DuckDB pushdown
- widen integer operands in pushdown tolerances, so narrow and extreme values no
  longer overflow
- enforce `strict_types` in warehouse comparisons
- reject crosswalk thresholds that are not plain numbers, and a `min_support` that is
  not an `int`

### Refactor

- route every warehouse pair through one registry, and plan a run from the schemas
  alone, so `validate` checks exactly what a run would compile

### Chore

- publish to PyPI only for `vX.Y.Z` tags, so a floating action tag cannot publish
- compare local and pushdown verdicts on generated data with Hypothesis in CI

### BREAKING CHANGE

- `%f` in `datetime_format` reads `.5` as 500 ms instead of 5 ns
- local integer tolerances use the true difference, so pairs that wrapped into a match
  now mismatch
- `strict_types: true` fails warehouse columns whose types differ after normalization
- crosswalk thresholds must be `int` or `float` values other than `bool`, and
  `min_support` must be an `int`

## v0.10.0 (2026-09-26)

Warehouse pushdown now reaches the same verdict as a local run in every case the
differential harness covers, and several local comparisons that reported a match on
values that differ are fixed. Read the behavior changes before upgrading: a run that
passed on 0.9.1 can fail on 0.10.0 because the data really does differ.

Snowflake pushdown failed on every real run before this release. The driver returns
`None` for a zero-row result, and every run starts with a zero-row schema probe, so
the `snowflake` extra now requires `snowflake-connector-python` 3.7.0. String literals
are escaped per dialect: a backslash in a regex or sentinel reaches Snowflake and
Databricks intact, an apostrophe survives on Databricks, and a value ending in a
backslash can no longer close its literal and run as part of the statement. Primary
keys go through the same normalization in a warehouse join as in a local run, and a
key that repeats after normalization raises `DataIntegrityError` in the warehouse too,
so added, removed, and changed counts can change.

Numeric comparisons no longer cast the target to the source's type. With
`strict_types` off, an integer `10` and a float `10.7` now differ, as they already did
in a warehouse, and a `Float32` `0.1` no longer equals a `Float64` `0.1`; add a
tolerance or a `cast_to` to forgive precision gaps. Under a tolerance, NaN matches only
NaN and an infinity only itself, on both paths, and unsigned differences no longer
wrap.

Strings in the `source` and `target` blocks expand `${NAME}` and `${NAME:-default}`
from the environment, so credentials can stay out of the file. A literal `${` must now
be written `$${`. Printed configs leave out `password`, `access_token`, and
`storage_options`, and validation errors no longer quote the input they reject;
`model_dump()` still returns every field.

Two additions: `max_levenshtein_distance` and `min_jaro_winkler_similarity` forgive
typos in text columns through the new `fuzzy` extra, and `veridelta crosswalk` proposes
`value_map` entries from how the source and target values line up, with the evidence
for each. Jaro-Winkler runs locally only; pushdown refuses it before any comparison
query runs.

The tutorials are now one numbered path of four notebooks, `01_core_concepts` through
`04_html_reports`, so the old notebook URLs return 404. The README and the site index
were rewritten around them.

### Feat

- expand `${NAME}` and `${NAME:-default}` in `source` and `target` strings, with `$${`
  for a literal `${`, and name the variable and its location when one is unset
- forgive text typos within `max_levenshtein_distance` or
  `min_jaro_winkler_similarity`, scored locally through the new `fuzzy` extra. Pushdown
  compiles the edit distance to `EDITDISTANCE` or `levenshtein` and refuses
  Jaro-Winkler before any comparison query runs
- propose `value_map` entries with `veridelta crosswalk` and
  `DiffEngine.propose_value_maps()`, printing paste-ready rules on stdout and the
  evidence for each entry on stderr
- normalize primary keys through stages 1-7 in every warehouse join, accept renamed
  keys, and reject keys that repeat after normalization with the same
  `DataIntegrityError` a local run raises
- give connectors `close()` and context-manager support, log connections and
  statements without SQL or credentials, and tell a missing lakehouse extra apart from
  a failed scan

### Fix

- pass `force_return_table=True` to Snowflake's Arrow fetch and require
  `snowflake-connector-python>=3.7.0`, so a zero-row result no longer fails the run
- escape string literals per dialect, so backslashes and apostrophes survive and a
  trailing backslash cannot escape its literal
- compare mixed numeric types in their common type instead of casting the target to
  the source's type
- stop NaN and infinity from matching any value under a tolerance, and measure
  unsigned differences without wrapping
- apply tolerances in pushdown only to columns a local run compares as numbers, and
  resolve one rule per column by its post-rename name on both paths
- count a row with one NULL side in pushdown's `changed_count`, and select no rows
  when every compared column is ignored
- count rows with null keys in the row totals, as the warehouse's `COUNT(*)` does
- drop pattern-ignored columns from the target side too, so `schema_mode: exact` no
  longer fails on them
- reject an empty `primary_keys`, an infinite tolerance, the same table on both sides
  of a pushdown, headers that collide after normalization, and a negative
  `--html-max-rows`
- keep the HTML report readable when a value is NaN or infinite, and show integers
  beyond 2^53 exactly
- leave `password`, `access_token`, and `storage_options`, including a file source's
  nested one, out of printed configs, and keep inputs out of validation errors

### Refactor

- share one rule folder and one rename and drop matcher between the two engines, and
  split `DiffEngine.run` into named stages
- raise `DatasetError`, a `VerideltaError`, from `load_nyc_taxi` instead of
  `RuntimeError`

### Chore

- run CI on stacked pull requests and execute the tutorial notebooks against their
  recorded output
- enable Ruff's McCabe complexity check at 10 and tighten the Cursor rules

### BREAKING CHANGE

- a literal `${` inside `source` or `target` must be written `$${`
- mixed numeric types compare by value, so integer and float columns that differ only
  in the fraction now mismatch locally
- `load_nyc_taxi` raises `DatasetError`, which `except RuntimeError` no longer catches
- the tutorial notebooks were renamed, and their old URLs return 404

## v0.9.1 (2026-09-11)

CI now pins JavaScript actions that declare Node 24, so GitHub-hosted runners
stop forcing the deprecated Node 20 runtime onto checkout, setup-uv, and
codecov.

### Chore

- bump `actions/checkout` to v5, `astral-sh/setup-uv` to v7, and
  `codecov/codecov-action` to v6 so every workflow step we pin runs on Node 24

## v0.9.0 (2026-09-11)

Internal. The public API does not change. This is the 1.0 candidate: the
breaking-change budget for the 0.7–0.9 line was spent in 0.7.0, 0.8.0 was
additive, and 0.9.0 is compiler and toolchain work.

Warehouse SQL now projects stages 1–7 once per column through a pair of
normalization CTEs. The join and the mismatch tally read those values, so
adding a transform no longer copies the expression tree into every later
predicate. Emitted SQL stays roughly linear in the number of compared
columns; the DuckDB harness is the same check it was in 0.6.0.

`engine.py`, `models.py`, `sentinels.py`, and `connectors/sql.py` are gated
at 100% branch coverage. `cast_to` is resolved through a `TypedDict` and an
exhaustive lookup table. `make lint` runs strict pyright. Global mypy
`ignore_missing_imports` is gone; missing stubs stay on per-module overrides
for optional drivers and test-only imports.

### Refactor

- project warehouse transforms through `_src_normalized` / `_tgt_normalized`
  CTEs so each column is normalized once, verified by the existing DuckDB
  harness and a SQL-length regression test
- convert `_get_effective_rule` to an `EffectiveRule` TypedDict so `cast_to`
  stays a `CastTarget` and `_CAST_TARGETS` is exhaustive

### Test

- fail `make test` and the core CI job unless the four core modules stay at
  100% branch coverage, and drop the dead `raise NotImplementedError`
  coverage exclude

### Chore

- add strict pyright to `make lint` and the CI lint job
- drop the global mypy `ignore_missing_imports` in favor of the existing
  per-module overrides

## v0.8.0 (2026-09-11)

Reporting and CLI polish. Purely additive: new flags default off, and the
exit codes stay 0 for a match and 1 for drift.

`--html PATH` writes one self-contained file. Styles and a 25-row pager are
inlined, so an air-gapped runner can open the report without fetching anything.
Embedded rows are capped (`--html-max-rows`, default 1000) and the truncation is
stated. Pushdown reports are labeled as primary-keys-only. Markup in column
names is escaped, and `<` inside the embedded JSON is rewritten so a value
cannot close the script block.

Progress chatter moved to stderr so `veridelta run --json | jq` does not have
to strip it. `--quiet` silences that chatter. `--version` / `-V` print the
package version.

`docs/configuration.md` is now bound to `DiffConfig`, `DiffRule`, and
`SourceConfig` by a test, so a new field cannot ship undocumented. A Getting
Started notebook covers local diffing and the `DiffResult` accessors added in
0.7.0.

### Feat

- write a standalone HTML report from the Python API or `--html`, with an
  inlined pager and a row cap that is visible in the page
- add `--json` so CI can read `DiffSummary` from stdout, `--quiet` to suppress
  progress, and `--version` / `-V`
- route every progress line to stderr, including the artifact path, so a piped
  `--json` run stays valid JSON
- add a Getting Started notebook and bind `docs/configuration.md` to the three
  config models so missing fields fail the test suite

### Fix

- explain configuration errors as a problem with the file, not the data, and
  point unexpected failures at the issue tracker with the exception class

## v0.7.0 (2026-09-10)

Two breaking changes, both mechanical.

`DiffEngine.run()` and `DiffEngine.run_from_configs()` now return a `DiffResult` rather
than a `DiffSummary`. Existing code reaches the old value through `.summary`:

```python
summary = DiffEngine(config, src, tgt).run().summary
```

`SourceType` and `output_format` are now closed sets. `SourceType` accepts `csv`,
`parquet`, `json`, `ndjson`, `arrow`, and `excel`; `output_format` accepts everything
but `excel`. The removed names (`fixed_width`, `netcdf`, `shapefile`, `geopackage`,
`sql`, `avro`, `xml`, `delta`) never had an implementation and already raised
`ConfigError` at run time, so a config using one was already broken; it now fails when
the config loads instead. Delta Lake is unaffected and still reached through the
`delta_lake` source type.

### Feat

- return `DiffResult` from the engine, carrying the added, removed, and changed frames
  alongside the summary. The rows were already materialized, counted, and discarded, so
  reaching them previously meant configuring `output_path` and reading files back
- add `DiffResult.get_mismatches(column)` to narrow the changed set to one column with
  the source and target values side by side, and `DiffResult.to_pandas()` for notebooks
- record the compared column set on `DiffResult`, so a mistyped column name is rejected
  on the pushdown path too, where the primary-key frames cannot reveal which columns
  were compared
- read JSON, NDJSON, Arrow IPC, and Excel. NDJSON and Arrow scan lazily; JSON and Excel
  are read whole, because Polars has no lazy reader for either
- write discrepancy artifacts as JSON, NDJSON, or Arrow IPC in addition to CSV and
  Parquet
- add an `excel` extra, reporting a missing install as a hint rather than an
  `ImportError` raised from inside Polars
- export the exception hierarchy, the warehouse and lakehouse configs, and the public
  type aliases from the package root. The docs already instructed users to catch
  `VerideltaError` and to construct a `SnowflakeConfig`, neither of which was reachable
  without importing from a submodule

### Fix

- ship the `py.typed` marker. The package advertised the `Typing :: Typed` classifier
  without it, so PEP 561 had every downstream mypy and pyright treat Veridelta as
  unannotated no matter how complete its annotations were
- bind `SourceType` and `ArtifactFormat` to the registries that implement them, with
  tests. A name could previously be advertised in the config schema with nothing behind
  it

### BREAKING CHANGE

- `DiffEngine.run()` and `DiffEngine.run_from_configs()` return `DiffResult`
- `SourceType` and `output_format` are constrained to implemented formats

## v0.6.0 (2026-09-10)

Warehouse pushdown now implements all nine transform stages. `pad_zeros`,
`datetime_format`, `timezone`, and `cast_to` previously raised `ConnectorError` on the
warehouse path, so any rule using them could only run locally.

`cast_to` is now a closed set of `Int64`, `Float64`, `String`, `Boolean`, `Date`, and
`Datetime`. Any other value is rejected at load time rather than silently skipping the
cast, so a config with a typo that previously reported a clean diff will now fail
validation. Configs already using valid Polars type names are unaffected.

### Feat

- compile `pad_zeros` as a sign-aware, non-truncating expression rather than an `LPAD`,
  which pads in front of a minus sign (`0-12` where Python's `zfill` gives `-012`) and
  discards characters past the target width
- compile `cast_to` through a per-dialect keyword table, truncating toward zero on a
  float-to-integer cast to match Polars where Snowflake and DuckDB round
- compile `datetime_format` by translating one directive at a time against a per-dialect
  table, with literal runs restricted to a separator allowlist and wrapped in the
  dialect's quoting. An untranslatable directive raises `ConfigError` rather than passing
  through, since an unrecognized directive parses nothing and returns NULL for every row
- enforce the `timezone` precondition against the probed warehouse schema, so a naive or
  non-temporal column fails the same way it does locally. The conversion itself emits no
  SQL: Polars rewrites only a column's timezone label and every downstream cast still
  reads the UTC instant, while warehouses have no per-column label to rewrite
- add a differential test harness that runs both engines over the same frames and
  compares the results, executing real compiler output through DuckDB

### Fix

- skip the text transform stages on non-text columns during pushdown, matching the local
  engine. A currency string compared against a native float previously asked the
  warehouse to run `REGEXP_REPLACE` over a number

### BREAKING CHANGE

- `cast_to` is constrained to a `Literal` of supported Polars type names. It previously
  accepted any string and resolved it with `getattr`, so an unrecognized name left the
  column uncast and the diff green. The field also reaches SQL as `CAST(x AS <type>)`,
  where a type name cannot be quoted or bound as a parameter

## v0.5.1 (2026-09-10)

Unsupported formats now raise `ConfigError` instead of `NotImplementedError`. Code
catching `NotImplementedError` around `LoaderFactory` or a diff run should catch
`ConfigError`, or `VerideltaError` for all framework failures.

### Fix

- raise `ConfigError` rather than `NotImplementedError` for a source format with no
  loader or an `output_format` with no writer, so the CLI reports "Configuration Error"
  instead of "Unexpected System Error" for an ordinary misconfiguration
- validate the artifact format before writing rather than inside the write loop. The
  check previously ran after the empty-frame guard, so a bad `output_format` was caught
  only when there was drift to write and a clean comparison passed silently
- name the supported formats in both messages, derived from the registry that backs the
  behavior so the message cannot drift from what actually works

## v0.5.0 (2026-09-09)

Null sentinels are no longer strings only. `null_values` and `default_null_values`
accept mixed scalars, and quoting now carries meaning: `-999` nulls out `-999` in a
numeric column and is ignored on a text column, while `"-999"` behaves the other way
around. A list that previously read `["N/A", "-999"]` still works unchanged; add the
unquoted form if the value also appears as a number.

### Feat

- widen `null_values` and `default_null_values` from `list[str]` to a mixed union of
  `str | int | float | bool`, with `strict=True` so Pydantic preserves the exact type
  each sentinel was written as
- filter sentinels against each column's dtype before use, in both the local engine and
  the warehouse compiler, so one global list can span a mixed schema. Text sentinels
  reach string, categorical, and enum columns; numbers reach any numeric column
  including decimals; booleans reach boolean columns only
- carry probed dtypes out of the warehouse schema probe into both pushdown query
  builders, filtering each side independently since the two relations can disagree
- raise `ConfigError` when an explicit per-column `null_values` rule holds no sentinel
  its column's type can match, locally and against the probed warehouse schema. A global
  `default_null_values` still skips silently, since spanning a mixed schema is its purpose
- reject `.nan` and `.inf` sentinels at load time, as NaN never compares equal to itself
  and infinity has no portable SQL literal

### Refactor

- emit one `CASE WHEN col IN (...) THEN NULL ELSE col END` per side instead of nested
  `NULLIF` calls, rendering numbers and booleans unquoted so they cannot break a numeric
  column's cast. Strings still route through `_literal` for apostrophe escaping

## v0.4.0 (2026-09-09)

Transforms now apply uniformly to every column, primary keys included, before the
joins. A `case_insensitive`, `whitespace_mode`, or `value_map` rule on a key column
therefore changes how rows are matched, not only how they are compared. Key
uniqueness is asserted after normalization, so a rule that collapses two keys into
one raises `DataIntegrityError` instead of exploding the join.

### Feat

- consolidate every column transform into one normalization pass per dataset, executed
  before the joins, and make the `DiffRule` docstring the canonical transform order that
  both the local engine and the SQL compiler follow
- implement `pad_zeros`, which stringifies first so a numeric `123` matches a text `"00123"`
- implement `datetime_format`, which parses text into timestamps so the column is compared
  as a timestamp rather than as text
- implement `timezone`, which converts timezone-aware data only and raises `ConfigError`
  for naive timestamps rather than assuming an origin zone and shifting every value
- compare every shared column on the warehouse path, with global `default_*` settings
  folded in, instead of only columns carrying an explicit rule
- populate `column_mismatches` on warehouse pushdown from a per-column `SUM(CASE ...)`
  tally, using `COALESCE(pred, FALSE)` to match the local engine under SQL three-valued logic
- write warehouse pushdown artifacts through the shared exporter under `_pks_only`
  filenames, since those queries project primary keys only
- reject a `pad_zeros` width supplied as a string or float instead of coercing it

### Fix

- stop applying `regex_replace` twice, once in the `run()` pre-pass and again during
  comparison, which corrupted any non-idempotent pattern
- stop a global `default_null_values` from failing every run containing numeric columns,
  by gating text transforms on string dtypes
- reject an unknown `timezone` name as a `ConfigError` naming the column, rather than
  surfacing a raw Polars error

## v0.3.0 (2026-09-08)

### Feat

- warehouse and lakehouse connectors with SQL pushdown (#5)
- add Snowflake and Databricks warehouse extras with Arrow SQL pushdown
- add Delta Lake and Iceberg lakehouse scans, including Iceberg `snapshot_id` time travel
- route YAML `source`/`target` through a discriminated `SourceRef` union and `DiffEngine.run_from_configs`
- compile warehouse anti-joins and row counts so pushdown reports added, removed, and changed
  counts against exact source and target totals
- enforce `schema_mode` and primary-key existence on warehouse relations via zero-row column probes
- allowlist dotted warehouse table identifiers and reject string-coerced numeric config fields
- report artifact persistence through `DiffSummary.artifacts_written`
- add `make docs` and `make docs-serve`, and build the documentation strictly in CI

### Fix

- stop the CLI from announcing discrepancy artifacts that warehouse pushdown never writes
- keep the Snowflake extra inside the driver's supported pyarrow range on Python 3.14
- update Python classifiers and correct homepage URL in pyproject.toml

## v0.2.0 (2026-04-30)

### Feat

- **core**: migrate to Polars Lazy computation graphs and implement enterprise CI/CD (#4)
- implement core semantic diffing engine and GitOps configuration (v0.1.0-alpha) (#3)
- add dynamic output_format for diff artifacts with strict fallback
- add RELAXED_ORDER schema validation mode
- implement core diff engine, schema validation, and documentation (#2)

### Fix

- update dataset URL to use versioning from package metadata
- satisfy CI requirements and trigger docs deployment
- add missing tests directory and modernize uv config
