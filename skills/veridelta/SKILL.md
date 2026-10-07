---
name: veridelta
description: Compares two datasets with the Veridelta command line and reports what differs, keeping row values out of the reply. Use when a task compares two tables or files, checks a data migration or pipeline change, or writes or fixes a veridelta.yaml file.
metadata:
  version: "1.11.0"
---

# Compare two datasets with Veridelta

Veridelta compares two datasets, such as two files or two warehouse tables, under the rules in a YAML configuration file. These steps keep a run predictable and a reply free of row values.

## Steps

1. Check the configuration file. `validate` reads no rows, and by default connects to nothing:

    ```bash
    veridelta validate -c veridelta.yaml --json
    ```

    Fix each entry in `errors` before a run. The command exits `1` while there is one, and `3`, with an `error` object, when the check cannot finish.

2. Run the comparison, so stdout carries only the summary:

    ```bash
    veridelta run -c veridelta.yaml --json
    ```

3. Read the exit code before the output. `0` is a match within `threshold`, and `1` is drift: both print the summary on stdout. `3` is a run that could not finish: stdout holds one object, `{"error": {"type": ..., "message": ...}}`, and stderr explains it too. A `ConfigError` means the configuration needs a fix. `2` is an invalid command line.

4. Report the counts, and the columns in `column_mismatches`, which is all the summary holds. Leave row values out of a reply unless the user asks for them. The discrepancy files, the HTML report, a Markdown summary that lists values, the proposals of `veridelta crosswalk`, and the example keys of `veridelta suggest` all hold row values.

5. When two columns hold the same values in different encodings, such as `Y` and `true`, propose a `value_map` from the data:

    ```bash
    veridelta crosswalk -c veridelta.yaml --json
    ```

    Show the proposals and their evidence to the user before adding them to the configuration.

6. When a column differs by small amounts, such as rounding, padding, or case, suggest a rule from the data:

    ```bash
    veridelta suggest -c veridelta.yaml --json
    ```

    Each suggestion names the rows it explains, and its example keys come from the data. Show the suggestions to the user before adding a rule: a rule forgives what it explains in later runs too, such as every gap below a tolerance.

## As MCP tools

Where the `mcp` extra is installed, `veridelta mcp` serves steps 1, 2, and 5 as Model Context Protocol tools, `validate_config`, `run_comparison`, and `propose_value_maps`, which return what `veridelta validate --json`, `veridelta run --json`, and `veridelta crosswalk --json` print. A third tool, `describe_schema`, lists one side's columns and their types, and reads no rows, which helps when a rule must name a column. `read_discrepancies` and `propose_value_maps` return values from the data, so they work only on a server started with `--allow-row-values`; keep those values out of a reply unless the user asks. The guide for AI agents shows how to register it with a host.

## Writing a configuration

`veridelta schema` prints the JSON Schema of the configuration file. Check a draft against it, then run `veridelta validate`.

## Checking the output

`veridelta schema run` prints the JSON Schema of what `veridelta run --json` prints, and `validate`, `crosswalk`, `suggest`, and `error` name the others. Check what you parse against them.

On a pull request, the comment the GitHub Action keeps ends with the run's summary as JSON, on the line after `<!-- veridelta-summary`, in an HTML comment that readers never see. Parse that line to read the result without running the comparison again.

## Fixing drift in a loop

After changing a pipeline, run the comparison again, on the pull request or with `veridelta run --json`, and read `added_count`, `removed_count`, `changed_count`, and `column_mismatches`. Find the cause in the change, not in the data, fix it, and run again. Stop when the run matches, or when the drift left is what the user asked for, and report the counts and the columns. Never add a rule or raise `threshold` to make a run pass unless the user agrees.

## Reference

- The guide for AI agents: https://veridelta.github.io/veridelta/agents/
- Every page of the docs in one file: https://veridelta.github.io/veridelta/llms-full.txt
