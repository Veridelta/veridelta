---
name: veridelta
description: Compares two datasets with the Veridelta command line and reports what differs, keeping row values out of the reply. Use when a task compares two tables or files, checks a data migration or pipeline change, or writes or fixes a veridelta.yaml file.
metadata:
  version: "1.0.0"
---

# Compare two datasets with Veridelta

Veridelta compares two datasets, such as two files or two warehouse tables, under the rules in a YAML configuration file. These steps keep a run predictable and a reply free of row values.

## Steps

1. Check the configuration file. `validate` reads no rows, and by default connects to nothing:

    ```bash
    veridelta validate -c veridelta.yaml --json
    ```

    Fix each entry in `errors` before a run. The command exits `1` while there is one.

2. Run the comparison, so stdout carries only the summary:

    ```bash
    veridelta run -c veridelta.yaml --json
    ```

3. Read the exit code before the output. `0` is a match within `threshold`. `1` is drift, or a failure: drift prints the summary on stdout, and a failure prints nothing there and explains itself on stderr. `2` is an invalid command line.

4. Report the counts, and the columns in `column_mismatches`, which is all the summary holds. Leave row values out of a reply unless the user asks for them. The discrepancy files, the HTML report, a Markdown summary that lists values, and the proposals of `veridelta crosswalk` all hold row values.

5. When two columns hold the same values in different encodings, such as `Y` and `true`, propose a `value_map` from the data:

    ```bash
    veridelta crosswalk -c veridelta.yaml --json
    ```

    Show the proposals and their evidence to the user before adding them to the configuration.

## Writing a configuration

`veridelta schema` prints the JSON Schema of the configuration file. Check a draft against it, then run `veridelta validate`.

## Reference

- The guide for AI agents: https://veridelta.github.io/veridelta/agents/
- Every page of the docs in one file: https://veridelta.github.io/veridelta/llms-full.txt
