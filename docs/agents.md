# AI agents

An AI agent, such as a coding assistant, runs Veridelta through its command line or through its MCP server. This page gives the steps that keep its runs predictable and its replies free of row values.

## Steps

1. Check the configuration file. `validate` reads no rows, and by default connects to nothing:

    ```bash
    veridelta validate -c veridelta.yaml --json
    ```

    The JSON holds the `config` it checked, `valid`, `errors`, and `warnings`, and the command exits `1` when there is an error. Fix each error before a run. A check that cannot finish exits `3` and prints an `error` object instead. `--allow-missing-env` checks a file whose secrets are not set, such as in a pull request job.

2. Run the comparison with `--json`, so stdout carries only the [summary](results.md#summary):

    ```bash
    veridelta run -c veridelta.yaml --json
    ```

3. Read the run's [exit code](cli.md#exit-codes) before its output:

    | Code | Meaning | stdout |
    | :--- | :--- | :--- |
    | `0` | A match within `threshold`. | The summary. |
    | `1` | Drift. | The summary. |
    | `2` | Invalid arguments. | Nothing. |
    | `3` | The run could not finish. | One object, `{"error": {"type": ..., "message": ...}}`, which stderr explains too. |

    Read `type` before acting on `message`. A `ConfigError` means the configuration file needs a fix, and [Exit codes](cli.md#exit-codes) says what the other types mean. Any type that is not a Veridelta error, such as one from a driver, is worth reporting to the user as a possible bug.

4. Report counts and column names, which is all the summary holds. Leave row values out of a reply unless the user asks for them.

5. When two columns hold the same values in different encodings, such as `Y` and `true`, propose a `value_map` from the data:

    ```bash
    veridelta crosswalk -c veridelta.yaml --json
    ```

    Show the proposals and their evidence to the user before adding them to the configuration.

6. When a column differs by small amounts, such as rounding, padding, case, or a spelling of NULL, suggest a rule from the data:

    ```bash
    veridelta suggest -c veridelta.yaml --json
    ```

    Each suggestion names the rows it explains, and its example keys come from the data. Show the suggestions to the user before adding a rule: a rule forgives what it explains in later runs too, such as every gap below a tolerance.

## MCP server

`veridelta mcp` serves steps 1, 2, and 5 above as [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) tools, so an agent's host can call them without a shell. Two more tools list a side's columns and read the rows that differ. It needs the `mcp` extra:

```bash
uv add 'veridelta[mcp]'
```

Register the server with the host from the folder that holds the configuration files. In Claude Code, this command does it:

```bash
claude mcp add veridelta -- uv run veridelta mcp --root .
```

A host that reads its servers from a file takes the same command as an entry. Claude Code reads `.mcp.json` at the project's root, and Cursor reads `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "veridelta": {
      "command": "uv",
      "args": ["run", "veridelta", "mcp", "--root", "."]
    }
  }
}
```

Where Veridelta is installed without uv, the command is `veridelta mcp` itself. To let the agent read rows, add `--allow-row-values` to the command. The server has five tools, and each takes `path`, the configuration file, which a relative path reads against the first root:

| Tool | Returns | Reads |
| :--- | :--- | :--- |
| `validate_config` | What `veridelta validate --json` prints: `config`, `valid`, `errors`, and `warnings`. Its `schemas` and `allow_missing_env` arguments work as the command's flags do. | No rows. With `schemas`, each side's columns. |
| `run_comparison` | What `veridelta run --json` prints, the [summary](results.md#summary), with `verdict`, `match` or `drift`, the `exit_code` the command gives, 0 or 1, and `artifacts_written`. | Both sides. |
| `describe_schema` | One side's `columns`, each name as stored mapped to its type as Polars names it, such as `Int64`, and the `side`. Its `side` argument is `source` or `target`. | That side's columns, and no rows. A side that reads a `query` is refused, since only running it would name its columns. |
| `read_discrepancies` | The rows of one `kind`, `added`, `removed`, or `changed`, up to `limit`, 20 by default: `kind`, `total`, `rows`, `truncated`, and `keys_only`, which says the pair was compared in place, so each row holds its primary key alone. | Both sides, since each call runs the comparison. Only on a server started with `--allow-row-values`. |
| `propose_value_maps` | What `veridelta crosswalk --json` prints, as `proposals`, with `total` and `truncated`. Its `min_confidence`, `min_support`, and `sample_fraction` arguments work as the command's flags do. | Both sides. Only on a server started with `--allow-row-values`. |

<details markdown>
<summary>Watch a client call three tools, as an agent's host does</summary>

![A terminal runs a script that starts veridelta mcp with row values allowed and calls three tools. validate_config answers valid with no errors. run_comparison answers drift, exit code 1, with one added, one removed, and one changed row, and one mismatch in status. read_discrepancies returns the changed row, id 2, whose status is closed in the source and shipped in the target.](assets/demo-mcp.gif)

```text
> # A client calls the MCP server's tools, as an agent's host does.
> python mcp_client.py
-> validate_config {"path": "veridelta.yaml"}
<- {
  "valid": true,
  "errors": [],
  "warnings": []
}
-> run_comparison {"path": "veridelta.yaml"}
<- {
  "verdict": "drift",
  "exit_code": 1,
  "added_count": 1,
  "removed_count": 1,
  "changed_count": 1,
  "column_mismatches": {
    "status": 1
  }
}
-> read_discrepancies {"path": "veridelta.yaml", "kind": "changed"}
<- {
  "total": 1,
  "truncated": false,
  "rows": [
    {
      "id": 2,
      "status_source": "closed",
      "amount_source": 20.5,
      "status_target": "shipped",
      "amount_target": 20.5,
      "status_is_match": false,
      "amount_is_match": true
    }
  ]
}
```

</details>

The person who starts the server decides what it may read, and no tool call can change that:

- Each `--root` names a folder the tools may read configuration files from, and a path outside every root fails the call. The server runs in the first root, so a relative path resolves there.
- A tool that opens a side reads files on this machine only from under the roots, `~` and links included, since a column name or an error can carry a file's text as a row does. Data elsewhere, such as an object store or a warehouse, is read as the command line reads it.
- A run writes the rows that differ to the configuration's `output_path`, so `run_comparison` and `read_discrepancies` refuse one outside every root before they read a row.
- A side's `query` runs as written, with the configuration's credentials, so a tool that would run one refuses unless the server is started with `--allow-queries`. Even then, a DuckDB file's connection reads other files, attaches databases, and loads extensions only from under the roots. A MotherDuck connection is not held to the roots.
- A tool returns findings, counts, and column names, never the configuration or a value from the data, unless the server is started with `--allow-row-values`. A password inside an error is masked, as on the command line, and so is every value of four characters or more that the configuration takes from an environment variable, wherever it appears in an answer.
- With `--allow-row-values`, `read_discrepancies` returns at most `--max-rows` rows per call, 50 by default. `propose_value_maps` returns at most that many value map entries, counting the entries a column's rule already has, and a proposal comes back whole or not at all.
- With `schemas`, a check reads each side's columns as `veridelta validate --schemas` does, and without it a check opens no data. `describe_schema` reads one side's columns the same way, and a warehouse table with the probe a run starts with.
- A host may start the server with only some of the user's environment variables. A `${NAME}` that the configuration references must reach the server, through the host's `env` setting for it if need be, or the check reports it unset. `allow_missing_env` checks a file without them.

A call that fails returns its error's type and message, as `run --json` prints them, such as `ConfigError` for a path outside the roots. [Serving tools to an agent](cli.md#serving-tools-to-an-agent) lists the command's flags.

## Where row values appear

The summary holds counts and column names only. These outputs hold values from the data:

- the discrepancy files a run writes to `output_path`;
- the [HTML report](results.md#html-report);
- a [Markdown summary](results.md#markdown-summary) with `--markdown-max-rows` above zero;
- the proposals `veridelta crosswalk` prints;
- what `read_discrepancies` and `propose_value_maps` return, on an MCP server started with `--allow-row-values`.

A pair compared in place, such as two warehouse tables, brings back counts and primary keys only, unless it fetches a [row sample](pushdown.md#row-samples).

## Writing a configuration

`veridelta schema` prints the JSON Schema of the configuration file. Check a draft against it, then run `veridelta validate`.

## Checking the output

`veridelta schema run` prints the JSON Schema of what `veridelta run --json` prints, and `validate`, `crosswalk`, `suggest`, and `error` name the others. The docs site serves the same files; see [Printing the schema](cli.md#printing-the-schema). A script or an agent can check what it parses against them.

On a pull request, the comment the [GitHub Action](ci.md#github-actions) keeps ends with the run's summary as JSON, in an HTML comment that readers never see. It holds counts and the column names the comment shows, never a value, and [Markdown summary](results.md#markdown-summary) shows how to parse it.

## Fixing drift in a loop

An agent that changes a pipeline can check its own work against the data the pipeline produced before, and keep fixing until the two match:

1. Change the pipeline. Push the change to the pull request, or run the pipeline locally.
2. Let the comparison run: the [GitHub Action](ci.md#github-actions) on the pull request, or `veridelta run --json` locally.
3. Read the result and its exit code. On the pull request, parse the JSON at the end of the Action's comment, as [Markdown summary](results.md#markdown-summary) shows. Locally, read what `run --json` prints. Both hold `added_count`, `removed_count`, and `changed_count`. `column_mismatches` holds every drifting column in `run --json`, and in the comment the ones its table lists.
4. Find the cause in the change, not in the data. The columns that drift, and how many rows each one changes, point to the code that writes them. With the MCP server started with `--allow-row-values`, `read_discrepancies` returns the rows that differ.
5. Fix the change, and go back to step 1.

Stop when the run matches, and report the counts. Stop too when the drift that remains is what the user asked for, such as a new rounding, and say which columns it is in. Never add a rule, or raise `threshold`, to make a run pass unless the user agrees: a rule changes what counts as a match, for every later run too.

## Docs for language models

The site publishes two plain-text files for language models, as the [llms.txt proposal](https://llmstxt.org/) describes:

- [`llms.txt`](https://veridelta.github.io/veridelta/llms.txt) links every page, each with its opening sentence.
- [`llms-full.txt`](https://veridelta.github.io/veridelta/llms-full.txt) holds every page of prose in one file, this one included.

## Agent skill

The steps above are also an agent skill, in [`skills/veridelta/SKILL.md`](https://github.com/Veridelta/veridelta/blob/main/skills/veridelta/SKILL.md). An agent that follows the Agent Skills standard loads it from a skills folder in the project. This installs it where Claude Code reads skills; for another agent, use the folder its documentation names:

```bash
mkdir -p .claude/skills/veridelta
curl -fsSL -o .claude/skills/veridelta/SKILL.md https://raw.githubusercontent.com/Veridelta/veridelta/main/skills/veridelta/SKILL.md
```

To pin the skill to a release, put the release's tag in place of `main`.
