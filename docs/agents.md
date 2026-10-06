# AI agents

An AI agent, such as a coding assistant, runs Veridelta through its command line. This page gives the steps that keep its runs predictable and its replies free of row values.

## Steps

1. Check the configuration file. `validate` reads no rows, and by default connects to nothing:

    ```bash
    veridelta validate -c veridelta.yaml --json
    ```

    The JSON holds `valid`, `errors`, and `warnings`, and the command exits `1` when there is an error. Fix each error before a run. `--allow-missing-env` checks a file whose secrets are not set, such as in a pull request job.

2. Run the comparison with `--json`, so stdout carries only the [summary](results.md#summary):

    ```bash
    veridelta run -c veridelta.yaml --json
    ```

3. Read the run's [exit code](cli.md#exit-codes) before its output:

    | Code | Meaning | stdout |
    | :--- | :--- | :--- |
    | `0` | A match within `threshold`. | The summary. |
    | `1` | Drift, or a failure. | The summary after drift. Nothing after a failure, which stderr explains. |
    | `2` | Invalid arguments. | Nothing. |

4. Report counts and column names, which is all the summary holds. Leave row values out of a reply unless the user asks for them.

5. When two columns hold the same values in different encodings, such as `Y` and `true`, propose a `value_map` from the data:

    ```bash
    veridelta crosswalk -c veridelta.yaml --json
    ```

    Show the proposals and their evidence to the user before adding them to the configuration.

## Where row values appear

The summary holds counts and column names only. These outputs hold values from the data:

- the discrepancy files a run writes to `output_path`;
- the [HTML report](results.md#html-report);
- a [Markdown summary](results.md#markdown-summary) with `--markdown-max-rows` above zero;
- the proposals `veridelta crosswalk` prints.

A pair compared in place, such as two warehouse tables, brings back counts and primary keys only, unless it fetches a [row sample](pushdown.md#row-samples).

## Docs for language models

The site publishes two plain-text files for language models, as the [llms.txt proposal](https://llmstxt.org/) describes:

- [`llms.txt`](https://veridelta.github.io/veridelta/llms.txt) links every page, each with its opening sentence.
- [`llms-full.txt`](https://veridelta.github.io/veridelta/llms-full.txt) holds every page of prose in one file, this one included.

`veridelta schema` prints the JSON Schema of the configuration file. Check a draft against it, then run `veridelta validate`.

## Agent skill

The steps above are also an agent skill, in [`skills/veridelta/SKILL.md`](https://github.com/Veridelta/veridelta/blob/main/skills/veridelta/SKILL.md). An agent that reads skills, such as Claude Code, loads it from a skills folder. This installs it in a project:

```bash
mkdir -p .claude/skills/veridelta
curl -fsSL -o .claude/skills/veridelta/SKILL.md https://raw.githubusercontent.com/Veridelta/veridelta/main/skills/veridelta/SKILL.md
```

To pin the skill to a release, put the release's tag in place of `main`.
