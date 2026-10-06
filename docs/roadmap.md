---
render_macros: true
---

# Roadmap

This page lists work that is not built yet. The current release is v{{ config.extra.version }}.

## Warehouses

- Pushdown for more SQL dialects, such as Redshift and Synapse.

## Editor

- A VS Code extension that runs a comparison and shows its discrepancy files inside the editor. Completion and checking of configuration files already work through the published JSON Schema; see [Editor support](configuration.md#editor-support). `veridelta validate` checks a file before a run.

## AI workflows

An AI agent can drive Veridelta through the [command line](cli.md) today. `run`, `validate`, and `crosswalk` print JSON with `--json`, and every command returns an [exit code](cli.md#exit-codes). These items make that easier and safer, from small steps to ambitious ones.

### Next

- Errors as JSON. Under `--json`, a command that fails prints one JSON object on stdout, with the error's type and message. Its exit code also differs from the code for drift.
- `llms.txt` and `llms-full.txt` on this site: an index and a single-file copy of these pages, in plain text for language models.
- A page for AI agents. It gives the steps: validate first, run with `--json`, and read the exit code. It also tells an agent to keep row values out of its replies unless asked.
- An agent skill: a `SKILL.md` file that an agent harness installs, with the same guidance as that page.
- An `AGENTS.md` file at the repository root, so every contributor's coding agent reads the same rules. Only Cursor reads them today, from `.cursor/rules`.
- Schemas for the JSON that `run`, `validate`, and `crosswalk` print, published as the configuration schema is.
- A tutorial that compares two model evaluation runs, keyed by example ID, with tolerances and fuzzy text matching.
- A section of the GitHub Action's pull request comment that an agent can parse.
- A recipe for an agent's fix loop: edit the pipeline, let CI run Veridelta, read the result, and fix what it reports.

### Later

- `veridelta mcp`: a [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server, in a `veridelta[mcp]` extra. Its tools validate a configuration, run a comparison, read discrepancy rows up to a limit, propose value maps, and describe the schema.
- Guardrails for that server. It reads configuration files only from allowed folders and never returns credentials. Row values stay off unless enabled, and then come up to a cap.
- `veridelta suggest`: a command that proposes rules from the pairs that differ. A rule can be a tolerance, trimming, case folding, a null sentinel, or a date format. Each proposal shows its evidence, as a crosswalk does. No model is called.
- Accepted drift: `run --baseline accepted.json` fails only on drift that the baseline does not list. An agent that changes a pipeline on purpose can show that nothing else moved.

### Ambitious

These items may not come soon, and some may never ship. They are listed so the direction is written down.

- A second phase of the [VS Code extension](#editor) that registers `veridelta mcp` with the editor, so its agent mode can call Veridelta.
- A rule that compares text by meaning, through embeddings from a provider the user chooses. Paraphrased text and model outputs match when their meaning matches.
- A summary of a run written by a language model, opt-in, with the user's own key. It sends totals only, unless the user allows row values. The MCP server above lets an agent explain a run, so this item may stay unbuilt.
