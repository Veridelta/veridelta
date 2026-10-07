---
render_macros: true
---

# Roadmap

This page lists work that is not built yet. The current release is v{{ config.extra.version }}. Each item names the use case it serves, with a link to its card in [who Veridelta serves](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md), or says that none asks for it yet. A test holds every item to that.

## Warehouses

- Pushdown for more SQL dialects, such as Redshift and Synapse. Serves [UC-05](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-05-compare-two-tables-where-they-are-stored).

## Editor

These steps build on [Editor support](configuration.md#editor-support), one at a time, tracked in [issue 127](https://github.com/Veridelta/veridelta/issues/127):

- A VS Code extension, in its own repository, whose first command runs `veridelta validate --json` on the open file and fills the Problems panel. Serves [UC-01](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-01-a-first-verdict-on-two-files).
- A second command that runs the comparison, opens the HTML report in the editor, and opens the discrepancy files. Serves [UC-01](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-01-a-first-verdict-on-two-files) and [UC-03](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-03-sign-off-from-the-report-alone).
- Registration of `veridelta mcp` with the editor's agent mode. Serves [UC-04](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-04-let-an-agent-run-the-comparison).

## AI workflows

These items build on [AI agents](agents.md), in this order, tracked in [issue 128](https://github.com/Veridelta/veridelta/issues/128). A model never decides a verdict: it drives the command line, explains a run, or proposes a rule with its evidence, and only a rule the user declares changes what matches.

### Next

- Schemas for the JSON that `run`, `validate`, and `crosswalk` print, published as the configuration schema is. Serves [UC-04](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-04-let-an-agent-run-the-comparison).
- A tutorial that compares two model evaluation runs, keyed by example ID, with tolerances and fuzzy text matching. Serves [UC-01](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-01-a-first-verdict-on-two-files).
- A section of the GitHub Action's pull request comment that an agent can parse. Serves [UC-02](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-02-the-same-verdict-on-every-pull-request) and [UC-04](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-04-let-an-agent-run-the-comparison).
- A recipe for an agent's fix loop: edit the pipeline, let CI run Veridelta, read the result, and fix what it reports. Serves [UC-04](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-04-let-an-agent-run-the-comparison).

### Later

- `veridelta suggest`: a command that proposes rules from the pairs that differ. A rule can be a tolerance, trimming, case folding, a null sentinel, or a date format. Each proposal shows its evidence, as a crosswalk does. No model is called. Serves [UC-01](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-01-a-first-verdict-on-two-files).
- Accepted drift: `run --baseline accepted.json` fails only on drift that the baseline does not list. An agent that changes a pipeline on purpose can show that nothing else moved. Serves [UC-02](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-02-the-same-verdict-on-every-pull-request).

### Ambitious

These items may not come soon, and some may never ship. They are listed so the direction is written down. Each is opt-in, and none changes a verdict except through a rule the user declares.

- A rule that compares text by meaning, through embeddings from a provider the user chooses. Paraphrased text and model outputs match when their meaning matches. No use case yet: nobody has asked for matching by meaning, and the item stays as direction until someone does.
- A summary of a run written by a language model, with the user's own key or a local model. It sends totals only, unless the user allows row values, and is tested from recorded exchanges. The MCP server lets an agent explain a run, so this item may stay unbuilt. No use case yet: the reviewer reads the report, and nobody has asked for prose.
