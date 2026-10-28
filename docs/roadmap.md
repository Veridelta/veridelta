---
render_macros: true
---

# Roadmap

This page lists work that is not built yet. The current release is v{{ config.extra.version }}. Each item names the use case it serves, with a link to its card in [who Veridelta serves](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md), or says that none asks for it yet. A test holds every item to that.

## Reviewing a run

These items help a reviewer sign off on a run from its report.

- A faster [`count_forgiven_values`](configuration.md#counting-forgiven-values) for rules that rewrite values. A tolerance now costs 1 to 2% of a run, but case-insensitive rules on ten text columns still cost 25%, since each carries a hash of every original value through the join. Serves [UC-03](https://github.com/Veridelta/veridelta/blob/main/product/USERS.md#uc-03-sign-off-from-the-report-alone).

## AI workflows

These items build on [AI agents](agents.md), in this order, tracked in [issue 128](https://github.com/Veridelta/veridelta/issues/128). A model never decides a verdict: it drives the command line, explains a run, or proposes a rule with its evidence, and only a rule the user declares changes what matches.

### Ambitious

These items may not come soon, and some may never ship. They are listed so the direction is written down. Each is opt-in.

- A rule that compares text by meaning, through embeddings from a provider the user chooses. Paraphrased text and model outputs match when their meaning matches. No use case yet: nobody has asked for matching by meaning, and the item stays as direction until someone does.
- A summary of a run written by a language model, with the user's own key or a local model. It sends totals only, unless the user allows row values, and is tested from recorded exchanges. The MCP server lets an agent explain a run, so this item may stay unbuilt. No use case yet: the reviewer reads the report, and nobody has asked for prose.
