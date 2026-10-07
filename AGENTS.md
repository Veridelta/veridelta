# Agent instructions

This file holds the rules for coding agents that change this repository. Claude Code and Cursor read it directly, as does any tool that reads `AGENTS.md`. It sums up [CONTRIBUTING.md](CONTRIBUTING.md), which people follow, and names the rule to read before changing a file under each path.

## Commands

The project uses [uv](https://docs.astral.sh/uv/) and a Makefile:

| Command | What it does |
| :--- | :--- |
| `make install` | Creates the environment with every extra, and installs the Git hooks. |
| `make all` | Formats, lints, type-checks, tests, and builds the docs, as CI does. Run it before a pull request. |
| `make notebooks` | Runs the tutorials, after a change to one. |
| `make schema` | Regenerates the schemas under `docs/schema/`, after a change to a configuration model or to the JSON a command prints. |
| `make postgres` | Runs the parity suite inside a live Postgres, after a change to the SQL compiler. |
| `make live` | Runs the parity suite inside one live warehouse, with that service's account. A release needs it to pass. |
| `make databases` | Reads real MySQL and SQL Server tables, after a change to the database connector. |
| `make accessibility` | Checks the docs site and the HTML report with axe-core and a keyboard, in Chromium, after a change to either. |
| `make demo` | Renders every recording from its tape in `demo/` with vhs v0.12.1, after a change to a command a tape types or to what it prints. |
| `make screenshots` | Captures the HTML report and renders the link preview card under `docs/assets/`, after a change to the report or to the summary sentence. |

Run Python tools through uv, such as `uv run pytest tests/unit`. Do not use `pip`, `poetry`, or `conda`.

## Rules for every change

- Work on a branch from `main`. Never commit to `main`.
- Write commit messages and pull request titles as [Conventional Commits](https://www.conventionalcommits.org/), such as `fix: read empty Avro files`. The `commit-msg` hook rejects any other form.
- Change behavior together with its tests. The suite fails on any warning, and the core modules keep 100% branch coverage.
- Leave `CHANGELOG.md` alone. The release pull request writes it.
- Add no dependency without a concrete need.
- Make the smallest change that meets the need, with no abstraction for a case that does not exist yet.
- Keep a public API as it is unless the task asks for a breaking change.
- Never edit a generated file by hand. Run its generator, such as `make schema` for the schemas under `docs/schema/`.
- A `# type: ignore`, `# noqa`, or `# pyright: ignore` carries its reason on the same line.
- Assemble warehouse SQL only in `connectors/sql.py`, from allowlisted identifiers, dialect quoting, and strict types, and never concatenate a configuration string into a statement. [The security rules](rules/security.md) hold the whole control.
- Follow the [writing rules](CONTRIBUTING.md#writing-documentation) in docs, docstrings, CLI help, commit messages, and pull requests. `tests/unit/test_docs_style.py` checks some of them.
- Meet the [accessibility expectations](ACCESSIBILITY.md#contributor-expectations) in a change to the HTML report or the docs site.
- Add no `CLAUDE.md`, `.claude/CLAUDE.md`, or `CLAUDE.local.md`, even uncommitted: Claude Code reads this file only while none is here or above it, so the suite fails on any of them here. Personal notes go in your own instructions, outside the project.

## Rules by path

`rules/` holds the detailed rules as concepts of the knowledge bundle, one file per subject, each with the paths it governs in its frontmatter; [rules/index.md](rules/index.md) lists them. Read the rule in a row before you write or change a file under its paths:

| Rule | Applies to |
| :--- | :--- |
| [Engine](rules/engine.md): data manipulation, models and configuration, loaders | `src/veridelta/` |
| [Security](rules/security.md): warehouse SQL assembly, the execution boundary | `src/veridelta/connectors/`, `src/veridelta/models.py`, `src/veridelta/engine.py` |
| [Testing](rules/testing.md): layout, fixtures, markers, the parity suite | `tests/` |

`tests/unit/test_rules.py` fails on any of these:

- a rule missing from this table or from the index;
- a path that does not exist;
- a row that names other paths than the rule's frontmatter;
- an `AGENTS.md` anywhere below the root.

## Skills

`.claude/skills/` holds the procedures an agent runs in this repository: `define-personas`, `define-key-metrics`, `defend-decision`, and `record-demo`. The folder is the one the Agent Skills standard discovers in a project, so every tool that reads the standard finds them. Claude Code and Cursor, the two tools the repository supports, both read it, and it moves to `.agents/skills/` once Claude Code reads that folder. `skills/veridelta/` is different: it is the product's own skill, which users install into their agents as [the AI agents page](docs/agents.md#agent-skill) says, and nothing in this repository loads it.

## Product documents

`product/` holds what the project knows about its users and its goals, as an [Open Knowledge Format](https://github.com/GoogleCloudPlatform/open-knowledge-format) bundle: one concept per Markdown file. Each directory has an `index.md` that lists its concepts. `product/USERS.md` holds the personas and use cases. `product/KEY_METRICS.md` and `product/metrics/` hold the north star with its drivers and guardrails. `product/FEATURES.md` maps each feature and roadmap item to the use case it serves, and `product/log.md` says what changed and why.

- A change serves a use case, named in its pull request as `UC-nn`, or says it is maintenance. A roadmap item names its use case, or says that none asks for it yet.
- A concept opens with frontmatter: `type`, `title`, a one-line `description`, `status`, and `generated`. A decision record under `decisions/` and a rule under `rules/` carry `generated` too.
- `generated` names who wrote the text and when: `{ by: claude-code, at: 2026-10-06T09:45:00Z }`, or `human:<id>` for a person. Never a model.
- Ids tie the documents together: a persona is `P-01`, a use case `UC-01`, a metric `NS-01`, `DR-01`, or `GR-01`. A heading or a metric's file name defines an id once. Name an id only once it is defined, and outside a heading name it as a link to its card.
- Write the documents with the `define-personas` and `define-key-metrics` skills in `.claude/skills/`, add a line to `product/log.md` under today's date, and follow the writing rules.

`tests/unit/test_product_bundle.py` fails on any of these:

- a missing field, or a producer that names a model;
- an id defined nowhere or twice, or named without a link;
- a second north star, or a driver or guardrail that does not support it;
- a concept missing from its index;
- a link that leads nowhere or to no heading;
- a use case that no feature serves, or a roadmap item that names no use case.

## Settled

Each of these choices has a record in `decisions/`, with what it was chosen over and how to reverse it. Do not reopen one without the maintainer:

- [The accessibility check runs axe-core from Python, with a pinned download](decisions/accessibility-check-in-python.md)
- [Each rule is one concept of the bundle](decisions/one-text-for-each-rule.md), which replaces [The agent rules live in AGENTS.md files beside the code](decisions/rules-live-beside-the-code.md) and, before it, [AGENTS.md holds the rules for coding agents](decisions/agents-md-is-canonical.md)
- [A tool has a file of its own only where it reads no shared one](decisions/a-tool-file-only-where-the-tool-needs-it.md)
- [The MySQL and SQL Server test drivers sit in their own dependency group](decisions/database-drivers-in-their-own-group.md)
- [The GitLab CI template is frozen](decisions/gitlab-template-is-frozen.md)
- [llms.txt comes from a hook in the repository](decisions/llms-txt-from-a-hook.md)
- [The demos are recorded with vhs](decisions/recording-with-vhs.md)
- [Promotional videos live in their own repository](decisions/promotional-videos-in-their-own-repository.md)
- [The MCP server runs on the official SDK, from an extra, over stdio](decisions/mcp-server-on-the-official-sdk.md)

Write a record with the `defend-decision` skill, in `.claude/skills/`, when a choice is non-obvious, likely to be questioned again, or made by an agent. A small choice needs none, and an old one is not backfilled.
