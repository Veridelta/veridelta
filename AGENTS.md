# Agent instructions

This file holds the rules for coding agents that change this repository. It sums up [CONTRIBUTING.md](CONTRIBUTING.md), which people follow, and links the detailed rules in `.cursor/rules`.

## Commands

The project uses [uv](https://docs.astral.sh/uv/) and a Makefile:

| Command | What it does |
| :--- | :--- |
| `make install` | Creates the environment with every extra, and installs the Git hooks. |
| `make all` | Formats, lints, type-checks, tests, and builds the docs, as CI does. Run it before a pull request. |
| `make notebooks` | Runs the tutorials, after a change to one. |
| `make schema` | Regenerates `docs/schema/veridelta.schema.json`, after a change to a configuration model. |
| `make postgres` | Runs the parity suite inside a live Postgres, after a change to the SQL compiler. |
| `make live` | Runs the parity suite inside one live warehouse, with that service's account. A release needs it to pass. |
| `make databases` | Reads real MySQL and SQL Server tables, after a change to the database connector. |

Run Python tools through uv, such as `uv run pytest tests/unit`. Do not use `pip`, `poetry`, or `conda`.

## Rules for every change

- Work on a branch from `main`. Never commit to `main`.
- Write commit messages and pull request titles as [Conventional Commits](https://www.conventionalcommits.org/), such as `fix: read empty Avro files`. The `commit-msg` hook rejects any other form.
- Change behavior together with its tests. The suite fails on any warning, and the core modules keep 100% branch coverage.
- Leave `CHANGELOG.md` alone. The release pull request writes it.
- Add no dependency without a concrete need.
- Follow the [writing rules](CONTRIBUTING.md#writing-documentation) in docs, docstrings, CLI help, commit messages, and pull requests. `tests/unit/test_docs_style.py` checks some of them.

## Detailed rules

Read the rules for the files a change touches:

| Rules | Applies to |
| :--- | :--- |
| [Core architecture](.cursor/rules/000-core-architecture.mdc) | Every change |
| [Engine and models](.cursor/rules/100-engine-polars.mdc) | `src/veridelta/` |
| [Testing standards](.cursor/rules/200-testing-standards.mdc) | `tests/` |
| [Security](.cursor/rules/300-security.mdc) | `src/veridelta/`, above all the SQL in `connectors/sql.py` |
| [Documentation](.cursor/rules/400-docs.mdc) | `docs/`, `README.md`, and docstrings |

## Settled

Each of these choices has a record in `decisions/`, with what it was chosen over and how to reverse it. Do not reopen one without the maintainer:

- [AGENTS.md holds the rules for coding agents](decisions/agents-md-is-canonical.md)
- [The MySQL and SQL Server test drivers sit in their own dependency group](decisions/database-drivers-in-their-own-group.md)
- [llms.txt comes from a hook in the repository](decisions/llms-txt-from-a-hook.md)

Write a record with the `defend-decision` skill, in `.claude/skills/`, when a choice is non-obvious, likely to be questioned again, or made by an agent. A small choice needs none, and an old one is not backfilled.
