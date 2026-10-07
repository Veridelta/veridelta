# Index

The rules a coding agent reads before it changes a file, one concept per file. Each names the paths it applies to in its frontmatter, and the "Rules by path" table in `AGENTS.md` says which to read for which path.

## Rule

- [Engine rules](engine.md): data manipulation, models and configuration, and loaders, under `src/veridelta/`.
- [Security rules](security.md): warehouse SQL assembly and the execution boundary, in `src/veridelta/connectors/`, `models.py`, and `engine.py`.
- [Testing rules](testing.md): the layout of `tests/`, fixtures, markers, the parity suite, and the coverage gate.
