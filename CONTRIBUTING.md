# Contributing to Veridelta

Contributions are welcome. Every change passes the same typing, formatting, and test checks that CI runs.

Everyone who takes part in the project follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## Development environment

Veridelta uses [uv](https://docs.astral.sh/uv/) for its environment and dependencies. A native setup is recommended. A Dev Container gives an isolated one.

### Native setup

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/).
2. Clone the repository and open its folder.
3. Install the dependencies and the Git hooks:

   ```bash
   make install
   ```

`make install` creates the virtual environment and installs the `pre-commit` hooks, which check formatting, license headers, and commit messages.

### Dev Container

1. Install Docker and VS Code.
2. Clone the repository.
3. Open the folder in VS Code and choose **Reopen in Container**. The container sets up the environment, the dependencies, and the Git hooks.

## Development workflow

Work on a short-lived branch, and never commit to `main` directly:

1. Create a branch, such as `git checkout -b feat/your-feature-name`.
2. Write the code and its tests.
3. Run the checks CI runs:

   ```bash
   make all  # formatting, linting, strict type checks, tests, and a strict docs build
   ```

The tests run each tutorial notebook in `docs/examples/` and require every `# Output:` comment to match what its cell prints. After editing a tutorial, `make notebooks` runs only those. After changing a configuration model, run `make schema` to regenerate `docs/schema/veridelta.schema.json`, the JSON Schema editors read. A test fails while it is stale.

### Pushdown parity

Pushdown must reach the same verdict as a local run. A differential harness runs both engines over the same frames and compares the results. It writes each case to a DuckDB file and runs the compiled SQL through `DuckDBPushdownSession`, the session DuckDB pushdown ships. That catches semantic errors such as NULL propagation, three-valued logic, and operator precedence, but not differences between vendors. Snowflake, Databricks, and BigQuery spellings are pinned by assertions on the emitted SQL. DuckDB's `levenshtein` counts bytes, not characters, so DuckDB pushdown refuses edit distance. The edit distance tests restore it on ASCII text, where the two agree, to check the predicate the other warehouses run. A property test also draws random configurations and data, from integers at the edges of their types to NULLs, NaN, and text timestamps, and requires both engines to reach the same counts on each. It never draws a rule the backend under test refuses.

`make all` runs the parity suite in DuckDB. After changing the SQL compiler, also run it inside a live Postgres with `make postgres`. It loads each case into the server that `VERIDELTA_POSTGRES_URI` names, compares the tables there and locally, and drops them. A disposable server works:

```bash
docker run --rm -d --name veridelta-postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
export VERIDELTA_POSTGRES_URI=postgresql://postgres:postgres@localhost:5432/postgres
make postgres
```

Cases that Postgres pushdown refuses, or whose data Postgres cannot store, carry the `duckdb_only` marker with the reason, and `make postgres` leaves them out. CI runs the suite against a `postgres:16` service on every pull request.

## Commit messages

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/), and the `commit-msg` hook rejects any other form:

- `feat:` a new feature.
- `fix:` a bug fix.
- `docs:` documentation.
- `test:` tests.
- `chore:` tooling or CI.
- `refactor:` a change that neither fixes a bug nor adds a feature.

The hooks also format the code and add the Apache-2.0 license header. If a hook changes a file or fails, stage the changes and run `git commit` again.

## Pull requests

1. Run `make all` before opening a pull request.
2. Open it against `main`, with a title in the Conventional Commits form, such as `feat: read Avro files`.
3. CI tests the pull request on several operating systems and Python versions. A pull request is merged only after every check passes.
4. CI runs on every pull request, whatever its base branch, so a pull request stacked on another one is checked too. Merge the base pull request first and delete its branch: GitHub then retargets the stacked one to `main`. Merging a stacked pull request while its base branch still exists lands it on that branch instead of `main`.
5. Workflows and `action.yml` run third-party actions pinned to a commit, with the release in a comment (`actions/checkout@<sha> # v5.1.0`). A moved tag upstream then cannot change what CI runs or what a release publishes. Pin any action you add the same way; `tests/unit/test_ci_integrations.py` checks it. Dependabot proposes newer pins and a refreshed `uv.lock` once a week, never higher floors in `pyproject.toml`.

## Releasing

A release is a pull request that changes the version. Merging it does the rest.

1. On a branch from `main`, run `uv run cz bump --version-files-only --yes`. It writes the new version into `pyproject.toml`, `src/veridelta/__init__.py`, `mkdocs.yml`, and the GitLab template, and prepends a generated block to `CHANGELOG.md`. Run `uv lock` to sync the lockfile, rewrite the generated block in the style of the earlier entries, and open a pull request titled `chore(release): X.Y.Z`.
2. Merge it once CI passes. The release workflow sees a version PyPI does not have, tags the merge commit `vX.Y.Z`, and starts a publishing run on that tag.
3. A maintainer approves that run's `pypi` deployment. It then builds the tagged commit, uploads the package, and creates the GitHub Release with generated notes, marked Latest when it is the newest version.

A merge that lands while a release waits for approval leaves the tag where it is, so the tagged commit is still the one released, and that merge's run ends without asking for a second approval. If the release's run was rejected or cancelled, the next merge to `main` starts it again; to abandon a version instead, release the next one.

If a release stops partway, re-run the failed jobs, or run the workflow by hand on the tag. A rerun uploads only the files PyPI lacks, so it never fails on a version PyPI already has, and it leaves an existing release page as it is. Pushing a version tag by hand still publishes, but the workflow refuses a tag whose name differs from the version in that commit.

## Writing documentation

The [Polars documentation](https://docs.pola.rs/) is the model: short declarative sentences, one topic per page, and examples that run. These rules apply to the docs, the README, the tutorials, docstrings, CLI help, and commit and pull request text.

- Open a page by defining its subject in one sentence. Introduce each code block with a sentence that ends in a colon.
- Use sentence case for headings, with code names in backticks.
- Describe behavior in the present tense and the active voice: "returns", "is compared", never "will".
- Write no em dashes, en dashes, or double hyphens as dashes. Use a colon or a period, and "to" for a range.
- Describe behavior; do not praise it. Leave out filler such as `simply`, `just`, and `note that`, and marketing such as `powerful` or `seamless`.
- Keep sentences to about 25 words, with one idea each. State a limitation plainly, next to the feature it limits.
- Keep test methodology and change history out of reference pages.
- Write docstrings in Google style: a one-line imperative summary that ends in a period, then `Args`, `Returns`, `Raises`, and `Examples` as needed.

`tests/unit/test_docs_style.py` checks the dash, wording, and list rules across the docs, the README, the tutorials, docstrings, CLI help, and the CI templates.
