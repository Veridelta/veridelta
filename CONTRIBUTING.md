# Contributing to Veridelta

Contributions are welcome. Every change passes the same typing, formatting, and test checks that CI runs.

Everyone who takes part in the project follows the [Code of Conduct](CODE_OF_CONDUCT.md). A change to the HTML report or the documentation site meets the [accessibility expectations](ACCESSIBILITY.md#contributor-expectations).

Coding agents read [AGENTS.md](AGENTS.md), which sums up this guide, and the rule under `rules/` for each path they change. Update them when a rule changes. Claude Code reads `AGENTS.md` from 2.1.277, and from 2.1.282 on Bedrock, Vertex, Foundry, a gateway, or with telemetry off. It does so only while no `CLAUDE.md` or `CLAUDE.local.md` exists in the working directory or above it, so `tests/unit/test_docs_links.py` refuses either file anywhere in the repository. Personal notes go in `~/.claude/CLAUDE.md`, which does not count.

## Development environment

Veridelta uses [uv](https://docs.astral.sh/uv/) for its environment and dependencies. A native setup is recommended. A Dev Container gives an isolated one.

### Native setup

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/).
2. Clone the repository and open its folder.
3. Install the dependencies and the Git hooks:

   ```bash
   make install
   ```

`make install` creates the virtual environment and installs the `pre-commit` hooks, which check formatting, license headers, and commit messages. The hooks run ruff, mypy, and commitizen from that environment, so they use the versions `uv.lock` pins, as `make lint` does.

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

Cases that Postgres pushdown refuses, or whose data Postgres cannot store, carry a `skip_on("postgres")` marker with the reason, and the suite skips them there. A case that pins one backend's own behavior carries `only_on` instead. CI runs the suite against a `postgres:16` service on every pull request.

### Database servers

`make databases` reads real MySQL and SQL Server tables through the `database` connector. Each test loads its rows with the server's own driver, from the `databases` dependency group, then checks what Veridelta reads back. Set `VERIDELTA_MYSQL_URI`, `VERIDELTA_MSSQL_URI`, or both, to servers the tests may create tables on. Disposable servers work:

```bash
docker run --rm -d --name veridelta-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=veridelta -e MYSQL_DATABASE=veridelta mysql:8.4
docker run --rm -d --name veridelta-mssql -p 1433:1433 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Veridelta-2026 mcr.microsoft.com/mssql/server:2022-latest
export VERIDELTA_MYSQL_URI=mysql://root:veridelta@127.0.0.1:3306/veridelta
export VERIDELTA_MSSQL_URI='mssql://sa:Veridelta-2026@127.0.0.1:1433/master?encrypt=true&trust_server_certificate=true'
make databases
```

A server whose variable is not set is skipped. CI runs the tests against a `mysql:8.4` service and a SQL Server 2022 service on every pull request. Both passwords there hold `@`, `:`, `/`, and `#`, so each read checks that a password reaches the driver intact.

### Accessibility

`make accessibility` builds the docs site and a sample HTML report, then checks both in Chromium:

- axe-core runs on every page, in light and dark mode, at desktop and phone widths. Any violation of WCAG 2.2 A or AA, or of axe-core's best practices, fails the check.
- A keyboard pages the report, and the report is read with JavaScript off.

The tests drive the browser with Python's Playwright, from the `accessibility` dependency group. Install its Chromium once, adding `--with-deps` on Linux for the system libraries:

```bash
uv run --group accessibility playwright install chromium
make accessibility
```

The browser fetches only the local site, so a font or a diagram from a CDN never changes a result. CI runs the same checks on every pull request.

axe-core comes from npm, pinned by version and digest, and Dependabot cannot bump it. To move to a new release, read `https://registry.npmjs.org/axe-core/<version>`. Copy its `dist.tarball` into `_AXE_CORE` and its `dist.integrity` into `_AXE_CORE_INTEGRITY`, both in `tests/accessibility/test_accessibility.py`.

### The recording

The README and the docs home embed a recording of the quick start, rendered from `demo/veridelta.tape` by [vhs](https://github.com/charmbracelet/vhs). `make demo` writes `docs/assets/demo.gif`; run it after a change to the quick start or to the run summary, with vhs installed from `brew install vhs` or its release binary. The tape types six commands at real speed on the two CI fixture files copied into `demo/`. `demo/transcript.txt` holds what the recording shows, written by hand, and `tests/unit/test_demo_tape.py` holds the commands to the CLI, the data to the fixtures, and the transcript to what the commands print. Commit the new GIF, with the transcript that matches it, in the change that moved the output.

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
3. CI tests the pull request on several operating systems and Python versions. A pull request is merged only after every check passes. The `CI Passed` check sums them up: it fails when any other job fails, is cancelled, or is skipped, so it is the one check for a ruleset on `main` to require. Each job stops after 15 minutes, so a hung job fails instead of holding a runner.
4. GitHub sometimes never starts a queued job, and cancels it after 15 minutes. The `Re-run Dropped CI Jobs` workflow then re-runs the failed jobs, at most three times, but only when no failed job ever started. A job that fails after it starts is never re-run: fix the failure instead.
5. CI runs on every pull request, whatever its base branch, so a pull request stacked on another one is checked too. Merge the base pull request first and delete its branch: GitHub then retargets the stacked one to `main`. Merging a stacked pull request while its base branch still exists lands it on that branch instead of `main`.
6. Workflows and `action.yml` run third-party actions pinned to a commit, with the release in a comment (`actions/checkout@<sha> # v<release>`). A moved tag upstream then cannot change what CI runs or what a release publishes. Pin any action you add the same way; `tests/unit/test_ci_integrations.py` checks it. Dependabot proposes newer pins and a refreshed `uv.lock` once a week, never higher floors in `pyproject.toml`.
7. CI installs what `uv.lock` pins and nothing else. Every `uv sync` in a workflow passes `--locked`, so a lockfile that no longer matches `pyproject.toml` fails the job instead of resolving again. The CI, docs, and live workflows also run one uv release, set by `version` in each `setup-uv` step, since Dependabot does not move it. To take a newer uv, change every one of those pins together; `tests/unit/test_ci_integrations.py` checks that they agree.
8. Every container image is pinned by digest, with its tag beside it: the service containers in `ci.yml`, and the Dev Container's base image and uv. Dependabot proposes new digests for the Dev Container once a week. It does not read a workflow's services, so to take a newer service image, replace its digest with the one `docker buildx imagetools inspect <image>:<tag>` prints.

## Live warehouse tests

The parity suite also runs inside the services pushdown supports: BigQuery, Databricks, MotherDuck, and Snowflake. Each case loads its frames into fresh tables, compares them there and locally, and drops them. A run first drops the tables a failed run left behind, once they are a day old.

The `Live Warehouses` workflow runs the suite. It starts only by hand, from the Actions tab with **Run workflow**, against one service or all of them. Each job then waits for a maintainer to approve its `live` deployment. GitHub offers **Run workflow** only for a workflow on the default branch.

Each service needs an account, a repository variable set to `true` that turns its job on, and secrets in the `live` environment. A service whose variable is not `true` shows as skipped.

| Service | Repository variable | Secrets in `live` |
| :--- | :--- | :--- |
| BigQuery | `LIVE_BIGQUERY` | `BQ_PROJECT`, `BQ_CREDENTIALS_JSON` |
| Databricks | `LIVE_DATABRICKS` | `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` |
| MotherDuck | `LIVE_MOTHERDUCK` | `MOTHERDUCK_TOKEN` |
| Snowflake | `LIVE_SNOWFLAKE` | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE` |

Create the `live` environment under Settings, then Environments, with a maintainer as its required reviewer, and add the secrets there. Add the variables under Settings, then Secrets and variables, then Actions, as repository variables, which the job conditions read.

- **BigQuery.** The free sandbox works and needs no card. Create a project and a dataset named `veridelta_live` in it. Then create a service account with the BigQuery Job User and BigQuery Data Editor roles, and a JSON key for it. `BQ_PROJECT` is the project ID, and `BQ_CREDENTIALS_JSON` holds the key file. Each query may bill 1 GB at most.
- **Databricks.** The Free Edition works. Create the schema `veridelta_live` in the `workspace` catalog. The SQL warehouse's Connection details tab gives `DATABRICKS_HOST`, the server hostname, and `DATABRICKS_HTTP_PATH`. `DATABRICKS_TOKEN` is a personal access token.
- **MotherDuck.** The free plan works. Create the database `veridelta_live`, and an access token for `MOTHERDUCK_TOKEN`.
- **Snowflake.** A 30-day trial works. Create it last, since the trial starts at sign-up. Create an X-Small warehouse that suspends after 60 seconds, with a resource monitor that caps its credits. Then create a database, and a service user with its own role and a key pair. The tables go in the database's `PUBLIC` schema, so the role needs `USAGE` on the warehouse, the database, and the schema, and `CREATE TABLE` on the schema. `SNOWFLAKE_PRIVATE_KEY` holds the private key in PEM form, and an encrypted key also needs `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`.

To run one service from a terminal, set `VERIDELTA_PARITY_BACKEND` and the service's settings, then run `make live`. There, BigQuery reads a key file from `BQ_CREDENTIALS_PATH`, or the default credentials without one, and Snowflake reads its key from `SNOWFLAKE_PRIVATE_KEY_PATH`. `MOTHERDUCK_DATABASE` can name a DuckDB file instead of `md:veridelta_live`, which runs the MotherDuck backend without an account:

```bash
VERIDELTA_PARITY_BACKEND=motherduck MOTHERDUCK_DATABASE=/tmp/live.duckdb make live
```

A case that a service refuses, or whose data it cannot store, carries a `skip_on` marker with the reason, as for Postgres.

## Releasing

A release is a pull request that changes the version. Merging it does the rest.

1. Run the `Live Warehouses` workflow on `main` against all services, and wait until each configured one passes. A difference it finds is fixed, or documented in `docs/pushdown.md` with a test that pins it, first. No service is configured yet: until [issue 111](https://github.com/Veridelta/veridelta/issues/111) sets them up, a release ships without this run, by the maintainer's decision of 2026-10-06, and its pull request says so.
2. On a branch from `main`, check the version `uv run cz bump --dry-run` proposes, as described below. Then run `uv run cz bump --version-files-only --yes`. It writes the new version into every file that `version_files` lists in `pyproject.toml`: the package, `mkdocs.yml`, the GitLab template, and each pin in the docs, the Action, and the bug report form. `tests/unit/test_version_pins.py` holds those pins to the package's version between releases, so a pin the list misses fails the suite. The bump also prepends a generated block to `CHANGELOG.md`. Run `uv lock` to sync the lockfile, rewrite the generated block in the style of the earlier entries, and open a pull request titled `chore(release): X.Y.Z`.
3. Merge it once CI passes. The release workflow sees a version PyPI does not have, tags the merge commit `vX.Y.Z`, and starts a publishing run on that tag.
4. A maintainer approves that run's `pypi` deployment. It then builds the tagged commit, uploads the package, and creates the GitHub Release with generated notes, marked Latest when it is the newest version.

Commitizen picks the version from every line of every commit message since the last tag, not only the titles. A squash merge's body lists the commits it squashed, and a Dependabot body quotes upstream release notes. So a stray `feat` line can propose a minor version where a patch is due. When the dry run proposes the wrong version, pass `--increment PATCH` or `--increment MINOR` to both commands.

A merge that lands while a release waits for approval leaves the tag where it is, so the tagged commit is still the one released, and that merge's run ends without asking for a second approval. If the release's run was rejected or cancelled, run the workflow by hand on the tag to start it again; a later merge to `main` does not. To abandon a version instead, release the next one.

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
- Describe behavior that exists in the repository. The roadmap lists only work that is not built, and when a feature ships, its how-to moves to the guide page and the roadmap item goes.
- Keep every example valid against the current configuration and public API, and name the optional extra a feature needs where users meet it.
- Name each link's target in its text, never "here". Tutorials and the README link to the site by absolute URL, which `tests/unit/test_docs_links.py` resolves.
- Treat runtime output, such as the run summary and the error headers, as an interface that scripts parse. Change it on purpose, never as a style edit.
- When a field joins `DiffConfig`, `DiffRule`, `SourceConfig`, or a connection model, describe it on the model's guide page, which `tests/unit/test_docs_coverage.py` checks, and run `make schema`.

`tests/unit/test_docs_style.py` checks the dash, wording, and list rules across the docs, the README, the tutorials, docstrings, CLI help, and the CI templates.

### Where each topic lives

Link to the page that owns a topic instead of repeating it:

- Install, the extras, and the quick start: `README.md`.
- The configuration file, its settings, environment variables, and editor support: `docs/configuration.md`.
- Files, lakehouse tables, databases, warehouses, their extras, connection fields, and logging: `docs/sources.md`.
- Rule fields, the transform order, and each transform: `docs/rules.md`.
- Comparing inside a warehouse, Postgres, or DuckDB, and how that differs from a local run: `docs/pushdown.md`.
- `DiffResult`, reports, OpenTelemetry metrics, and artifacts: `docs/results.md`.
- Commands, flags, exit codes, and `validate`: `docs/cli.md`.
- The GitHub Action and the GitLab template: `docs/ci.md`, where macros are off so `${{ }}` renders as written.
- How an agent runs Veridelta: `docs/agents.md`. The build writes `llms.txt` and `llms-full.txt` from every page through `hooks/llms_txt.py`.
- The public Python surface: `docs/api.md`, generated from docstrings and never hand-copied.
- Work that is not built: `docs/roadmap.md`.
- Tutorials: the notebooks under `docs/examples/`.
