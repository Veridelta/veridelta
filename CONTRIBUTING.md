# Contributing to Veridelta

We welcome contributions to Veridelta. To maintain an enterprise-grade standard, all code must adhere to strict architectural, typing, and formatting guidelines. 

## 1. Development Environment

We rely on [uv](https://docs.astral.sh/uv/) for deterministic, high-performance environment management. You do not need Docker to contribute; native execution is the recommended path.

### Option A: Native Setup (Recommended)
Provides native execution performance across macOS, Linux, and Windows.

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/) following the official instructions for your operating system.
2. Clone the repository and navigate into it.
3. Install dependencies and arm the local Git hooks using the Makefile:
   ```bash
   make install
   ```
*(Note: `make install` automatically provisions the virtual environment and installs the `pre-commit` hooks that enforce formatting, licensing, and commit conventions).*

### Option B: Dev Container (Optional)
For an isolated, containerized workflow, we provide a pre-configured Dev Container.

1. Ensure Docker and VS Code are installed.
2. Clone the repository.
3. Open the folder in VS Code and select **"Reopen in Container"** when prompted. The environment, dependencies, and Git hooks will configure automatically.

## 2. Development Workflow

We strictly follow Trunk-Based Development. **Never commit directly to `main`.**

1. Create a feature branch: `git checkout -b feat/your-feature-name`
2. Write your code and tests.
3. Verify your changes locally using the Makefile:
   ```bash
   make all  # Runs formatting, linting, strict type-checking, and tests
   ```
   The tests include the tutorial notebooks in `docs/examples/`: each one is executed, and every `# Output:` comment must match what its cell prints. Run `make notebooks` to check just those after editing a tutorial. After changing a configuration model, run `make schema` to regenerate the JSON Schema that editors read, `docs/schema/veridelta.schema.json`; a test fails while it is stale.

   `make all` checks that pushdown SQL reaches the local engine's verdicts by running it in DuckDB. After changing the SQL compiler, also run the same parity suite inside a live Postgres with `make postgres`. It loads each case into the server named by `VERIDELTA_POSTGRES_URI`, compares the tables there and locally, and drops them. A disposable server works:
   ```bash
   docker run --rm -d --name veridelta-postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
   export VERIDELTA_POSTGRES_URI=postgresql://postgres:postgres@localhost:5432/postgres
   make postgres
   ```
   Cases that Postgres pushdown refuses, or whose data Postgres cannot store, carry the `duckdb_only` marker with the reason, and `make postgres` leaves them out. CI runs the suite against a `postgres:16` service on every pull request.

## 3. Commit Standards

We strickly enforce [Conventional Commits](https://www.conventionalcommits.org/). Our `commit-msg` hook will automatically reject any commit that does not follow this structure:
* `feat:` A new feature.
* `fix:` A bug fix.
* `docs:` Documentation changes.
* `test:` Adding or updating tests.
* `chore:` Tooling or CI updates.
* `refactor:` Code changes that neither fix a bug nor add a feature.

*Note: During the commit phase, our hooks will also format your code and inject the required Apache-2.0 license headers. If a hook modifies a file or fails, simply stage the updated changes and run `git commit` again.*

## 4. Pull Requests

1. Ensure `make all` passes locally.
2. Open a PR against the `main` branch. Ensure your PR title also follows the Conventional Commits format (e.g., `feat: added semantic parser`).
3. **The CI Pipeline is the final gatekeeper.** It will automatically test your PR across multiple operating systems and Python versions. If the static analysis or test matrix fails, the PR cannot be merged.
4. CI runs on every pull request, whatever its base branch, so a PR stacked on another one is checked too. Merge the base PR first and delete its branch: GitHub then retargets the stacked PR to `main`. Merging a stacked PR while its base branch still exists lands it on that branch instead of `main`.
5. Workflows and `action.yml` run third-party actions pinned to a commit, with the release in a comment (`actions/checkout@<sha> # v5.1.0`), so a moved tag upstream cannot change what CI runs or what a release publishes. Pin any action you add the same way; `tests/unit/test_ci_integrations.py` checks it. Dependabot proposes newer pins and a refreshed `uv.lock` once a week, never higher floors in `pyproject.toml`.

## 5. Releasing

A release is a pull request that changes the version. Merging it does the rest.

1. On a branch from `main`, run `uv run cz bump --version-files-only --yes`. It writes the new version into `pyproject.toml`, `src/veridelta/__init__.py`, `mkdocs.yml`, and the GitLab template, and prepends a generated block to `CHANGELOG.md`. Run `uv lock` to sync the lockfile, rewrite the generated block in the style of the earlier entries, and open a pull request titled `chore(release): X.Y.Z`.
2. Merge it once CI passes. The release workflow sees a version PyPI does not have, tags the merge commit `vX.Y.Z`, and starts a publishing run on that tag.
3. A maintainer approves that run's `pypi` deployment. It then builds the tagged commit, uploads the package, and creates the GitHub Release with generated notes, marked Latest when it is the newest version.

A merge that lands while a release waits for approval leaves the tag where it is, so the tagged commit is still the one released, and that merge's run ends without asking for a second approval. If the release's run was rejected or cancelled, the next merge to `main` starts it again; to abandon a version instead, release the next one.

If a release stops partway, re-run the failed jobs, or run the workflow by hand on the tag. A rerun uploads only the files PyPI lacks, so it never fails on a version PyPI already has, and it leaves an existing release page as it is. Pushing a version tag by hand still publishes, but the workflow refuses a tag whose name differs from the version in that commit.