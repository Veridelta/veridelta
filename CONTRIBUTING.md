# Contributing to Veridelta

We welcome contributions to Veridelta. To maintain an enterprise-grade standard, all code must adhere to strict architectural and formatting guidelines.

## 1. Development Environment

We mandate isolated, reproducible environments. You can contribute via our DevContainer (Zero Setup) or Local Installation.

### Option A: DevContainer (Recommended)
1. Ensure Docker and VS Code are installed.
2. Clone the repository: `git clone https://github.com/Veridelta/veridelta.git`
3. Open the folder in VS Code and select **"Reopen in Container"** when prompted. The environment, dependencies, and extensions will configure automatically.

### Option B: Local Setup (uv)
1. Install [uv](https://docs.astral.sh/uv/): `curl -LsSf https://astral.sh/uv/install.sh | sh`
2. Clone the repository and navigate into it.
3. Install dependencies and activate the environment:
   ```bash
   uv sync --all-extras
   source .venv/bin/activate
   ```
4. Install the pre-commit hooks: `uv run pre-commit install`

## 2. Development Workflow

We strictly follow Trunk-Based Development. **Never commit directly to `main`.**

1. Create a feature branch: `git checkout -b feat/your-feature-name`
2. Write your code and tests.
3. Verify your changes using the Makefile:
   ```bash
   make all  # Runs formatting, linting, type-checking, and tests
   ```

## 3. Commit Standards

We enforce [Conventional Commits](https://www.conventionalcommits.org/). Your commit messages must follow this structure:
* `feat:` A new feature.
* `fix:` A bug fix.
* `docs:` Documentation changes.
* `chore:` Tooling or CI updates.
* `test:` Adding or updating tests.
* `refactor:` Code changes that neither fix a bug nor add a feature.

## 4. Pull Requests

1. Ensure `make all` passes locally.
2. Open a PR against the `main` branch.
3. The CI pipeline will automatically test your PR across multiple operating systems and Python versions. If the CI fails, the PR cannot be merged.