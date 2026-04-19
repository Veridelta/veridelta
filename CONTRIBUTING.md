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