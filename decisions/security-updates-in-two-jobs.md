---
type: Decision
title: A workflow fixes a vulnerable lockfile in two jobs
description: Why the daily Security Updates workflow runs uv lock in a job with no secret and opens the pull request from a job that runs nothing, rather than leaving security fixes to Dependabot.
status: stable
decided: 2026-11-14
generated: { by: claude-code, at: 2026-11-14T20:40:00+00:00 }
---

**Claim:** `.github/workflows/security-updates.yml` audits `uv.lock` on main every day and runs `uv lock --upgrade-package` with no version pinned for each vulnerable package, in a job with a read-only token and no secret. A second job opens or updates one pull request through the GitHub API with the `SECURITY_UPDATES_TOKEN` secret, and checks nothing out and installs nothing. CI's `lock_diff.py` step refuses any pull request's lockfile that moves a package to an older version.

**Evidence:** Dependabot's pull request 468 pinned soupsieve 4.0, which needs Python 3.11.5, so the lock fell back to beautifulsoup4 4.6.3 for Python 3.11.0 to 3.11.4. `uv lock --upgrade-package soupsieve --upgrade-package tornado`, with no pin, gave soupsieve 4.1.1 and kept beautifulsoup4 4.15.0 (pull request 470). `python3 .github/scripts/lock_diff.py` on main's lock and pull request 468's prints "beautifulsoup4 4.6.3 is older than 4.15.0" and exits 1. `TestDependencyAudit` in `tests/unit/test_ci_integrations.py` pins the two jobs' permissions and what each runs.

**Alternative considered:** One job that runs `uv lock`, commits, and pushes with a write token, or Dependabot's own security updates alone.

**Why rejected:** `uv lock` can build a package from source, which runs its code, so one job would expose a token that writes to the repository. Dependabot pins the fixed version, which is what broke pull request 468, and a pull request opened with the workflow's own token starts no CI.

**How to reverse:** Delete `.github/workflows/security-updates.yml` and the `TestDependencyAudit` tests that name it, and the `SECURITY_UPDATES_TOKEN` secret. The audit and the lockfile check in `ci.yml` stand on their own.
