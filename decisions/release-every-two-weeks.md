---
type: Decision
title: Releases ship every two weeks, and a patch ships at once only for an urgent fix
description: Why user-facing changes wait for a release every two weeks, chosen over one patch release for each fix, which put 79 versions on PyPI in a month.
status: stable
decided: 2026-11-25
generated: { by: claude-code, at: 2026-11-25T14:32:13Z }
---

**Claim:** A release ships every two weeks when a user-facing change has landed since the last one. A patch ships at once only for a security fix in the package, a fix for a wrong verdict or lost data, or a fix for a broken install. A change to CI, the docs, the tests, or only the lockfile ships no release. A breaking change waits for the next minor version, so a user meets one upgrade section at a time.

**Evidence:** Issue 124 set one patch release for each fix on 2026-10-06, for the cleanup track, and all 13 of its findings are closed. PyPI then held 79 versions from 2026-10-06 to 2026-11-05, 35 of them on 2026-10-07 alone, so a user pinning a version met a new one most days. Each release needs a maintainer to approve the Live Warehouses run and the `pypi` deployment, as `CONTRIBUTING.md` says under "Releasing". The opposite drift cost too: 0.43.0 gathered 52 commits over 20 days, a `docs/upgrading.md` section of fifteen changes, and security fixes that waited three weeks. `.github/dependabot.yml` sets `versioning-strategy: lockfile-only` for `uv`, so a Dependabot bump changes no requirement a user installs.

**Alternative considered:** Keep one patch release for each fix, so a user gets every fix the day it merges.

**Why rejected:** It costs the maintainer two approvals for each fix, and it fills PyPI with versions that differ by one change, which a user cannot tell apart from the version list. An urgent fix still ships at once under this rule, so the users who need a fix that day still get it.

**How to reverse:** Delete the paragraph on when to release in `CONTRIBUTING.md` under "Releasing", set this record's `status` to `deprecated`, and record the new cadence.
