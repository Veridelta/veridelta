---
type: Decision
title: The docs site is one build of main
description: Why the docs site is built from main and deployed through the Pages actions, with no copy per release, rather than versioned with mike.
status: stable
decided: 2026-10-11
generated: { by: claude-code, at: 2026-10-11T18:08:40+00:00 }
---

**Claim:** The docs site is one build of `main`, deployed by `.github/workflows/docs.yml` through `actions/upload-pages-artifact` and `actions/deploy-pages`. No release keeps a copy of its own. [Upgrading](https://veridelta.github.io/veridelta/upgrading/) lists what each breaking release changed, and the [changelog](https://veridelta.github.io/veridelta/changelog/) lists every change, for anyone on an older release.

**Evidence:** `.github/workflows/docs.yml` builds with `mkdocs build --strict` on every push to `main`, and the newest commit on `main` deploys last. The `gh-pages` branch was removed on 2026-10-09, after #357 moved the deploy to the Pages actions. The maintainer chose latest only on 2026-10-11, in the checklist on issue 290. The site can describe a merged change for the minutes before its release pull request ships.

**Alternative considered:** Versioned docs through `mike`, with one copy of the site per release and a version picker.

**Why rejected:** `mike` publishes to a `gh-pages` branch, which brings back the branch, its ruleset, and a second deploy path that the Pages change retired. Below 1.0, with one known user and an Upgrading page, a copy per release costs more than it serves.

**How to reverse:** Add `mike` to the dev group, deploy each release with `mike deploy` to `gh-pages` from `.github/workflows/docs.yml`, and set Settings, Pages, Source back to that branch.
