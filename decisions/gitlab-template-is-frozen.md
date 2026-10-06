---
type: Decision
title: The GitLab CI template is frozen
description: Why the GitLab template stays as it is, gains no new input unless a GitLab user asks, and is neither kept in step with the Action nor removed.
status: stable
decided: 2026-10-06
generated: { by: claude-code, at: 2026-10-06T16:05:00Z }
---

**Claim:** `ci/gitlab/veridelta.yml` stays, documented and tested as it is at 0.14.0. A new input of the GitHub Action is not mirrored into it unless a GitLab user asks, and the parity test in `tests/unit/test_ci_integrations.py` then lists the input as not mirrored, with this record as the reason.

**Evidence:** No issue, discussion, or person has named GitLab, and the maintainer uses GitHub: the cut list and the maintainer's second answer in `product/USERS.md`. The template runs on no GitLab runner in CI, so its tests are static. Every Action input added since 0.11.0 was mirrored by hand, most recently in #118.

**Alternative considered:** Removing the template and its tests in the next minor release, with the docs pointing at the `v0.14.0` include, which keeps working.

**Why rejected:** Removal is a breaking change for a user nobody has seen, to save a few lines of static tests. Freezing costs nothing now and keeps the door open.

**How to reverse:** To maintain it again, resume mirroring and delete the exemption list from the parity test. To remove it, delete the file, its section in `docs/ci.md`, and `TestGitLabTemplate`, and set this record to `deprecated`.
