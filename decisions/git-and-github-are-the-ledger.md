---
type: Decision
title: Git and GitHub are the ledger, and a run record ties a verdict to them
description: Why Veridelta records hashes, the commit, and the CI run in each run instead of keeping its own ledger of who declared a rule and who approved a run, chosen over a ledger file in the repository.
status: stable
decided: 2026-10-28
generated: { by: claude-code, at: 2026-10-28T19:30:00Z }
---

**Claim:** Veridelta keeps no ledger of who declared a rule or who approved a run. The configuration and the baseline are committed files, so git records who changed them, and GitHub records each pull request's approval. Each `veridelta run` writes a run record instead: the SHA-256 of the configuration and the baseline, the commit, and the CI run, which tie its verdict to those records.

**Evidence:** `collect_provenance` in `src/veridelta/_provenance.py` builds the record, and `RunProvenance` in `src/veridelta/models.py` holds it on `DiffSummary.provenance`. `tests/unit/test_provenance.py` holds the hashes to the files' bytes, the commit to `git rev-parse HEAD`, and the run URL to GitHub's variables, and checks that no field can fail a run. `tests/unit/test_report.py` checks that the report and the pull request comment name it. The `action` job in `.github/workflows/ci.yml` checks that a real run's summary carries the configuration's and the baseline's hashes, the run's link, and `GITHUB_SHA`. [UC-03](../product/USERS.md#uc-03-sign-off-from-the-report-alone) is the reviewer who signs off from the report alone, and [issue 440](https://github.com/Veridelta/veridelta/issues/440) tracks the work.

**Alternative considered:** A ledger file in the repository, such as `approvals.jsonl`, that `veridelta` appends a line to when someone approves a run.

**Why rejected:** Anyone with write access can add any name to such a file, so it proves less than GitHub's own review record, which it would duplicate. Signed commits and required reviews on the configuration and the baseline give the same trail with identities GitHub verifies, at no cost on a public repository.

**How to reverse:** Write the use case that needs a ledger in `product/USERS.md` first. Then set this record's `status` to `deprecated`, and add the ledger beside `DiffSummary.provenance`, which it can key on the configuration's hash.
