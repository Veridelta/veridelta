I've confirmed the full release procedure; nothing was modified. Local `main` equals `origin/main` (882e1c5) with a clean tree, so I ran commitizen in place without a worktree. SHA1s of `CHANGELOG.md`, `pyproject.toml`, `mkdocs.yml`, `__init__.py` and `uv.lock` were identical before and after, and the tag count (10) and HEAD did not change.

**Summary**
- **Procedure:** the repo does not release with a plain `cz bump`. Each release is a PR on branch `chore/release-X.Y.Z` titled `chore(release): X.Y.Z`, made with `cz bump --files-only`. The generated changelog block is rewritten by hand into prose, and `uv.lock` is synced. After the squash-merge, an annotated tag `vX.Y.Z` (message `vX.Y.Z`) and a GitHub Release follow within seconds. The tag push publishes to PyPI about 15 minutes later.
- **Version:** commitizen picks **0.10.0** (MINOR), because no commit has `!` or a `BREAKING CHANGE` footer. Against that, the repo called 0.9.0 "the 1.0 candidate", but many intentional behavior changes have landed since.
- **If you choose 1.0.0:** `--increment MAJOR` gives 1.0.0 cleanly. The same PR should also delete `major_version_zero = true`, or later breaking commits will keep auto-bumping MINOR (see section 7).

## 1. Commitizen config and version carriers

`/home/user/veridelta/pyproject.toml` lines 233–243 (end of file):
```toml
[tool.commitizen]
name = "cz_conventional_commits"
version = "0.9.1"
tag_format = "v$version"
version_files = [
    "pyproject.toml:version",
    "src/veridelta/__init__.py:__version__",
    "mkdocs.yml:version",
]
update_changelog_on_bump = true
major_version_zero = true
```
- **Not set, so commitizen 4.13.10 defaults apply:**
  - `bump_message` (default `bump: version $current_version → $new_version`; never used on main)
  - `changelog_incremental` (false), `changelog_file`, `annotated_tag`, `gpg_sign`
  - `version_provider` (defaults to the `[tool.commitizen] version` line), `version_scheme`
  - `template`, `change_type_order`, and the pre/post bump hooks
- There is no `.cz.toml`, `cz.json`, `.cz.yaml`, `setup.cfg` or `.bumpversion*` file.

Files containing `0.9.1` (excluding `uv.lock`, `.venv` and `site`):

| File:line | Text | Updated by |
|---|---|---|
| `pyproject.toml:7` | `version = "0.9.1"` (`[project]`) | `version_files` |
| `pyproject.toml:235` | `version = "0.9.1"` (`[tool.commitizen]`) | commitizen |
| `src/veridelta/__init__.py:37` | `__version__ = "0.9.1"` | `version_files` |
| `mkdocs.yml:43-44` | `extra:` / `  version: 0.9.1` | `version_files` |
| `CHANGELOG.md:1` | `## v0.9.1 (2026-09-11)` | changelog |
| `uv.lock:3975-3976` | `name = "veridelta"` / `version = "0.9.1"` | **Not** in `version_files`. A 1-line change in every release commit since 0.5.1. |

Where the version is read:
- `docs/roadmap.md:3`: `Veridelta is currently in **v{{ config.extra.version }}**.`
- `src/veridelta/cli.py:358`: `version=f"veridelta {__version__}"`
- `src/veridelta/datasets.py:24-32`: builds `git_ref = f"v{__version__}"` from the installed version, then `_TAXI_URL = f"https://raw.githubusercontent.com/Veridelta/veridelta/{_GIT_REF}/docs/assets/data/sample_taxi_data.parquet"`.
  - So `load_nyc_taxi()` in a released install needs the tag to exist.
  - CI is not affected: `tests/notebooks/test_tutorials.py:65-67` seeds the cache from the repo's own copy.

## 2. CHANGELOG convention

Verbatim lines 1–105 and 136–139 of `/home/user/veridelta/CHANGELOG.md`. Lines 106–135 (v0.7.0's `### Feat` with 7 bullets and `### Fix` with 2) are left out.
````markdown
## v0.9.1 (2026-09-11)

CI now pins JavaScript actions that declare Node 24, so GitHub-hosted runners
stop forcing the deprecated Node 20 runtime onto checkout, setup-uv, and
codecov.

### Chore

- bump `actions/checkout` to v5, `astral-sh/setup-uv` to v7, and
  `codecov/codecov-action` to v6 so every workflow step we pin runs on Node 24

## v0.9.0 (2026-09-11)

Internal. The public API does not change. This is the 1.0 candidate: the
breaking-change budget for the 0.7–0.9 line was spent in 0.7.0, 0.8.0 was
additive, and 0.9.0 is compiler and toolchain work.

Warehouse SQL now projects stages 1–7 once per column through a pair of
normalization CTEs. The join and the mismatch tally read those values, so
adding a transform no longer copies the expression tree into every later
predicate. Emitted SQL stays roughly linear in the number of compared
columns; the DuckDB harness is the same check it was in 0.6.0.

`engine.py`, `models.py`, `sentinels.py`, and `connectors/sql.py` are gated
at 100% branch coverage. `cast_to` is resolved through a `TypedDict` and an
exhaustive lookup table. `make lint` runs strict pyright. Global mypy
`ignore_missing_imports` is gone; missing stubs stay on per-module overrides
for optional drivers and test-only imports.

### Refactor

- project warehouse transforms through `_src_normalized` / `_tgt_normalized`
  CTEs so each column is normalized once, verified by the existing DuckDB
  harness and a SQL-length regression test
- convert `_get_effective_rule` to an `EffectiveRule` TypedDict so `cast_to`
  stays a `CastTarget` and `_CAST_TARGETS` is exhaustive

### Test

- fail `make test` and the core CI job unless the four core modules stay at
  100% branch coverage, and drop the dead `raise NotImplementedError`
  coverage exclude

### Chore

- add strict pyright to `make lint` and the CI lint job
- drop the global mypy `ignore_missing_imports` in favor of the existing
  per-module overrides

## v0.8.0 (2026-09-11)

Reporting and CLI polish. Purely additive: new flags default off, and the
exit codes stay 0 for a match and 1 for drift.

`--html PATH` writes one self-contained file. Styles and a 25-row pager are
inlined, so an air-gapped runner can open the report without fetching anything.
Embedded rows are capped (`--html-max-rows`, default 1000) and the truncation is
stated. Pushdown reports are labeled as primary-keys-only. Markup in column
names is escaped, and `<` inside the embedded JSON is rewritten so a value
cannot close the script block.

Progress chatter moved to stderr so `veridelta run --json | jq` does not have
to strip it. `--quiet` silences that chatter. `--version` / `-V` print the
package version.

`docs/configuration.md` is now bound to `DiffConfig`, `DiffRule`, and
`SourceConfig` by a test, so a new field cannot ship undocumented. A Getting
Started notebook covers local diffing and the `DiffResult` accessors added in
0.7.0.

### Feat

- write a standalone HTML report from the Python API or `--html`, with an
  inlined pager and a row cap that is visible in the page
- add `--json` so CI can read `DiffSummary` from stdout, `--quiet` to suppress
  progress, and `--version` / `-V`
- route every progress line to stderr, including the artifact path, so a piped
  `--json` run stays valid JSON
- add a Getting Started notebook and bind `docs/configuration.md` to the three
  config models so missing fields fail the test suite

### Fix

- explain configuration errors as a problem with the file, not the data, and
  point unexpected failures at the issue tracker with the exception class

## v0.7.0 (2026-09-10)

Two breaking changes, both mechanical.

`DiffEngine.run()` and `DiffEngine.run_from_configs()` now return a `DiffResult` rather
than a `DiffSummary`. Existing code reaches the old value through `.summary`:

```python
summary = DiffEngine(config, src, tgt).run().summary
```

`SourceType` and `output_format` are now closed sets. `SourceType` accepts `csv`,
`parquet`, `json`, `ndjson`, `arrow`, and `excel`; `output_format` accepts everything
but `excel`. The removed names (`fixed_width`, `netcdf`, `shapefile`, `geopackage`,
`sql`, `avro`, `xml`, `delta`) never had an implementation and already raised
`ConfigError` at run time, so a config using one was already broken; it now fails when
the config loads instead. Delta Lake is unaffected and still reached through the
`delta_lake` source type.
[... lines 106-135 elided ...]
### BREAKING CHANGE

- `DiffEngine.run()` and `DiffEngine.run_from_configs()` return `DiffResult`
- `SourceType` and `output_format` are constrained to implemented formats
````

**How the file is edited**
- Every section is added by exactly one commit: the squash-merged release PR. `git log -- CHANGELOG.md` shows only the `chore(release): …` commits (#6 through #22), plus the 0.2.0 direct commit and #5.
- There are no follow-up edits, and feature PRs never touch the file. #33's body says: "CHANGELOG.md is left to `cz bump`."

**The format is hand-written prose under commitizen-style headings**
- It opens with commitizen's heading `## vX.Y.Z (YYYY-MM-DD)`, then 1–4 prose paragraphs that lead with the user-visible or breaking change.
- Bullets under `### Feat/Fix/Refactor/Test/Chore/BREAKING CHANGE` are rewritten by hand. They are lowercase and imperative, wrapped at about 80–90 columns with a 2-space continuation, and have **no `**scope**:` prefix and no `(#NN)`**.
- The three recent sections compare like this:

| Version | Source PR | Prose paragraphs | Sections |
|---|---|---|---|
| v0.9.1 | one `chore(ci)` PR (#21) | 1 | Chore (1 bullet). Commitizen generates nothing from a chore commit, so the whole section is hand-written. |
| v0.9.0 | one `refactor:` PR (#19) | 3 | Refactor (2), Test (1), Chore (2) |
| v0.8.0 | one `feat:` PR (#17) | 4 | Feat (4), Fix (1) |

- **BREAKING CHANGE:** v0.7.0 (lines 136–139) and v0.6.0 (176–181) put `### BREAKING CHANGE` **last**, restating the prose. Commitizen's default order puts it first (`defaults.py:147` `["BREAKING CHANGE", "Feat", "Fix", "Refactor", "Perf"]`). v0.5.1 and v0.4.0 called out behavior changes only in the opening prose.
- `### Chore` and `### Test` are hand-added, because `cz_conventional_commits` only renders feat/fix/refactor/perf. No `### Docs` or `### Build` heading has ever been used.
- v0.2.0 and v0.3.0 are close to raw commitizen output, e.g. line 270 `- warehouse and lakehouse connectors with SQL pushdown (#5)`.

**The release PR bodies spell out the procedure**
- #12 (0.5.1): "via `cz bump --files-only --increment PATCH`. The changelog entry was expanded by hand from the generated one-liner. Also syncs `uv.lock`, whose `veridelta` entry was still pinned at 0.4.0…"
- #10 (0.5.0): "via `cz bump --files-only`. The changelog prepended cleanly this time… The generated entry was expanded by hand to match the 0.4.0 format: a prose paragraph covering the behavior change, then the itemized list."
- #8 (0.4.0): "`cz bump` regenerated the full history at the bottom of `CHANGELOG.md` rather than prepending, because `tag_format = "v$version"`…". Headings were normalized to the `v` prefix afterwards.

## 3. Release history

| Tag | Type | Commit | Tag message |
|---|---|---|---|
| v0.9.1 | annotated | e367071 `chore(release): 0.9.1 (#22)` | `v0.9.1` |
| v0.9.0 | annotated | 5d465fe `chore(release): 0.9.0 (#20)` | `v0.9.0` |
| v0.8.0 | annotated | cea7626 `(#18)` | `v0.8.0` |
| v0.7.0 | annotated | 2f2f983 `(#16)` | `v0.7.0` |
| v0.6.0 | annotated | ca3eb23 `(#14)` | `v0.6.0` |
| v0.5.1 | annotated | ffbd8c9 `(#12)` | `Release v0.5.1` |
| v0.5.0 | annotated | b6a46ed `(#10)` | `Release v0.5.0` |
| v0.4.0 | annotated | 0514d0a `(#8)` | `v0.4.0` |
| v0.3.0 | annotated | 4e32496 `(#6)` | `v0.3.0: warehouse and lakehouse connectors` |
| v0.2.0 | lightweight | 1452820 `chore(release): bump version to 0.2.0` (direct commit, no PR) | n/a |

**Tags**
- Created by `nicholas.harder <nharder@umich.com>` (a local identity) and unsigned.
- `git cat-file -t v0.9.1` returns `tag`.

**Release commits**
- Always a PR from `chore/release-X.Y.Z`, titled `chore(release): X.Y.Z`.
- Squash-merged: author `Nick Harder <veridelta.labs@gmail.com>`, committer `GitHub`.
- The commitizen default subject `bump: version A → B` has never been used.
- `git show --stat v0.9.1` (parent 5159a00 = #21): `CHANGELOG.md | 11`, `mkdocs.yml | 2`, `pyproject.toml | 4`, `src/veridelta/__init__.py | 2`, `uv.lock | 2` (5 files, +16/−5).
- v0.9.0 touches the same 5 files (CHANGELOG +38).
- #22's body: "Version files: `0.9.0` → `0.9.1`." and "- [ ] After merge, tag `v0.9.1` and create the GitHub release".

**Timing for v0.9.1**
- PR merged 16:52:03Z, tag 16:52:09Z, GitHub Release 16:52:11Z.
- release.yml run created 16:52:12Z, job started 17:07:16Z, PyPI upload 17:07:24Z.
- Tags land 3–13 s after the merge for every release from v0.4.0 on, so this looks scripted.

**GitHub Releases**
- One exists for every tag: author NickHarder, name equals the tag, target `main`, no assets.
- The body is GitHub's auto-generated notes, not the changelog text: `## What's Changed` / `* chore(ci): run GitHub Actions on Node 24 by @NickHarder in …/pull/21` / … / `**Full Changelog**: …/compare/v0.9.0...v0.9.1`.
- Releases for v0.4.0–v0.7.0 were created later in batches; since v0.8.0 they appear seconds after the tag.

**PyPI** has 0.2.0 through 0.9.1 (latest 0.9.1). All 10 release.yml runs succeeded.

## 4. Workflows (full)

`/home/user/veridelta/.github/workflows/release.yml`:
```yaml
name: Publish to PyPI

on:
  push:
    tags:
      - "v*"  # Trigger exclusively when a version tag is pushed (e.g., v0.1.0)

jobs:
  build-and-publish:
    name: Build and Publish Artifacts
    runs-on: ubuntu-latest
    
    environment:
      name: pypi
      url: https://pypi.org/project/veridelta/
      
    permissions:
      id-token: write 
      contents: read

    steps:
      - name: Checkout Code
        uses: actions/checkout@v5
        with:
          fetch-depth: 0

      - name: Install uv and Set up Python
        uses: astral-sh/setup-uv@v7
        with:
          enable-cache: false
          python-version: "3.12"

      - name: Build Sdist and Wheel
        run: uv build

      - name: Publish to PyPI
        run: uv publish
```
`/home/user/veridelta/.github/workflows/docs.yml`:
```yaml
name: Deploy Documentation

on:
  push:
    branches: ["main"]
  workflow_dispatch:

permissions:
  contents: write

env:
  FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true

jobs:
  deploy-docs:
    name: Build & Deploy MkDocs
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v5
        with:
          fetch-depth: 0

      - name: Install uv and Set up Python
        uses: astral-sh/setup-uv@v7
        with:
          enable-cache: false
          python-version: "3.12"

      - name: Install Dependencies
        run: uv sync --all-extras --group dev

      - name: Deploy MkDocs to GitHub Pages
        run: uv run mkdocs gh-deploy --force --strict
```

**release.yml**
- Publishes to PyPI via trusted publishing (`id-token: write` plus `uv publish`, no token secret).
- It runs no tests, never checks that the tag matches `[project] version`, and cannot create a GitHub Release (`contents: read`).
- Every run waits about 15 minutes between job creation and job start, while the steps take about 10 s.
  - That fits a wait timer or required reviewer on the `pypi` environment.
  - This is **inferred**: environment rules can't be read without admin access.

**docs.yml**
- Runs on every push to `main`, not on tags. It force-pushes `gh-pages`, whose latest commit is `Deployed 882e1c5`.
- Merging the release PR therefore publishes the new `extra.version` to the site before the tag and PyPI exist.
- `mkdocs-jupyter` has no options (`mkdocs.yml:33`), so notebooks are not executed during the docs build.

**ci.yml** (lines 3–7) runs on pushes to `main` and on every PR, never on tags. It uses `uv sync --all-extras` without `--locked`, so a stale `uv.lock` would not fail CI.

## 5. Release documentation

- **No written release process:** `CONTRIBUTING.md`, README, `docs/`, `.cursor/rules/` and the Makefile have no release, bump, tag or PyPI instructions. The procedure is documented only in the release PR bodies quoted in section 2.
- **Commit rules:** `/home/user/veridelta/CONTRIBUTING.md:39-47` lists the Conventional Commits types (feat/fix/docs/test/chore/refactor), enforced by a hook. Lines 51–56 say PR titles must be conventional and CI is the gate.
- **Commit-msg hook:** `/home/user/veridelta/.pre-commit-config.yaml:27-31` runs commitizen `rev: v4.13.9` at `stages: [commit-msg]`. The dev dependency is `commitizen>=4.13.10` (`pyproject.toml:66`), and 4.13.10 is installed.
- **Inputs to a 1.0 decision:**
  - `pyproject.toml:17` `"Development Status :: 3 - Alpha"`.
  - `SECURITY.md:7-10` lists `>= 0.1.x` as supported.
  - #12's body calls a behavior change in a patch "defensible under a patch given `major_version_zero` and the Alpha classifier".
  - #16's body: "This release carries the entire breaking-change budget for the 0.7 through 0.9 line… so 1.0 can be a straight promotion rather than another migration."

## 6. Commits on origin/main since v0.9.1 (20, all squash-merged PRs)

| PR | SHA | Type | Subject | Call out? |
|---|---|---|---|---|
| 23 | 0b76861 | chore | tighten Cursor rules and add a docs rule | no |
| 24 | 21860ad | docs | align README, site, and docs rule with 0.9.1 | no |
| 25 | b56e0c2 | refactor | Phase 1 maintainability (shared rule folding, …) | yes (`DatasetError`) |
| 26 | 2081c15 | docs | Phase 2 onboarding (class docstrings, notebooks, field tables) | no |
| 27 | 6399822 | feat(connectors) | Phase 3 parity: lifecycle, logging, honest errors | yes |
| 28 | 9947b04 | build(lint) | enable Ruff C901 (no behavior change) | no |
| 29 | e2907f1 | fix(engine) | drop pattern-ignored columns from the target side too | minor |
| 30 | 30327a3 | docs | single roadmap source of truth, HTML report tutorial | no |
| 31 | 0c02eb7 | docs | rewrite README and site index | no |
| 33 | 14e8108 | fix | harden warehouse pushdown, close local/warehouse parity gaps | **yes (headline)** |
| 35 | 46bbac2 | feat(pushdown) | normalize primary keys, reject duplicates like a local run | **yes** |
| 36 | 4af2786 | docs | collapse tutorials into a four-notebook path | yes (URLs) |
| 37 | 656c449 | fix | numeric comparisons no longer truncate, wrap, or forgive NaN/inf | **yes** |
| 38 | 67f2c05 | chore(ci) | CI on stacked PRs, check tutorial notebooks | no (contributors only) |
| 39 | 1ad13ac | feat(config) | expand `${VAR}` in source/target blocks | **yes** |
| 40 | 7ef78fd | feat | fuzzy text matching (Levenshtein, Jaro-Winkler) | **yes** |
| 41 | 78292d2 | feat | propose value_map crosswalks (`veridelta crosswalk`) | new API |
| 42 | 126c804 | fix | hide connection secrets when printed; tutorials | **yes** |
| 43 | cf1ee10 | fix(models) | storage_options out of printed lakehouse configs | **yes** |
| 44 | 882e1c5 | fix(models) | nested storage_options out of printed file configs | yes |

- **Counts:** feat 5, fix 6, docs 5, chore 2, build 1, refactor 1.
- **Missing PR numbers:** #32 and #34 were merged into leftover base branches of stacked PRs and never reached main; they were re-landed as #36 and #35.
- **No breaking markers:** no subject has `!` and no body has a `BREAKING CHANGE` footer.
- **Scope of the change:** 50 files, +9566/−1844.

**Behavior changes a release note must call out** (quoted from the PR bodies, most important first):
1. **#33, warehouse fixes (the headline for 0.9.1 users):**
   - "Snowflake pushdown failed on every real run": zero-row `fetch_arrow_all()` returned `None`. The `snowflake` extra floor rises from 3.0.0 to **3.7.0**.
   - String literals were escaped the same way for every dialect. Backslashes broke regexes and sentinels, `x\' OR 1=1 --` could escape the literal, and Databricks dropped apostrophes. This is security-relevant.
   - Pushdown now compares non-numeric columns exactly even when a global tolerance is set.
   - Rules on renamed columns now apply locally. An exact-name rule now beats an ignore pattern, and the first declared rename wins.
   - New `ConfigError` cases: an empty `primary_keys`, an infinite tolerance, the same table on both sides, a renamed primary key in pushdown (later lifted by #35), headers that collide after normalization, and a negative `max_rows`.
   - The HTML report survives NaN/Inf values.
2. **#37, numeric comparison:**
   - With `strict_types` off, mixed numeric types are compared in their common type, so Int `10` vs Float `10.7` now mismatch locally and Decimals are no longer rounded to the target's scale.
   - "A `Float32` `0.1` no longer equals a `Float64` `0.1`."
   - Under a tolerance, NaN matches only NaN and infinity only itself, on both paths.
   - Unsigned differences no longer wrap.
   - The tolerance SQL shape changes. The advice to users is to add a tolerance or `cast_to` to forgive precision gaps.
3. **#35, pushdown keys:**
   - Primary keys are normalized (stages 1–7) in warehouse joins, and renamed keys work.
   - "Duplicated data now fails in the warehouse" with `DataIntegrityError`.
   - Added/removed/changed counts can change, and a run can issue up to 10 queries instead of 8.
   - `PushdownQueryType` gains `"duplicates"`, and the compilers gain a keyword-only `key_rules` parameter.
4. **#39, `${VAR}` expansion:**
   - Supports `${NAME}` and `${NAME:-default}` in `source`/`target` strings, including nested `options` and `storage_options`.
   - **A value that contains `${` literally must now be written `$${`.**
   - An unset variable without a default raises a `ConfigError` naming the variable. Expanded values are always strings, so Delta `version` and Iceberg `snapshot_id` still need literal YAML integers.
   - Also: `hide_input_in_errors=True` on the connection models, so validation errors no longer quote inputs such as passwords.
5. **#40, fuzzy matching:**
   - New rule fields `max_levenshtein_distance` and `min_jaro_winkler_similarity`, and a new `fuzzy` extra (`rapidfuzz>=3.0.0`, included in `all`).
   - Jaro-Winkler is local-only; pushdown raises `ConfigError`.
   - "`run()` now builds match expressions before the duplicate-key check": today this only means a missing `fuzzy` extra `ConfigError` wins over `DataIntegrityError`.
6. **#42–#44, printed configs hide credentials:**
   - Snowflake `password`, Databricks `access_token`, lakehouse `storage_options`, and file `options['storage_options']` are left out of printed configs (they are not masked).
   - The whole lakehouse map is omitted, so `AWS_REGION` no longer prints either.
   - `model_dump()` still includes all of them.
7. **#25, exception type:** `load_nyc_taxi` now raises `DatasetError`. It is **not** a `RuntimeError` subclass (`exceptions.py:50`), so `except RuntimeError` callers break.
8. **#27, connectors:** they gain `close()` and context-manager support, plus loggers. Two pushdown fixes also change counts: rows where one side is NULL no longer vanish from `changed_count`, and a run with every column ignored no longer reports all keys as drift.
9. **#29:** on a direct `DiffEngine` run, pattern-ignored columns are now dropped from the target side too, which fixes a false `ConfigError` under `schema_mode="exact"`.
10. **#36, tutorial URLs:** the tutorials are renamed to `01_…`–`04_…`. "Old published notebook URLs will 404."
11. **#41, crosswalk proposals (additive):**
    - New `veridelta crosswalk` subcommand (`--min-confidence 0.95`, `--min-support 5`, `--sample-fraction`, `--json`). Rules go to stdout, evidence to stderr; exit codes are 0/1/2.
    - New `DiffEngine.propose_value_maps()` and `DiffEngine.propose_value_maps_from_configs()`.
    - Warehouse sources raise `ConnectorError`.
12. **#38, CI only:** CI now runs on stacked PRs and executes the tutorial notebooks. Nothing user-facing.

**Public API since v0.9.1:** the package root adds `DatasetError`, `ValueMapEntry` and `ValueMapProposal`; nothing was removed.

## 7. Commitizen dry runs (read-only, in place)

I used `.venv/bin/cz` directly (commitizen 4.13.10, `PYTHONDONTWRITEBYTECODE=1`) rather than `uv run`, which would sync `.venv`. `cz version --project` returns `0.9.1`.

`cz bump --dry-run --yes`:
```text
bump: version 0.9.1 → 0.10.0
tag to create: v0.10.0
increment detected: MINOR

## v0.10.0 (2026-09-26)

### Feat

- propose value_map crosswalks from the data (veridelta crosswalk) (#41)
- fuzzy text matching with Levenshtein and Jaro-Winkler limits (#40)
- **config**: expand ${VAR} references in source and target blocks (#39)
- **pushdown**: normalize primary keys and reject duplicates like a local run (#35)
- **connectors**: Phase 3 parity - lifecycle, logging, honest errors, and pushdown edge-case tests (#27)

### Fix

- **models**: keep nested storage_options out of printed file configs (#44)
- **models**: keep storage_options out of printed lakehouse configs (#43)
- hide connection secrets when printed; tutorials show value_map proposals and ${VAR} (#42)
- numeric comparisons no longer truncate, wrap, or forgive NaN and infinity (#37)
- harden warehouse pushdown and close local/warehouse parity gaps (#33)
- **engine**: drop pattern-ignored columns from the target side too (#29)

### Refactor

- Phase 1 maintainability - shared rule folding, alignment maps, sliced DiffEngine.run (#25)
```
That is the whole generated block. The 5 docs, 2 chore and 1 build PRs are absent, and it carries `**scope**:` prefixes and `(#NN)` references, which the house style strips.

**Other variants**
- `cz bump --get-next` returns `0.10.0`.
- `cz bump --dry-run --yes --increment MAJOR` gives `bump: version 0.9.1 → 1.0.0`, `tag to create: v1.0.0`, `increment detected: MAJOR` and the heading `## v1.0.0 (2026-09-26)`. A diff against the MINOR run shows only those lines differ.
- `cz bump 1.0.0 --dry-run --yes` and `--get-next --increment MAJOR` also give 1.0.0.

**What commitizen's source says** (`.venv/lib/python3.11/site-packages/commitizen/commands/bump.py`)
- **Lines 150–154:** the `major_version_zero` setting only swaps in `BUMP_MAP_MAJOR_VERSION_ZERO` (breaking changes count as MINOR) during auto-detection. An explicit `--increment` or a manual version bypasses it.
- **Lines 177–180:** the "`--major-version-zero is meaningless for current version`" error checks only the CLI flag, not the config.
  - So if 1.0.0 ships with `major_version_zero = true` still in `pyproject.toml`, a later `feat!:` would silently auto-bump 1.0.0 to 1.1.0.
  - A 1.0.0 release PR should delete that line.
- **What gets written:**
  - `update_changelog_on_bump` turns the changelog on for every bump (lines 258–264).
  - The bump always passes `"incremental": True` (line 317) and writes `CHANGELOG.md` (lines 331–337) before updating the version files (343–353).
  - `--files-only` and `--version-files-only` stop after that, with no commit or tag (lines 369–377).
  - In 4.13.10, `--files-only` prints "deprecated … Use --version-files-only instead."
- **Changelog date:** the heading date is the day the command runs.

**Cautions**
- Never run a bare `cz changelog`. `changelog_incremental` is unset, so it would regenerate the whole file from git history and wipe the hand-written prose (#8 describes a similar accident).
- `uv.lock` is not in `version_files`. Run `uv lock` after the bump to get the usual one-line `veridelta` diff.