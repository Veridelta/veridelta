---
render_macros: false
---

# CI integrations

Veridelta ships a GitHub Action and a GitLab CI template. Both run `veridelta run`, then report the result:

- they post its Markdown summary where reviewers look;
- they keep the JSON summary, the HTML report, and the OpenTelemetry metrics as artifacts;
- they fail the job on drift or on an error.

Both are available from the release after 0.10.0; the examples pin `v0.12.0`.

## GitHub Actions

```yaml
name: Data parity
on: pull_request

permissions:
  contents: read
  pull-requests: write   # only needed for the summary comment

jobs:
  compare:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: Veridelta/veridelta@v0.12.0
        with:
          config: veridelta.yaml
          extras: snowflake
        env:
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
```

**Version.** Pin the action to a release tag such as `v0.12.0`, or to a commit SHA. The action installs Veridelta from its own ref, so the tag you pin is the version that runs. To keep the action at one ref and install a different version from PyPI, set `version`.

**Credentials.** Pass credentials as step environment variables, as above, and reference them from the configuration as `${SNOWFLAKE_PASSWORD}` (see [Environment variables](configuration.md#environment-variables)).

**What it does:**
- Appends the summary to the job summary.
- Uploads `summary.json`, `summary.md`, `report.html`, and `otel-metrics.json` as one artifact.
- On `pull_request` and `pull_request_target` events, keeps one comment on the pull request up to date, one per configuration.

**Fork PRs.** Pull requests from forks get a read-only token, so the comment step logs a warning instead of failing.

### Inputs

| Input | Default | Meaning |
| :--- | :--- | :--- |
| `config` | `veridelta.yaml` | Configuration file, relative to `working-directory`. |
| `working-directory` | `.` | Directory the comparison runs in. |
| `extras` | empty | Optional extras to install, comma-separated, such as `database,fuzzy`. |
| `version` | empty | Install this version from PyPI instead of the action's own ref. |
| `python-version` | `3.12` | Python to run Veridelta with. |
| `html-max-rows` | `1000` | Rows per table in the HTML report. |
| `fail-on-mismatch` | `true` | Fail the step on drift. An error always fails it. |
| `comment` | `true` | Keep a summary comment on the pull request. |
| `github-token` | `github.token` | Token used to comment. |
| `upload-artifact` | `true` | Upload the reports as an artifact. |
| `artifact-name` | derived | Artifact name. Set it when a matrix runs one configuration more than once, since artifact names must be unique within a run. |

### Outputs

| Output | Meaning |
| :--- | :--- |
| `status` | `match`, `drift`, or `error`. |
| `is-match` | `true` when the comparison matched within its threshold. |
| `exit-code` | Exit code of `veridelta run`. |
| `summary-json`, `summary-markdown`, `report-html` | Paths to the reports. Empty when the run did not finish. |
| `otel-metrics` | Path to the run's [OpenTelemetry metrics](configuration.md#opentelemetry-metrics). Empty when the run did not finish. |

To act on drift in a later step instead of failing, set `fail-on-mismatch: false` and read `status`.

### Sending metrics to an observability backend

From 0.12.0, the action also writes the run's [OpenTelemetry metrics](configuration.md#opentelemetry-metrics). A later step can send them to any OTLP/HTTP endpoint, such as a Collector or a vendor's OTLP intake. `always()` sends a drifting run's metrics too, after the action's step has failed:

```yaml
      - uses: Veridelta/veridelta@v0.12.0
        id: veridelta
        with:
          config: veridelta.yaml
      - name: Send the metrics
        if: always() && steps.veridelta.outputs.otel-metrics != ''
        env:
          OTLP_ENDPOINT: ${{ secrets.OTLP_ENDPOINT }}
          METRICS: ${{ steps.veridelta.outputs.otel-metrics }}
        run: >-
          curl --fail -sS -X POST -H "Content-Type: application/json"
          --data-binary "@$METRICS" "$OTLP_ENDPOINT/v1/metrics"
```

Add any header your backend requires, such as an API key, from a secret.

## GitLab CI

```yaml
include:
  - remote: https://raw.githubusercontent.com/Veridelta/veridelta/v0.12.0/ci/gitlab/veridelta.yml
    inputs:
      config: veridelta.yaml
      extras: snowflake
```

The template defines one job, named `veridelta` by default, which:
- installs the release the template ships with, so include it from a release tag;
- prints the summary to the job log;
- keeps the reports and the OpenTelemetry metrics as artifacts, exposed on the merge request as "Veridelta report". A later job can send `veridelta-report/otel-metrics.json` to an OTLP/HTTP endpoint as above.

**Merge request notes.** To keep a summary note on the merge request, add a project access token with the `api` scope as a masked CI/CD variable named `VERIDELTA_GITLAB_TOKEN`. Without it, the job still runs and reports.

**Inputs:**
- `config`, `version`, `extras`, `html-max-rows`, `fail-on-mismatch`, and `comment` mean what they do for the GitHub Action.
- `stage`, `job-name`, and `image` place the job in your pipeline.

**Credentials.** Set them as masked CI/CD variables; the configuration reads them as `${NAME}`.

## Checking configurations in review

`veridelta validate` catches a configuration that cannot run before the comparison job does, and needs no credentials:

```yaml
- uses: astral-sh/setup-uv@v7
- run: uvx veridelta@0.12.0 validate -c veridelta.yaml --allow-missing-env
```

`--allow-missing-env` reads each unset `${NAME}` as the text `NAME`, with a warning, so the job needs no secrets. The job exits `1` on an error, and a warning never fails it. Install the same extras the comparison uses, such as `uvx --from 'veridelta[snowflake]==0.12.0' veridelta validate ...`: `validate` checks the environment it runs in. See [Checking a configuration](configuration.md#checking-a-configuration).

## Exit codes and statuses

Both integrations read `veridelta run`'s exit code together with its JSON summary:

| Status | Exit code | Meaning |
| :--- | :--- | :--- |
| `match` | 0 | The comparison is within its threshold. |
| `drift` | 1 | The JSON summary reports `is_match: false`. |
| `error` | anything else | The run did not finish, such as a configuration error, an unreachable source, or a missing extra. There is no summary, and the job always fails. |
