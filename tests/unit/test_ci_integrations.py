# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Static checks for the GitHub Action, the GitLab CI template, and the workflows.

None of them can run inside the test suite, so these pin the contracts a typo
would break: every input a step reads is declared, no input is expanded inside a
shell script, third-party actions are pinned, the `veridelta run` command line
they build still parses with the CLI's own parser, and a release publishes only
a new version, only from its tag, with no more permission than each job needs.
They also pin the CI safeguards: one required check covers every job, jobs have
time limits, and only jobs GitHub never started are re-run.
"""

import re
import shlex
from pathlib import Path
from typing import Any

import pytest
import yaml

from veridelta import __version__
from veridelta.cli import build_parser

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_ACTION = _ROOT / "action.yml"
_GITLAB = _ROOT / "ci" / "gitlab" / "veridelta.yml"
_RELEASE = _ROOT / ".github" / "workflows" / "release.yml"
_WORKFLOWS = sorted((_ROOT / ".github" / "workflows").glob("*.yml"))
_DEPENDABOT = _ROOT / ".github" / "dependabot.yml"
_DOCS = _ROOT / ".github" / "workflows" / "docs.yml"
_CI = _ROOT / ".github" / "workflows" / "ci.yml"
_RERUN = _ROOT / ".github" / "workflows" / "rerun-dropped.yml"
_COMMIT_PIN = re.compile(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}")


def _action() -> dict[str, Any]:
    """Load the composite action definition."""
    loaded: dict[str, Any] = yaml.safe_load(_ACTION.read_text(encoding="utf-8"))
    return loaded


def _steps() -> list[dict[str, Any]]:
    """Return the action's steps."""
    steps: list[dict[str, Any]] = _action()["runs"]["steps"]
    return steps


def _run_script() -> str:
    """Return the script of the step that runs Veridelta."""
    script: str = next(step for step in _steps() if step.get("id") == "run")["run"]
    return script


def _cli_arguments(script: str) -> list[str]:
    """Extract the `veridelta run` arguments from a script, with variables filled in.

    Args:
        script (str): Shell script containing one `veridelta run` invocation.

    Returns:
        list[str]: The arguments after `veridelta`, as the CLI parser sees them.
    """
    line = next(line for line in script.splitlines() if "veridelta run" in line)
    command = line[line.index("veridelta run") :].split(">", 1)[0]
    # Row caps must be numbers for the parser; every other value is a path or name.
    filled = re.sub(
        r"\$\{?[A-Z_]*MAX_ROWS\}?|\$\[\[\s*inputs\.html-max-rows\s*\]\]", "1000", command
    )
    filled = re.sub(r"\$\{?[A-Za-z_][A-Za-z0-9_]*\}?", "placeholder", filled)
    filled = re.sub(r"\$\[\[\s*inputs\.[A-Za-z0-9_-]+\s*\]\]", "placeholder", filled)
    return shlex.split(filled)[1:]


class TestGitHubAction:
    """Pin the composite action's contract."""

    def test_it_declares_every_input_it_reads(self) -> None:
        """Ensure a renamed or misspelled input fails here rather than reading as empty."""
        declared = set(_action()["inputs"])
        # `if:` conditions read inputs without `${{ }}`, so match the bare form.
        referenced = set(re.findall(r"\binputs\.([A-Za-z0-9_-]+)", _ACTION.read_text()))

        assert referenced <= declared, referenced - declared
        assert referenced == declared, declared - referenced

    def test_it_never_expands_an_expression_inside_a_shell_script(self) -> None:
        """Ensure inputs reach scripts through `env` only, so none can inject shell syntax."""
        for step in _steps():
            if "run" in step:
                assert "${{" not in step["run"], step["name"]

    def test_it_names_a_shell_for_every_script(self) -> None:
        """Ensure composite steps run under bash on every runner, Windows included."""
        for step in _steps():
            if "run" in step:
                assert step.get("shell") == "bash", step["name"]

    def test_it_pins_third_party_actions_to_commits(self) -> None:
        """Ensure a moved tag upstream cannot change what this action runs."""
        for step in _steps():
            if "uses" in step:
                assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", step["uses"]), step["uses"]

    def test_its_outputs_are_written_by_the_run_step(self) -> None:
        """Ensure each declared output is one the script actually sets."""
        script = _run_script()
        for name, output in _action()["outputs"].items():
            match = re.fullmatch(r"\$\{\{ steps\.run\.outputs\.([a-z-]+) \}\}", output["value"])
            assert match is not None, name
            assert f'echo "{match.group(1)}=' in script, name

    def test_its_command_line_parses_with_the_cli(self) -> None:
        """Ensure a renamed CLI flag breaks this test instead of every user's workflow."""
        arguments = _cli_arguments(_run_script())

        parsed = build_parser().parse_args(arguments)

        assert parsed.command == "run"
        assert parsed.json is True
        assert parsed.html == "placeholder/report.html"
        assert parsed.markdown == "placeholder/summary.md"
        assert parsed.markdown_max_rows == 1000
        assert parsed.otel == "placeholder/otel-metrics.json"

    def test_it_exposes_the_metrics_file_only_for_a_finished_run(self) -> None:
        """Ensure `otel-metrics` names the export after a match or drift, and is empty on error."""
        script = _run_script()

        assert _action()["outputs"]["otel-metrics"]["value"] == (
            "${{ steps.run.outputs.otel-metrics }}"
        )
        assert 'echo "otel-metrics="\n' in script
        assert 'echo "otel-metrics=$out/otel-metrics.json"\n' in script


def _gitlab() -> tuple[dict[str, Any], dict[str, Any]]:
    """Load the GitLab template's `spec` header and its job document."""
    header, jobs = yaml.safe_load_all(_GITLAB.read_text(encoding="utf-8"))
    return header, jobs


def _gitlab_job() -> dict[str, Any]:
    """Return the template's single job."""
    _, jobs = _gitlab()
    (job,) = jobs.values()
    found: dict[str, Any] = job
    return found


class TestGitLabTemplate:
    """Pin the GitLab CI template's contract."""

    def test_it_declares_every_input_it_reads(self) -> None:
        """Ensure each `$[[ inputs.x ]]` is declared, and each declared input is used."""
        header, _ = _gitlab()
        declared = set(header["spec"]["inputs"])
        referenced = set(re.findall(r"\$\[\[\s*inputs\.([A-Za-z0-9_-]+)", _GITLAB.read_text()))

        assert referenced == declared

    def test_it_passes_inputs_to_the_script_as_variables(self) -> None:
        """Ensure no input is interpolated into the shell script itself."""
        for line in _gitlab_job()["script"]:
            assert "$[[" not in line

    def test_its_command_line_parses_with_the_cli(self) -> None:
        """Ensure a renamed CLI flag breaks this test instead of every pipeline."""
        arguments = _cli_arguments(_gitlab_job()["script"][0])

        parsed = build_parser().parse_args(arguments)

        assert parsed.json is True
        assert parsed.markdown == "veridelta-report/summary.md"
        assert parsed.markdown_max_rows == 1000
        assert parsed.otel == "veridelta-report/otel-metrics.json"

    def test_it_installs_the_release_it_ships_with(self) -> None:
        """Ensure commitizen keeps the default version in step with the package.

        `cz bump` rewrites the line marked `veridelta-version`, so a template
        included from a release tag installs that same release.
        """
        header, _ = _gitlab()

        assert header["spec"]["inputs"]["version"]["default"] == __version__

    def test_its_merge_request_note_script_compiles(self) -> None:
        """Ensure the embedded Python that posts the note is at least valid syntax."""
        script = _gitlab_job()["script"][0]
        body = script.split("<<'PY'", 1)[1].split("\nPY\n", 1)[0]

        compile(body.split("\n", 1)[1], "note.py", "exec")


def _release() -> dict[str, Any]:
    """Load the release workflow."""
    loaded: dict[str, Any] = yaml.safe_load(_RELEASE.read_text(encoding="utf-8"))
    return loaded


def _release_triggers() -> dict[str, Any]:
    """Return the release workflow's triggers.

    YAML 1.1 reads a bare `on` key as the boolean `True`, which is how PyYAML
    loads every GitHub workflow.
    """
    workflow: dict[Any, Any] = yaml.safe_load(_RELEASE.read_text(encoding="utf-8"))
    triggers: dict[str, Any] = workflow[True] if True in workflow else workflow["on"]
    return triggers


def _release_job(name: str) -> dict[str, Any]:
    """Return one job of the release workflow."""
    job: dict[str, Any] = _release()["jobs"][name]
    return job


def _release_script(name: str) -> str:
    """Return every shell script of one release job, joined."""
    return "\n".join(step["run"] for step in _release_job(name)["steps"] if "run" in step)


class TestReleaseWorkflow:
    """Pin the release workflow: a merged version bump becomes a tag, a package, and a page.

    PyPI's trusted publisher is bound to this file's name and to the `pypi`
    environment, so neither may change without updating PyPI first.
    """

    def test_it_runs_on_main_on_version_tags_and_by_hand(self) -> None:
        """Ensure merges reach the tagging job and version tags reach publishing."""
        triggers = _release_triggers()

        assert set(triggers) == {"push", "workflow_dispatch"}
        assert triggers["push"]["branches"] == ["main"]
        assert triggers["push"]["tags"] == ["v[0-9]+.[0-9]+.[0-9]+"]

    def test_it_tags_only_from_main(self) -> None:
        """Ensure a branch dispatched by hand cannot tag its own commit."""
        assert _release_job("tag")["if"] == "github.ref == 'refs/heads/main'"

    def test_it_tags_only_a_version_pypi_lacks(self) -> None:
        """Ensure a merge that leaves the version alone releases nothing.

        Only a 404 from PyPI's page for the version continues; a version PyPI
        already has stops cleanly, and any other answer fails the job rather
        than guessing.
        """
        script = _release_script("tag")

        assert '["project"]["version"]' in script
        assert '"https://pypi.org/pypi/veridelta/${version}/json"' in script
        assert re.search(r"^\s*200\)[^\n]*exit 0", script, re.MULTILINE)
        assert re.search(r"^\s*404\) ;;", script, re.MULTILINE)
        assert re.search(r"^\s*\*\)[^\n]*exit 1", script, re.MULTILINE)

    def test_it_tags_the_merged_commit_and_never_moves_a_tag(self) -> None:
        """Ensure an annotated tag lands on the commit that carries the version.

        A merge that lands while the version still awaits its release finds the
        tag on the earlier commit. That commit is the one released, so the job
        notes it and carries on rather than moving the tag or failing: the only
        failure left is PyPI answering something other than 200 or 404.
        """
        script = _release_script("tag")

        assert 'git tag -a "$tag" -m "$tag" "$GITHUB_SHA"' in script
        assert '!= "$GITHUB_SHA"' in script
        assert "::notice::${tag} is on ${existing}" in script
        assert not re.search(r"git (tag|push)\b[^\n]*\s(-f|--force)\b", script)
        assert script.count("exit 1") == 1

    def test_it_starts_a_release_run_only_when_none_is_under_way(self) -> None:
        """Ensure the tag's release is started once, and again if it was stopped.

        A run waiting for the `pypi` approval counts as under way, so a later
        merge never asks for a second approval. A run that was rejected or
        cancelled does not, so the next merge starts the tag's release again.
        """
        script = _release_script("tag")
        listing = f'gh run list --workflow {_RELEASE.name} --branch "$tag"'

        assert listing in script
        assert 'select(.status != "completed")' in script
        assert script.index(listing) < script.index(f"gh workflow run {_RELEASE.name}")

    def test_it_starts_the_publish_run_on_the_new_tag(self) -> None:
        """Ensure the tag is published although a token-pushed tag starts no workflow.

        GitHub starts no run for a tag pushed with the job's own token, but it
        does for a `workflow_dispatch`, so the job dispatches this file on the tag.
        """
        assert f'gh workflow run {_RELEASE.name} --ref "$tag"' in _release_script("tag")

    def test_it_publishes_only_from_a_version_tag(self) -> None:
        """Ensure a push to main never builds or uploads a package."""
        publish = _release_job("publish")

        assert publish["if"] == "startsWith(github.ref, 'refs/tags/v')"
        assert publish["environment"]["name"] == "pypi"

    def test_it_refuses_a_tag_that_names_another_version(self) -> None:
        """Ensure a hand-pushed `v1.2.3` on a commit at another version uploads nothing."""
        script = _release_script("publish")

        assert '"v${version}" != "$GITHUB_REF_NAME"' in script
        assert script.index("GITHUB_REF_NAME") < script.index("uv publish")

    def test_it_uploads_only_files_pypi_lacks(self) -> None:
        """Ensure a release never fails by uploading a file PyPI already has.

        PyPI refuses a file name it has seen, even with new content, and a
        rebuilt sdist rarely matches the uploaded one byte for byte. So the
        built files PyPI already lists are left out before uploading, and the
        upload is skipped when none are left.
        """
        steps = {step["name"]: step for step in _release_job("publish")["steps"]}
        names = list(steps)
        script = steps["Leave Out Files PyPI Already Has"]["run"]

        assert names.index("Build Sdist and Wheel") < names.index(
            "Leave Out Files PyPI Already Has"
        )
        assert names.index("Leave Out Files PyPI Already Has") < names.index("Publish to PyPI")
        assert '"https://pypi.org/pypi/veridelta/${version}/json"' in script
        assert 'rm "dist/${name}"' in script
        assert 'echo "files=${left}" >> "$GITHUB_OUTPUT"' in script
        assert steps["Publish to PyPI"]["if"] == "steps.upload.outputs.files != '0'"
        assert "uv publish --check-url https://pypi.org/simple/" in steps["Publish to PyPI"]["run"]

    def test_it_creates_the_release_page_after_publishing(self) -> None:
        """Ensure the GitHub Release appears only once the package is on PyPI."""
        release = _release_job("github-release")
        script = _release_script("github-release")

        assert release["needs"] == "publish"
        assert (
            'gh release create "$TAG" --verify-tag --generate-notes --title "$TAG" '
            '--latest="$latest"'
        ) in script
        assert 'gh release view "$TAG"' in script

    def test_it_marks_the_newest_version_latest(self) -> None:
        """Ensure the newest version's release is Latest, and only that one.

        Latest goes to the highest full version tag, so a rerun on an older
        tag cannot take it from a newer release, and an existing release of
        the newest version is marked Latest too.
        """
        script = _release_script("github-release")

        assert "matching-refs/tags/v" in script
        assert "grep -E '^v[0-9]+\\.[0-9]+\\.[0-9]+$'" in script
        assert "sort -V" in script
        assert 'gh release edit "$TAG" --latest' in script

    def test_each_job_gets_only_the_permissions_it_needs(self) -> None:
        """Ensure write access is granted per job, never to the whole workflow."""
        assert _release()["permissions"] == {"contents": "read"}
        assert _release_job("tag")["permissions"] == {"contents": "write", "actions": "write"}
        assert _release_job("publish")["permissions"] == {
            "id-token": "write",
            "contents": "read",
        }
        assert _release_job("github-release")["permissions"] == {"contents": "write"}


def _workflow(path: Path) -> dict[Any, Any]:
    """Load a workflow file."""
    loaded: dict[Any, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return loaded


def _workflow_steps(path: Path) -> list[dict[str, Any]]:
    """Return every step of every job in a workflow file."""
    loaded: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [step for job in loaded["jobs"].values() for step in job.get("steps", [])]


class TestWorkflowPins:
    """Pin the third-party actions every workflow runs, and keep the pins current."""

    def test_it_finds_the_workflows(self) -> None:
        """Ensure a moved workflows folder cannot silently skip every check below."""
        assert {path.name for path in _WORKFLOWS} >= {
            "ci.yml",
            "docs.yml",
            "release.yml",
            "rerun-dropped.yml",
        }

    @pytest.mark.parametrize("workflow", _WORKFLOWS, ids=lambda path: path.name)
    def test_it_never_expands_an_expression_inside_a_shell_script(self, workflow: Path) -> None:
        """Ensure values reach scripts through `env` only, so none can inject shell syntax.

        A branch name or a pull request title is chosen by whoever opens it, and
        `${{ }}` would paste it into the script before the shell parses it.
        """
        for name, job in _workflow(workflow)["jobs"].items():
            for step in job.get("steps", []):
                if "run" in step:
                    assert "${{" not in step["run"], (workflow.name, name, step["name"])

    @pytest.mark.parametrize("workflow", _WORKFLOWS, ids=lambda path: path.name)
    def test_it_pins_every_action_to_a_commit(self, workflow: Path) -> None:
        """Ensure a moved tag upstream cannot change what CI runs or what a release publishes.

        The release workflow holds `id-token: write` for PyPI, so an action it
        runs by tag could publish whatever that tag points at next.
        """
        for step in _workflow_steps(workflow):
            uses = step.get("uses")
            if uses is None or uses.startswith("./"):
                continue
            assert _COMMIT_PIN.fullmatch(uses), f"{workflow.name}: {uses}"

    @pytest.mark.parametrize("path", [*_WORKFLOWS, _ACTION], ids=lambda path: path.name)
    def test_it_names_the_release_behind_every_pin(self, path: Path) -> None:
        """Ensure each commit pin says which release it is, as Dependabot keeps it."""
        for line in path.read_text(encoding="utf-8").splitlines():
            if re.search(r"uses: " + _COMMIT_PIN.pattern, line):
                assert re.search(r"@[0-9a-f]{40} # v\d+(\.\d+)*$", line), line

    def test_dependabot_keeps_the_pins_and_the_lockfile_current(self) -> None:
        """Ensure Dependabot updates action pins and `uv.lock`, never the package's floors."""
        config: dict[str, Any] = yaml.safe_load(_DEPENDABOT.read_text(encoding="utf-8"))
        updates = {update["package-ecosystem"]: update for update in config["updates"]}

        assert config["version"] == 2
        assert set(updates) == {"github-actions", "uv"}
        assert all(update["directory"] == "/" for update in updates.values())
        assert all(update["schedule"]["interval"] == "weekly" for update in updates.values())
        # The floors in pyproject.toml are a promise to users; only the lockfile moves.
        assert updates["uv"]["versioning-strategy"] == "lockfile-only"


class TestDocsWorkflow:
    """Pin how the documentation site deploys."""

    def test_it_deploys_one_commit_at_a_time(self) -> None:
        """Ensure merges that land together cannot race to push `gh-pages`.

        Deploys queue instead of cancelling one another, so none stops partway
        and the newest commit on `main` deploys last.
        """
        workflow: dict[str, Any] = yaml.safe_load(_DOCS.read_text(encoding="utf-8"))

        assert workflow["concurrency"] == {"group": "docs-deploy", "cancel-in-progress": False}


class TestCIWorkflow:
    """Pin the safeguards that keep an unfinished or failed CI run from reading as green."""

    def test_one_check_passes_only_when_every_job_passes(self) -> None:
        """Ensure `CI Passed` waits for every job and fails unless each one succeeded.

        A ruleset on `main` requires this one check instead of each matrix job by
        name. So it covers every job, runs even after one fails, and counts a
        cancelled or skipped job as a failure. A skipped required check would
        read as passing, which is why it never uses a condition that can skip it.
        """
        jobs = _workflow(_CI)["jobs"]
        gate = jobs["ci-passed"]
        [step] = gate["steps"]

        assert gate["name"] == "CI Passed"
        assert gate["if"] == "always()"
        assert set(gate["needs"]) == set(jobs) - {"ci-passed"}
        assert step["env"] == {"RESULTS": "${{ join(needs.*.result, ' ') }}"}
        assert 'for result in $RESULTS; do\n  [ "$result" = success ] || exit 1' in step["run"]

    @pytest.mark.parametrize("workflow", [_CI, _DOCS, _RERUN], ids=lambda path: path.name)
    def test_every_job_has_a_time_limit(self, workflow: Path) -> None:
        """Ensure a hung job fails within minutes, not after GitHub's six-hour default.

        The slowest job takes about two minutes. The release workflow keeps the
        default, since its publishing job waits for a person to approve it.
        """
        for name, job in _workflow(workflow)["jobs"].items():
            assert 1 <= job.get("timeout-minutes", 0) <= 15, (workflow.name, name)


def _rerun_script() -> str:
    """Return the shell script of the re-run workflow's one step."""
    [step] = _workflow(_RERUN)["jobs"]["rerun"]["steps"]
    script: str = step["run"]
    return script


class TestRerunDroppedJobs:
    """Pin the workflow that re-runs CI when GitHub never started some of its jobs.

    GitHub sometimes never assigns a runner to a queued job and cancels it after
    15 minutes. No test ran, so a re-run is safe. Any other failure stays red,
    so a test that fails is never retried until it passes.
    """

    def test_it_runs_after_each_ci_run(self) -> None:
        """Ensure it follows the CI workflow by its name, which a rename would break."""
        workflow = _workflow(_RERUN)
        triggers = workflow[True] if True in workflow else workflow["on"]

        assert triggers == {
            "workflow_run": {"workflows": [_workflow(_CI)["name"]], "types": ["completed"]}
        }

    def test_it_acts_only_on_a_failed_run_and_at_most_three_times(self) -> None:
        """Ensure a cancelled run is left alone and a run that keeps failing stops."""
        assert _workflow(_RERUN)["jobs"]["rerun"]["if"] == (
            "github.event.workflow_run.conclusion == 'failure' "
            "&& github.event.workflow_run.run_attempt < 4"
        )

    def test_it_gets_only_the_permissions_it_needs(self) -> None:
        """Ensure it can re-run jobs and read why they failed, and nothing else."""
        workflow = _workflow(_RERUN)

        assert workflow["permissions"] == {}
        assert workflow["jobs"]["rerun"]["permissions"] == {"actions": "write", "checks": "read"}

    def test_it_reruns_only_when_every_failed_job_never_started(self) -> None:
        """Ensure one job that failed after it started keeps the whole run red.

        `CI Passed` fails whenever another job fails, so it counts only when
        GitHub never started it either.
        """
        script = _rerun_script()
        gate = _workflow(_CI)["jobs"]["ci-passed"]["name"]

        assert (
            f'select(.conclusion == "cancelled" or (.conclusion == "failure" and .name != "{gate}"))'
        ) in script
        assert '*"was not acquired by Runner"*) ;;' in script
        assert script.index("was not acquired") < script.index("rerun-failed-jobs")

    def test_it_never_reruns_a_run_a_newer_commit_replaced(self) -> None:
        """Ensure an old run is not re-run, which would cancel the newer run on its branch."""
        script = _rerun_script()

        assert '[ "$newest" != "$RUN_ID" ]' in script
        assert script.index('"$newest" != "$RUN_ID"') < script.index("rerun-failed-jobs")
