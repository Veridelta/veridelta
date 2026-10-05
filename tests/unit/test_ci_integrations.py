# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Static checks for the GitHub Action, the GitLab CI template, and the release workflow.

None of them can run inside the test suite, so these pin the contracts a typo
would break: every input a step reads is declared, no input is expanded inside a
shell script, third-party actions are pinned, the `veridelta run` command line
they build still parses with the CLI's own parser, and a release publishes only
a new version, only from its tag, with no more permission than each job needs.
"""

import re
import shlex
from pathlib import Path
from typing import Any

import pytest
import yaml

from veridelta import __version__
from veridelta.cli import build_parser

_ROOT = Path(__file__).resolve().parents[2]
_ACTION = _ROOT / "action.yml"
_GITLAB = _ROOT / "ci" / "gitlab" / "veridelta.yml"
_RELEASE = _ROOT / ".github" / "workflows" / "release.yml"


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


@pytest.mark.unit
@pytest.mark.fast
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


@pytest.mark.unit
@pytest.mark.fast
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


@pytest.mark.unit
@pytest.mark.fast
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

    def test_it_never_expands_an_expression_inside_a_shell_script(self) -> None:
        """Ensure values reach scripts through `env` only, so none can inject shell syntax."""
        for name, job in _release()["jobs"].items():
            for step in job["steps"]:
                if "run" in step:
                    assert "${{" not in step["run"], (name, step["name"])
