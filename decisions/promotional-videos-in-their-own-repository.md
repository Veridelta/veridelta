---
type: Decision
title: Promotional videos live in their own repository
description: Why the promotional videos are made in Veridelta/veridelta-media with Remotion, while the terminal recordings they show stay here, where CI runs their commands.
status: stable
decided: 2026-10-07
generated: { by: claude-code, at: 2026-10-07T09:05:00Z }
---

**Claim:** The promotional videos, their storyboard, and their launch texts live in `Veridelta/veridelta-media`, a Remotion project, while every terminal recording they show stays in this repository as a tape in `demo/`, and the media repository renders its clips from a pinned Veridelta tag with `make demo-video`.

**Evidence:** The maintainer chose a new repository for promotional media on 2026-10-07. `tests/unit/test_demo_tape.py` runs every tape's commands here on each pull request, so a clip rendered from a tagged tape shows output the CLI printed at that release. `make demo-video` writes each recording as an MP4 to `demo/video/`, which git ignores, for the media repository to fetch.

**Alternative considered:** A `media/` folder in this repository, with the Remotion project beside the tapes, so one pull request changes a feature and its video.

**Why rejected:** It would add Node, a lockfile Dependabot watches, a license check for Remotion, and rendered video to a Python package's repository, and every pull request would build or skip them. The videos change on a launch's schedule, not on each pull request, and a pinned tag keeps every frame traceable to a tape without that cost.

**How to reverse:** Move the media repository's project into `media/` here, add its type check and preview render as a CI job, point its clip script at `make demo-video` in the same checkout, and archive `Veridelta/veridelta-media`.
