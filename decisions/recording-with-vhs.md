---
type: Decision
title: The quick start is recorded with vhs
description: Why the README's GIF and the docs home's video render from a vhs tape in the repository, not from an asciinema cast or a video pipeline.
status: stable
decided: 2026-10-06
generated: { by: claude-code, at: 2026-10-06T20:55:00Z }
---

**Claim:** `demo/veridelta.tape` is the one source of the recording: `make demo` renders `docs/assets/demo.gif` for the README and `docs/assets/demo.mp4` for the docs home from it with vhs, and a unit test runs the tape's commands on every pull request.

**Evidence:** The tape types four commands at real speed on the CI fixtures copied into `demo/`. `tests/unit/test_demo_tape.py` parses each `veridelta` line with the CLI's parser, runs the commands and holds the transcript on `docs/index.md` to what they print, and holds the cues in `docs/assets/demo.vtt` and the video's length to the tape's typing speed and pauses. The video plays without a script, and the transcript is plain text.

**Alternative considered:** An asciinema cast, played on the docs home with the asciinema player and shown in the README as an SVG rendered from the cast.

**Why rejected:** The player shows the cast only with JavaScript, from a CDN that `tests/accessibility/test_accessibility.py` blocks or as script vendored into a site that loads one file of it, and a cast is a timed log of bytes that no test can replay through the CLI. A video pipeline such as Remotion was the other option, and it needs Node, Chrome, and a storyboard to render thirty seconds of terminal text.

**How to reverse:** Replace the `demo` target in the `Makefile` and the tape's `Output` lines with the new recorder's command, re-render `docs/assets/demo.gif` and `docs/assets/demo.mp4`, and point `_typed()` in `tests/unit/test_demo_tape.py` at the new format's commands.
