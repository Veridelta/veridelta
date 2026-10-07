---
type: Decision
title: The demos are recorded with vhs
description: Why each terminal recording renders from a vhs tape in the repository, with the vhs release the Makefile pins, not from an asciinema cast or a video pipeline.
status: stable
decided: 2026-10-06
generated: { by: claude-code, at: 2026-10-07T09:05:00Z }
---

**Claim:** Each tape in `demo/` is the one source of its recording: `make demo` renders its GIF under `docs/assets/` with the vhs release the `Makefile` pins, and a unit test runs every tape's commands on every pull request. The quick start's GIF is the one the README and the docs home embed.

**Evidence:** Each tape types its commands at real speed on files in `demo/`; the quick start's are the CI fixtures. `tests/unit/test_demo_tape.py` parses each `veridelta` line with the CLI's parser, runs every tape's commands, and holds `demo/<tape>.txt`, the text each recording shows, to what they print. `make demo` checks `vhs --version` against `VHS_VERSION` before it renders. The image needs no script.

**Alternative considered:** An asciinema cast, played on the docs home with the asciinema player and shown in the README as an SVG rendered from the cast.

**Why rejected:** The player shows the cast only with JavaScript, from a CDN that `tests/accessibility/test_accessibility.py` blocks or as script vendored into a site that loads one file of it, and a cast is a timed log of bytes that no test can replay through the CLI. A video pipeline such as Remotion was the other option, and it needs Node, Chrome, and a storyboard to render thirty seconds of terminal text; promotional videos use one, in their own repository.

**How to reverse:** Replace the `demo` and `demo-video` targets in the `Makefile` and each tape's `Output` line with the new recorder's command, re-render the GIFs under `docs/assets/`, and point `_typed()` in `tests/unit/test_demo_tape.py` at the new format's commands.
