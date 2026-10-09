---
name: record-demo
description: Records, re-records, or adds a terminal demo under demo/ with vhs, and renders the report screenshots and the link preview card. Use when a command a tape types, or what it prints, changes; when a feature needs a recording; or when promotional clips are needed. Never for an edited or staged recording.
metadata:
  version: "1.3.0"
---

# Record a demo

Each recording is a tape in `demo/`, typed at real speed by vhs into a GIF under `docs/assets/`. A recording is evidence that the product does what the docs say, so it is held to the same standard as a test.

## When to record again

- A command a tape types changes its flags or its output. `tests/unit/test_demo_tape.py` fails on the transcript first; re-render the GIF in the same change.
- The run summary, the progress lines, or an error a tape shows changes.
- The HTML report or the summary sentence changes: run `make screenshots`.

## Adding a tape

1. Write `demo/<name>.tape`. It sets `Output ../docs/assets/demo-<name>.gif`, then `Require veridelta`, then `Source settings.tape`, then its own `Set Height`.
2. Type only commands the test can run: `cat`, `head -n N`, `veridelta`, `echo`, `python <script>` for a script in `demo/`, and `#` comments that label the screen.
3. Put the files it reads in `demo/`. Keep them small and made up.
4. Write `demo/<name>.txt`, what the terminal shows, by running the tape's commands as the test does.
5. Show the recording on its docs page in a closed `<details markdown>` block, with alt text that says what happens and the transcript as a `text` code block.
6. Add the page to `_EMBEDDED_IN` in `tests/unit/test_demo_tape.py`.

## Rendering

- `make demo` renders every tape, through `uv run`, with the vhs release the `Makefile` pins. It refuses any other and prints the install steps.
- `make demo-video` writes an MP4 of each recording to `demo/video/`, which git ignores, for promotional videos.
- `make screenshots` captures the HTML report in light and dark and renders the link preview card, with the `accessibility` group's Playwright.

Commit each GIF or PNG with the transcript or the change that it shows.

## Tapes for video

`demo/promo/` holds tapes made for promotional video alone, in a 32 pixel font, a few lines to a tape, so the text stays legible when a video shrinks to a phone:

- `data` and `run` type the quick start's commands.
- `mcp` runs the MCP client with `--brief`, which prints each call and answer as YAML, one field to a line, and still prints every row an answer returns.
- The `accounts-` tapes follow the "From drift to rules" guide, one step to a tape: the two files, the first run on two files and `--key`, `suggest`, `crosswalk`, the run with four rules, the run with a baseline, and the same run on the fixed export. They read `demo/accounts_legacy.csv` and `demo/accounts_rewrite.csv`, the guide's data, with the configurations and the baseline beside them. `demo/accounts_fixed.csv` is the rewrite with account 17's region corrected, the one defect the guide finds, and `demo/accounts_fixed.yaml` checks it by the same rules.

Each writes `video/promo-<name>.mp4` itself, and `make demo-video` renders them after the others. It then runs `demo/screenshots.py --promo`, which writes `video/promo-report.png`, the HTML report of the run `accounts-baseline` types. No docs page shows them, but the test holds each tape to the CLI and to its transcript, `demo/promo/<name>.txt`, as it does every tape.

## Promotional clips

Promotional videos live in their own repository, which renders its clips from a pinned Veridelta release with `make demo-video`, so every terminal frame traces to a tape here. To pace a terminal with a voice, it may set a tape's transcript in its own type instead. It then shows the transcript's text unchanged, in the tape's order, and names the tape on screen.

## The honesty rules

- Real commands on real files. Nothing typed is faked, and no output is pasted in.
- No edited frames: re-render instead of cutting.
- Every number on screen comes from the run the recording shows.
- No caption, label, or alt text claims more than the docs say.
- A script that stands in for an agent says so on screen, as `demo/mcp.tape` does. A real agent's session is recorded whole, with the kit in `demo/agent/`.
