# Recording an agent

This folder is a kit for recording a real agent that compares two datasets through `veridelta mcp`. A recording made with it shows the agent as it is: the prompt, the tools it calls, and its answer, with nothing edited.

## What is here

- `orders_before.csv` and `orders_after.csv`: twelve orders exported before and after a pipeline change. Two orders changed status, one amount moved by a cent, and one order is new.
- `veridelta.yaml`: compares the two files on `order_id`, with no rules.
- `PROMPT.md`: the prompt to paste.

## Recording

1. Copy this folder outside the repository, so the agent sees only these files.
2. In the copy, register the server with Claude Code. Row values are allowed, so the agent can read the rows that differ:

    ```bash
    claude mcp add veridelta -- uvx --from 'veridelta[mcp]' veridelta mcp --root . --allow-row-values
    ```

3. Open Claude Code in the copy, start a screen recorder, paste the prompt from `PROMPT.md`, and let the agent finish.
4. Stop the recording, and keep it whole. Cut no frame, and add no caption that claims more than the docs.

Another agent host works too: give it the command after `--`, as its documentation says.
