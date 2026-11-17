---
type: Decision
title: The MCP server takes a ${NAME} only as the whole value of a credential setting
description: Why veridelta mcp refuses a ${NAME} outside a credential setting at expansion and masks every value it expanded, chosen over withholding reader messages or masking pieces of values.
status: stable
decided: 2026-11-17
generated: { by: claude-code, at: 2026-11-17T10:37:44Z }
---

**Claim:** On `veridelta mcp`, a `${NAME}` may stand only as the whole value of a credential setting, or as a database uri's whole user or whole password, in that setting's shape, and is refused as it is expanded, before any check or reader. Every answer is masked, in any case and percent-encoded too, against every value the server has expanded since it started.

**Evidence:** Through the MCP client on origin/main, a value came back unmasked in each of these forms: percent-encoded in a path (`%2B`, `%25`), named through a YAML escape (`\x24{KEY}`), cut by PyIceberg's `parse_location` at `#` or `?`, from Polars' cache of object stores in a later call that never named the variable, cut at a scheme's `:` (`sqlite+${KEY}://x.db`) or at a login's `:` in a uri's user, as the head of a whole `DATABASE_URL`, re-sorted by the Azure SDK from a SAS token written as `"?${SAS}"`, and swapped out of the file between the load and the second read that `environment_values` made. A reader option such as `new_columns`, `row_index_name`, or `include_file_paths`, a `query`, and a glob's brackets also carry a value into a column name or a count. A prototype that refuses at `config._substitute` refused every one of these configurations and accepted the credential forms. `TestEnvironmentReferences` in `tests/unit/test_mcp_server.py` holds a test for each form, a check that every field outside the slot table is refused, a check that the server loads only through `_load_checked`, and a corpus of credential values echoed raw, lowercased, and percent-encoded by every engine call, of which no six characters in a row reach any answer of any tool. `TestReferenceHook` in `tests/unit/test_config.py` covers the hook.

**Alternative considered:** Withhold every reader and driver message once any configuration in the process has expanded a variable, with an allowlist of messages and a mark on each setting a message cites. A second alternative masks each piece of a value and its decoded views.

**Why rejected:** Withholding makes the server unable to say why any credentialed read failed, even with `--allow-row-values`, and needs 60 to 80 hand-placed marks where one wrong mark leaks. Piece masking still matches patterns: it leaves cuts at other characters and other encodings open, garbles answers across configurations, and lets an agent guess a value piece by piece.

**How to reverse:** Pass `on_reference=None` in `_load_checked` in `src/veridelta/mcp_server.py`, delete the refusal tests in `TestEnvironmentReferences`, and restore the list of values that come back unmasked in `docs/agents.md`. The mask over recorded values keeps its protection. A narrower step adds a `--public-env NAME` flag that skips the slot table for a variable the operator declares holds no secret.
