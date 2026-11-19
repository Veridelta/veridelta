---
type: Decision
title: A ClickHouse password goes into the URI unencoded, and one ConnectorX would encode is refused
description: Why connection_uri writes a ClickHouse password into the URI as written and refuses one with a character ConnectorX's URL parser encodes, chosen over sending it encoded or reading ClickHouse another way.
status: stable
decided: 2026-11-19
generated: { by: claude-code, at: 2026-11-19T07:05:00Z }
---

**Claim:** `connection_uri` in `src/veridelta/connectors/database.py` writes a ClickHouse password into the URI's login as written: the `password` setting, or the URI's own password decoded. It refuses, with a `ConfigError` before any connection, a password with a character outside `_CLICKHOUSE_PASSWORD`: letters, digits, and `! $ % & ' ( ) * + , . _ ~ -`.

**Evidence:** ConnectorX 0.4.6 parses a ClickHouse URI with Rust's URL parser and sends `url.password()` in the `X-ClickHouse-Key` header without decoding it. Against the fake ClickHouse in `tests/integration/test_mcp_password_echo.py`, the URI password `p%40ss%2Fw%2Brd%3D%25` arrived as `p%40ss%2Fw%2Brd%3D%25`. Raw `a+b`, `a%b`, `a%41b`, and `a!*()'~$&,b` arrived as written. Raw `p@ss`, `a=b`, `a;b`, `a:b`, `a b`, `a[b]`, and `é` arrived as `p%40ss`, `a%3Db`, `a%3Bb`, `a%3Ab`, `a%20b`, `a%5Bb%5D`, and `%C3%A9`: the parser encodes them in a URI's password, raw or not. `test_clickhouse_receives_a_password_with_symbols_as_written` now drives `veridelta run`'s connector, with the password in the setting and encoded in the URI, and `veridelta mcp`, with it from the environment, and the fake receives exactly the password each time. Against a live `clickhouse-server:26.8`, a user with the password `Vd!$%&'()*+,._~-%2B1` reads its table through the `password` setting and through the URI with the password percent-encoded. Before this change both reads failed with `Authentication failed`. The Database Servers CI job runs that check, `test_it_signs_in_to_clickhouse_with_each_symbol_a_password_takes` in `tests/integration/test_database_servers.py`.

**Alternative considered:** Keep percent-encoding every password into the URI, as for the other readers, and document that a ClickHouse password holds only letters, digits, `-`, `.`, `_`, and `~`.

**Why rejected:** ClickHouse then refuses a password with `+`, `!`, or `%`, which ConnectorX can send whole, and a URI password a user percent-encodes as the docs say. A sign-in failure also names no cause, while the refusal names the characters to use. No URI form carries `@`, `/`, `=`, a space, or a letter outside ASCII through that parser, so no change in Veridelta can sign in with one.

**How to reverse:** Remove `_clickhouse_uri`, `_CLICKHOUSE_PASSWORD`, and the ClickHouse branch of `connection_uri`, with their tests in `tests/unit/test_connectors.py`, `CLICKHOUSE_CORPUS` in `tests/integration/password_corpus.py`, the ClickHouse cases of `tests/integration/test_mcp_password_echo.py`, and the two ClickHouse password tests of `tests/integration/test_database_servers.py`. Then restore the ClickHouse paragraph of `docs/sources.md`.
