---
type: Decision
title: The MCP server connects only to the hosts its operator allows
description: Why veridelta mcp refuses a side whose host is not given with --allow-host, chosen over blocking private addresses or limiting ${NAME} to credential fields.
status: stable
decided: 2026-11-14
generated: { by: claude-code, at: 2026-11-14T23:05:58Z }
---

**Claim:** `veridelta mcp` refuses, before it connects, a side that names a host not given with `--allow-host`: a URL's host, a bucket, a database server, a Snowflake account's host, a Databricks workspace, or a host in `storage_options`. With no `--allow-host`, a tool reads only files under the roots, BigQuery, MotherDuck, and Hugging Face. The command line is unchanged.

**Evidence:** `_destinations` and `_refuse_hosts` in `src/veridelta/mcp_server.py`, called from `_load` and from `check_configuration` with `schemas`. `TestAllowedHosts` in `tests/unit/test_mcp_server.py` refuses each kind of host, including a loopback URL, a link-local address, a second Postgres host, a `host=` query parameter, and an authority with a backslash. Before the change, a configuration under a root with `path: http://127.0.0.1:8799/x.csv?t=${DEMO_TOKEN}` made `describe_schema` send the token to that server, and a Postgres `uri` sent it to a host the configuration named. Masking kept it out of the answer only.

**Alternative considered:** Resolve each host and refuse loopback, link-local, and private addresses, and expand `${NAME}` only in credential fields such as `password` and `access_token`.

**Why rejected:** A public host the agent chooses still receives a credential field during login, so the secret leaves anyway. Resolving before the driver connects is open to DNS rebinding, and object store options such as an endpoint or a proxy would need their own resolution. An allowlist compares names the operator wrote, and no configuration can widen it.

**How to reverse:** Remove the `_refuse_hosts` calls in `src/veridelta/mcp_server.py`, the `--allow-host` flag in `src/veridelta/cli.py`, and `TestAllowedHosts`, and restore the sentence in `docs/agents.md` that remote data is read as the command line reads it.
