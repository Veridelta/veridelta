---
type: Decision
title: The MCP server refuses a Delta Lake table in WebHDFS
description: Why veridelta mcp refuses a webhdfs:// or opendal+webhdfs:// table_uri whatever its host, chosen over passing the URI's host to deltalake as the store's endpoint and holding it to --allow-host.
status: stable
decided: 2026-11-19
generated: { by: claude-code, at: 2026-11-19T06:10:00Z }
---

**Claim:** On `veridelta mcp`, `_refuse_webhdfs` refuses a Delta Lake side whose `table_uri` has a scheme in `_WEBHDFS_SCHEMES`, `webhdfs` or `opendal+webhdfs` as `urlsplit` reads it, whatever host it names, before any reader connects. `veridelta run` reads such a table as before.

**Evidence:** deltalake 1.6.6 reads both schemes through OpenDAL's WebHDFS store, `opendal-service-webhdfs` 0.56.0. Its `WebhdfsConfig` takes the namenode from `endpoint` alone, which deltalake fills from the `opendal.endpoint` storage option. With none, `build` uses `WEBHDFS_DEFAULT_ENDPOINT`, `http://127.0.0.1:9870`. The store's `from_uri`, which would read the URI's host, is not called. A probe read `webhdfs://lake.example.com/t`, `opendal+webhdfs://lake.example.com/t`, and the same with a port: each sent `GETFILESTATUS`, a `PUT` of `MKDIRS`, and `LISTSTATUS_BATCH` to a listener on `127.0.0.1:9870`, and with `opendal.endpoint` set, to the endpoint instead. The store sends a file's `OPEN` with no `noredirect`, so reqwest follows the namenode's redirect to a datanode at whatever host it names. The same probe read each of the 43 schemes deltalake 1.6.6 lists other than `file`, `fs`, and `opendal+fs`, at a host other than this machine, with no storage option or credential, through a proxy on this machine that every proxy variable named. Only the two reached a host other than its URI's or a service's own, such as S3 or a cloud's metadata server. Most of the rest stopped before any request, for want of an option or a credential, so the probe does not show where they go once given one. `tests/integration/test_mcp_webhdfs.py` pins both halves: the tool refuses the side with no connection to that proxy, and its control, with the rule off, sends the reader's requests to `127.0.0.1:9870`.

**Alternative considered:** Pass the URI's host to deltalake as `opendal.endpoint`, with `https://` before it, so the reader reaches the host `--allow-host` checked.

**Why rejected:** The endpoint pins the namenode only. The namenode answers each file's read with a redirect to a datanode, usually another host on plain HTTP, which reqwest follows with no check, so a server that allowed one host would still read from any host its namenode names. Passing the option would also mean taking `opendal.` options on the server, which it now refuses, and changing what `veridelta run` reads.

**How to reverse:** Remove `_WEBHDFS_SCHEMES`, `_refuse_webhdfs`, and its call in `_refuse_hosts` in `src/veridelta/mcp_server.py`, with `TestWebHdfsTables` in `tests/unit/test_mcp_server.py` and `tests/integration/test_mcp_webhdfs.py`, and the sentences about it in `docs/agents.md`, `docs/upgrading.md`, and `rules/security.md`. Do so only once the reader holds each redirect to the allowed hosts too.
