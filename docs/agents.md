# AI agents

An AI agent, such as a coding assistant, runs Veridelta through its command line or through its MCP server. This page gives the steps that keep its runs predictable and its replies free of row values.

## Steps

1. Check the configuration file. `validate` reads no rows, and by default connects to nothing:

    ```bash
    veridelta validate -c veridelta.yaml --json
    ```

    The JSON holds the `config` it checked, `valid`, `errors`, `warnings`, and `findings`, which place each error and warning in the file by `path`, or by `line` and `column` counted from 1. The command exits `1` when there is an error. Fix each error before a run. A check that cannot finish exits `3` and prints an `error` object instead. `--allow-missing-env` checks a file whose secrets are not set, such as in a pull request job.

2. Run the comparison with `--json`, so stdout carries only the [summary](results.md#summary):

    ```bash
    veridelta run -c veridelta.yaml --json
    ```

3. Read the run's [exit code](cli.md#exit-codes) before its output:

    | Code | Meaning | stdout |
    | :--- | :--- | :--- |
    | `0` | A match within `threshold`. | The summary. |
    | `1` | Drift. | The summary. |
    | `2` | Invalid arguments. | Nothing. |
    | `3` | The run could not finish. | One object, `{"error": {"type": ..., "message": ...}}`, which stderr explains too. |

    Read `type` before acting on `message`. A `ConfigError` means the configuration file needs a fix, and [Exit codes](cli.md#exit-codes) says what the other types mean. Any type that is not a Veridelta error, such as one from a driver, is worth reporting to the user as a possible bug.

4. Report counts and column names, which is all the summary holds. Leave row values out of a reply unless the user asks for them.

5. When two columns hold the same values in different encodings, such as `Y` and `true`, propose a `value_map` from the data:

    ```bash
    veridelta crosswalk -c veridelta.yaml --json
    ```

    Show the proposals and their evidence to the user before adding them to the configuration.

6. When a column differs by small amounts, such as rounding, padding, case, a date format, or a spelling of NULL, suggest a rule from the data:

    ```bash
    veridelta suggest -c veridelta.yaml --json
    ```

    Each suggestion names the rows it explains, and its example keys come from the data. Show the suggestions to the user before adding a rule: a rule forgives what it explains in later runs too, such as every gap below a tolerance.

## MCP server

`veridelta mcp` serves steps 1, 2, 5, and 6 above as [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) tools, so an agent's host can call them without a shell. Two more tools list a side's columns and read the rows that differ. The server needs the `mcp` extra:

```bash
uv add 'veridelta[mcp]'
```

Register the server with the host from the folder that holds the configuration files. In Claude Code, this command does it:

```bash
claude mcp add veridelta -- uv run veridelta mcp --root .
```

A host that reads its servers from a file takes the same command as an entry. Claude Code reads `.mcp.json` at the project's root, and Cursor reads `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "veridelta": {
      "command": "uv",
      "args": ["run", "veridelta", "mcp", "--root", "."]
    }
  }
}
```

In VS Code, [the extension](configuration.md#the-vs-code-extension) registers the server with agent mode. It passes one `--root` per workspace folder, runs the server in the first, and turns the settings `veridelta.mcp.allowRowValues`, `veridelta.mcp.allowQueries`, and `veridelta.mcp.maxRows` into its flags. Without the extension, `.vscode/mcp.json` takes the same command, with the workspace folder as the root:

```json
{
  "servers": {
    "veridelta": {
      "type": "stdio",
      "command": "uv",
      "args": ["run", "veridelta", "mcp", "--root", "${workspaceFolder}"],
      "cwd": "${workspaceFolder}"
    }
  }
}
```

Where Veridelta is installed without uv, the command is `veridelta mcp` itself. To let the agent read rows, add `--allow-row-values` to the command. To let it read data on another machine, such as a database server or a bucket, add `--allow-host` for each host, in lowercase. The server has six tools. Each takes `path`, the configuration file, and resolves a relative path against the first root:

| Tool | Returns | Reads |
| :--- | :--- | :--- |
| `validate_config` | What `veridelta validate --json` prints: `config`, `valid`, `errors`, `warnings`, and `findings`, which place each one in the file. Its `schemas` and `allow_missing_env` arguments work as the command's flags do. | No rows. With `schemas`, each side's columns. |
| `run_comparison` | The [summary](results.md#summary) that `veridelta run --json` prints, with three more fields: `verdict`, which is `match` or `drift`; `exit_code`, which is `0` or `1` as the command exits; and `artifacts_written`. | Both sides. |
| `describe_schema` | One side's `columns`, each name as stored mapped to its type as Polars names it, such as `Int64`, and the `side`. Its `side` argument is `source` or `target`. | That side's columns, and no rows. A side that reads a `query` is refused, since only running it would name its columns. |
| `read_discrepancies` | The rows of one `kind`, `added`, `removed`, or `changed`, up to `limit`, 20 by default: `kind`, `total`, `rows`, `truncated`, and `keys_only`, which is true when the pair was compared in place, so each row holds its primary key alone. | Both sides, since each call runs the comparison. Only on a server started with `--allow-row-values`. |
| `propose_value_maps` | What `veridelta crosswalk --json` prints, as `proposals`, with `total` and `truncated`. Its `min_confidence`, `min_support`, and `sample_fraction` arguments work as the command's flags do. | Both sides. Only on a server started with `--allow-row-values`. |
| `suggest_rules` | What `veridelta suggest --json` prints, as `suggestions`, with `total` and `truncated`. Its `max_share` argument works as the command's `--max-share` does. Each suggestion's `examples` hold primary keys from the data. | Both sides, read locally, so a pair compared in place is refused. Only on a server started with `--allow-row-values`. |

<details markdown>
<summary>Watch a client call three tools, as an agent's host does</summary>

![A terminal runs a script that starts veridelta mcp with row values allowed and calls three tools. validate_config answers valid with no errors. run_comparison answers drift, exit code 1, with one added, one removed, and one changed row, and one mismatch in status. read_discrepancies returns the changed row, id 2, whose status is closed in the source and shipped in the target.](assets/demo-mcp.gif)

```text
> # A client calls the MCP server's tools, as an agent's host does.
> python mcp_client.py
-> validate_config {"path": "veridelta.yaml"}
<- {
  "valid": true,
  "errors": [],
  "warnings": []
}
-> run_comparison {"path": "veridelta.yaml"}
<- {
  "verdict": "drift",
  "exit_code": 1,
  "added_count": 1,
  "removed_count": 1,
  "changed_count": 1,
  "column_mismatches": {
    "status": 1
  }
}
-> read_discrepancies {"path": "veridelta.yaml", "kind": "changed"}
<- {
  "total": 1,
  "truncated": false,
  "rows": [
    {
      "id": 2,
      "status_source": "closed",
      "amount_source": 20.5,
      "status_target": "shipped",
      "amount_target": 20.5,
      "status_is_match": false,
      "amount_is_match": true
    }
  ]
}
```

</details>

<details markdown>
<summary>Watch an agent compare two exports in Claude Code</summary>

A real Claude Code session, recorded with the kit in [`demo/agent/`](https://github.com/Veridelta/veridelta/tree/main/demo/agent) and the server started with `--allow-row-values`. It runs unedited from the prompt to the agent's first answer. Two header lines, the model and the plan, are removed, and the last frame is held so the answer can be read.

![A Claude Code session. The user pastes a prompt asking the agent to check the configuration, run the comparison, read the rows that differ, and say whether any change looks like rounding, without changing a file. The agent calls the veridelta tools six times and runs three shell commands. Its answer: the configuration is valid and matches rows on order_id with no rules; the run is drift, 12 orders before and 13 after, with 4 issues. Status changed on orders 1004, shipped to cancelled, and 1009, open to cancelled. Amount changed on order 1006, 58.50 to 58.51. Order 1013 is new. Only the one-cent change could be rounding, and a tolerance rule would forgive it, which the agent leaves to the user.](assets/demo-agent-session.gif)

```text
> Compare the two order exports in this folder with the veridelta tools. Check the
  configuration first, then run the comparison, then read the rows that differ. Tell me
  what changed, column by column, and whether any change looks like rounding rather than
  a real difference. Do not change any file.

  Searched for 1 pattern, called veridelta 6 times, ran 3 shell commands

I've finished the comparison. The after export has 1 new order and 3 changed orders, and
no orders were removed. I didn't change any files.

Configuration: veridelta.yaml checked out with no errors or warnings. It matches rows on
order_id and has no tolerance or other rules. Both files have the same three columns:
order_id (integer), status (text) and amount (decimal).

Run: the result was "drift", with 12 orders before and 13 after. The match rate is
66.67%, with 4 issues in total.

Changes by column:

| Column      | Mismatches | Orders | Before -> after     |
| status      | 2          | 1004   | shipped -> cancelled |
|             |            | 1009   | open -> cancelled    |
| amount      | 1          | 1006   | 58.50 -> 58.51       |
| (new order) | -          | 1013   | open, 99.00          |

In the changed orders, every column other than the one listed stayed the same. I checked
these rows against the CSV files directly and they match what the tool reported.

Rounding or a real difference?
- The two status changes are real. Text values can't be rounding, and both orders were
  cancelled. Order 1004 was already shipped, so that cancellation may be worth asking
  about.
- The amount change on order 1006 is the only one that could be rounding. It's a single
  cent, and both files store the amount to 2 decimal places, so it isn't one export
  cutting off digits. The likeliest rounding explanation is two systems rounding
  something like 58.505 in different directions. But the files can't tell that apart
  from a real one-cent price change. Whoever owns the source system would have to
  confirm it.
- The new order 1013 is a real difference, since it only exists in the after file.

The configuration has no tolerance for amount, so even a one-cent difference counts as
drift. If you decide amount differences up to one cent are acceptable, a tolerance rule
would make order 1006 pass. That's your decision to make; I haven't added the rule.
```

</details>

The person who starts the server decides what it may read, and no tool call can change that:

- Each `--root` names a folder the tools may read configuration files from, and a path outside every root fails the call. The server runs in the first root, so a relative path resolves there.
- A tool that opens a side reads files on this machine only from under the roots, after it follows links, since a column name or an error can carry a file's text as a row does. It expands a leading `~/` in a file path to the home folder, as the readers do, and reads the rest as written, so `./~/x` names a folder `~` in the working directory. A Snowflake `private_key_path` is read as its driver reads it instead, as a bullet below says. It reads a `sqlite://` path as the driver does: decoded, with no `~` expanded, and from the working directory when relative. It follows the links in that path, so a `..` after a link climbs from where the link leads, and the check and the driver read that one path. It refuses any other URI the driver reads as SQLite, such as one whose scheme is `sqlite+x`. A `path` with `*`, `?`, or `[` in it is a pattern that Polars expands, so the tool lists the files it matches, hidden ones included, and refuses the side unless each resolves under a root. Such a pattern may not hold `..`, nor a name with a glob character that starts with a dot, such as `.*`, since Polars lets that name match `..`. A `file:` URI may not hold a pattern at all, since Polars expands one with another matcher, so write it as a plain path. PyArrow drops a folder and the `..` after it before it follows a link, where the system follows the link first, so the check takes both readings. On Windows, a path that starts with two separators of either kind, such as `\\host\share\x.csv`, `//host/share/x.csv`, or `\\?\UNC\host\share\x.csv`, or with `\??\`, names another machine or a device, and resolving it reaches that machine. So a tool refuses such a path before anything resolves it, whether a tool call, a `path`, an `output_path`, a Snowflake `private_key_path`, a `sqlite://` path such as `sqlite:////host/share/x.db`, or a file a table lists names it. Cygwin and MSYS read such a path as Windows does. A long path to a file on this machine, such as `\\?\C:\data\x.csv`, is refused too, even under a root, so write it without `\\?\`. Windows follows a directory junction, a drive that `subst` makes, and a short name such as `PROGRA~1` as it resolves a path, so a tool checks the folder each leads to. It also strips a trailing dot or space from a name, as in `x.csv.`, and reads `C:x.csv` from the working directory on drive `C:`. On Linux and macOS, a path that starts with `//` is a path on this machine, and is checked as one. Data elsewhere, such as an object store or a warehouse, is held to the hosts the server allows, as the bullets below say.
- A location with a scheme, such as `s3://b/t`, is also checked as a path from the working directory, since a reader that does not know its scheme opens it here. Polars opens `s3:x/a.parquet`, with no `//`, and `md://x/a.parquet` that way, and DuckDB opens a `database` with a scheme it does not know, such as `lakefs://r/x.duckdb`, that way. A `file:`, `fs:`, or `opendal+fs:` location's path is checked too, under a root however a reader parses it. Polars and deltalake read `file:tmp/x` as `/tmp/x`, and PyIceberg as `tmp/x` in the working directory. A `file:` location may name no host but `localhost`, since deltalake opens `file://host/t` on that host on Windows, so a tool refuses one that does, as a location or as a file a table lists. Delta Lake opens an `fs:` or `opendal+fs:` location here whatever host it names. A tool also refuses a `file:`, `fs:`, or `opendal+fs:` location whose path starts with `//`, such as `file:////localhost/x`, since Python 3.14 drops `localhost` or this machine's name there, where DuckDB opens `/localhost/x`, and Windows reads such a path as one on another machine. Write the location with three slashes after the scheme's `:`, as in `file:///data/t`. A tool refuses one whose path Python cannot read as a path on this machine too, such as one with a second `:` on Windows before Python 3.14.
- A Delta Lake log or Iceberg metadata can name a file anywhere, in an object store or on this machine. So for each such table, wherever its `table_uri` points, a tool first lists each file its scan reads. For Delta Lake, these are the data files, the file of each deletion vector, and the log of a table on this machine. For Iceberg, they are the version hint, the metadata file, the manifests, and the data and delete files. A tool refuses the table if a file on this machine lies outside every root, if a file elsewhere lies on a host the server does not allow, or if the files cannot be listed. It checks every path and every host a configuration names before it lists a table, since the listing connects to the table's host.
- A Delta Lake table has its whole log read before the table is loaded, so a deletion vector that an earlier version recorded counts too. A vector at a relative path must name a folder of letters and digits, or the table is refused. A table is refused if a checkpoint in its log names a sidecar, a file deltalake would read from wherever the checkpoint says. Only a v2 checkpoint holds one, and `veridelta run` cannot read such a table either. A table is refused too if a file in its log cannot be read as Delta Lake writes it.
- A Delta Lake `table_uri` written as a URI, such as `file:///data/events`, is refused when its path holds a `.` or `..` segment, in any spelling, such as `%2e%2e`. deltalake removes such a segment by its text before it reads the table, where the listing reads the folder the system finds, so the two could read different logs. Write the URI without the segment, or write the table as a path: deltalake and the listing both read a path such as `../events` as the system does. An `fs:` or `opendal+fs:` `table_uri` is refused when it holds a `%` escape, since deltalake decodes such a URI's path twice and the listing once. Write the table as a path or a `file:` URI instead, which both decode once. deltalake expands only `~` and `~/`, so the server expands a leading `~`, such as `~alice/events`, before deltalake reads the path, as Polars does.
- The listing reads a table in an object store with its `storage_options`, so it needs the keys the scan needs. On the server, an Iceberg scan reads its files as the listing does: PyIceberg plans the scan and reads every file through one PyArrow FileIO, built from `storage_options` alone. The `polars_iceberg` plugin, which parses a location otherwise, is not used, and neither is Polars' cache of metadata files. The FileIO checks each file just before it opens it, and stops the scan at one outside every root or on a host the server does not allow, so a file that changes after the listing cannot lead the scan out. That FileIO opens a file only on this machine, or in S3, Google Cloud Storage, Alibaba Cloud OSS, or Azure at an `abfs://`, `abfss://`, `wasb://`, or `wasbs://` location. So on the server, an Iceberg table fails if it, or a file it names, lies anywhere else, even with its host allowed. Such locations include `hf://`, `hdfs://`, `viewfs://`, `http://`, `https://`, and `az://`. PyArrow opens HDFS through a library it can load from the working directory, the first root, where an agent can write one.
- On the server, a Delta Lake scan lists and checks its table's files again as it starts, and reads the version it checked. It reads the data files through PyArrow, which checks each one just before it opens it, and returns the rows `veridelta run` returns. For a table on this machine, the server copies each file of the log that deltalake reads, and each deletion vector stored in the table's folder, into a private folder just after it checks the file, and deltalake reads that copy. A file that changes after its copy is never read. To load a version, deltalake reads `_last_checkpoint`, and at the top of the log the files of that version and the ones before it, and passes over a checkpoint it cannot read to the commits before it. So the server copies `_last_checkpoint`, and each commit, checkpoint, checksum, or compacted commit from version 0 to the version it reads, or to the latest. It checks every other file of the log, and each file of a later version, but never copies one. The copy of one table holds at most 10,000 files and folders, and takes at most 1 GiB, where each file counts its bytes and one block of the temporary folder's file system, 4 KiB or more, and each folder counts one block. The server refuses a table whose copy would pass either limit, since files that take no room under a root, such as sparse files, or hard links named as commits to one empty file, would otherwise fill the temporary folder. So a table with more than about 10,000 commits, checkpoints, and checksums up to the version read is refused, as is one whose log up to that version takes more than 1 GiB. Read such a table with `veridelta run`, or set `version` to an earlier one. On Linux and macOS, the server also refuses a table whose log holds a name with a `\`, which deltalake reads as `/`, so the name could lead it to a commit outside the log. The server copies a file only when the file it opens is the one its path named just before the check, and a regular file, so a file swapped in after that moment is refused, and so is a named pipe. Two swaps that bracket the check get through: a file swapped out for the check and back in before the open. The copy then holds a file the check never saw, which can lie outside the roots. Closing that needs the path of the open file itself, which Linux, macOS, and Windows each give through a call of their own. The copy goes in the system's temporary folder, so the server refuses a table on this machine when that folder lies under a root, where an agent could change the copy, or when its path holds a `%`, which deltalake would decode, or on Linux and macOS a `\`, which deltalake reads as `/`. Either way deltalake would read the log of another folder, which the server never wrote or checked. Set `TMPDIR` to a folder outside the roots whose path holds neither, and whose path only the server's own user can change, since another user who can change it could swap the copy after it is written. A process killed during a call leaves its copy in the temporary folder, held to the same limits. The server writes each file of the copy under a name it builds in that folder, never at a path a log names, and refuses a link there. A tool refuses a table on this machine whose log stores a deletion vector at a path of its own, as a shallow clone does. deltalake reads such a vector where the log says, so a copy could hold it only by writing there or by changing the log. Read such a table with `veridelta run`. A vector stored in the table's folder, where writers store one by default, is copied. deltalake still reads one kind of file itself: the log of a table elsewhere, such as in an object store, which a writer to that store could change between the server's read and deltalake's, so that deltalake reads a sidecar or a vector the server never checked. deltalake reads those only through the table's own store, so they lie on a host the server allows. A copy on this machine would not hold such a log, since deltalake would then read the table's vectors from this machine, not from the store.
- A tool that opens a side connects to another machine only when each host the side names is given with `--allow-host`, and refuses the side before it connects otherwise. A side names a URL's host, a bucket or a container, a database server, a Snowflake account's host, a Databricks workspace's host, and any host its `storage_options` name, such as an endpoint.
- A tool reads a host only when it is written in one shape: DNS labels or an IP address in ASCII, with at most a port of digits, as in `minio.internal:9000`, `https://minio.internal:9000/`, or `s3://bucket/t`. It refuses a location, an endpoint, a bucket, an account, or a workspace with a login, an `@` anywhere, or a `%`, a `\`, a space, a control character, or a letter outside ASCII in its host, since readers split such text in different places. The AWS SDK in PyArrow reads `http://127.0.0.1:9000@minio.internal` as `127.0.0.1`, for one, where a URL parser reads `minio.internal`. A file that a table's log or metadata names this way is refused, other than an Azure file named as its container, an `@`, and its account's host, as below. An `@` after such a file's host, as in a partition folder such as `owner=ann@x.com`, is part of its path. A database `uri` may hold a login before its host. An Azure location such as `abfss://container@account.dfs.core.windows.net/t` holds its container where a login goes, so write the container as the host, as in `az://container/t`, and the account in `storage_options`, such as `azure_storage_account_name`. Give both the container and the account name with `--allow-host`. Write an Iceberg table in Azure as `abfs://container/t`, with the account's name alone in `adls.account-name`, since its FileIO opens no `az://` location. Spark and Java Iceberg name the table's files as `abfss://container@account.dfs.core.windows.net/...`, which the server reads in that container. Such a file must name the account that `adls.account-name` names, at its `dfs` or `blob` host, and the server refuses a file in any other account. Give its host with `--allow-host` too, such as `account.dfs.core.windows.net`, beside the container and the account name. Give the table an account key in base64 as `adls.account-key`, or a SAS token as `adls.sas-token`, with or without the `?` before it. A SAS token with a `#` in it is refused, since PyArrow drops the text after one. The server refuses a table in Azure with neither, since PyArrow would then sign in as the server's machine does, which can run the `az` command. It also refuses an account name other than 3 to 24 lowercase letters and digits, a key that is not base64, and `adls.client-id`, `adls.client-secret`, and `adls.tenant-id`, since PyArrow ends the server's process on a name with a port in it, on a key it cannot decode, and when such a sign-in fails, as with a wrong client secret.
- A tool refuses a location on another machine whose path holds a `.` or `..` segment, such as `s3://bucket/t/../u`. Any spelling counts: `%2e`, decoded as often as an escape remains, a `\` as a separator, and a tab or line break inside the segment. A reader sends the path to the store as written, and the store, or a layer before it, can remove the segment with the bucket or container before it. The read would then reach another bucket or container on the same host, with the same credential. The rule holds every scheme with a host, such as `s3://`, `gs://`, `az://`, `abfss://`, `wasbs://`, `oss://`, and `https://`, and each file that a Delta Lake log or Iceberg metadata names there, which the refusal leaves out. A name that holds dots, such as `..x` or `.hidden`, passes, and so does an empty segment, as in `s3://bucket//t`, which removes no other. A path on this machine is resolved as the system resolves it, and a service that picks its own hosts, such as `hf://`, is not held to the rule.
- A tool refuses a bucket or a container with a capital letter, such as `s3://Lake/t`, even when `lake` is allowed. The server compares a host with the allowed ones in lowercase, as DNS does. Polars, Delta Lake, and PyIceberg keep a bucket's case, and a bucket named in another case is another bucket. The rule holds the name after every scheme whose host a URL parser keeps in its case, such as `s3://` or `hdfs://`, since the server cannot tell which reader takes it as a bucket. It exempts only `http`, `https`, `ftp`, `ws`, and `wss`, whose host a URL parser lowercases. It also holds a bucket or container in `storage_options`, such as `aws_bucket`, and each file that a Delta Lake log or Iceberg metadata names, with the container of an Azure file named as `container@host`. A host after `https://`, an endpoint, an account name, and the account's host after an `@` keep any case, since each is a DNS name. Write the bucket or container in lowercase, as S3, Google Cloud Storage, and Azure name them. A legacy S3 bucket in `us-east-1` whose name holds a capital letter does not open on the server, so read it with `veridelta run`. For the same reason, `--allow-host` takes a host only in lowercase, and the server does not start with `--allow-host LAKE`. Writing it in lowercase would allow bucket `lake`, another bucket, which the operator never named.
- Every location with a scheme of two or more characters names a host, other than `file:`, `fs:`, `opendal+fs:`, `hf:`, `md:`, and `motherduck:`. That holds for a scheme the server has no list of, such as `hdfs:`, `sftp:`, or `s3n:`, since a reader can know more schemes than the server does. A location with no host, such as `hdfs:///t` or `dbfs:/t`, is refused, since its reader picks the host from elsewhere. A `dbfs:` location is refused whatever host it names, since deltalake opens its path on this machine. So is a file whose name has a colon after two or more letters, such as `ab:c.csv`.
- MotherDuck, Hugging Face, and BigQuery with the server's Application Default Credentials need no host. A configuration still chooses the database, the repository, or the billing project there, and a credential it holds reaches that service, such as a MotherDuck or Hugging Face token from a `${NAME}`.
- A MotherDuck `database` passes on only the options `attach_mode`, `saas_mode`, and `session_name`, each with a value of letters, digits, `_`, and `-`. A tool refuses any other option, since some, such as `motherduck_host`, choose the host MotherDuck's extension connects to. Its name, before the `?`, holds only letters, digits, `_`, `-`, and `/`, and does not start with `/`, such as `md:sales` or `md:_share/sales/1a2b`. MotherDuck's extension also reads an option after a `&` or a `#` in the name, and DuckDB opens `md://x` as a file.
- A DuckDB `database` other than MotherDuck's is a file's path, checked under the roots. The file opens as a DuckDB database through no extension, whatever it holds. A `file:` URI is refused, since DuckDB opens `file://host/x.duckdb` as `/host/x.duckdb`, outside the path a URI parser reads. So is a path that starts with `~` and a name, such as `~analyst/x.duckdb`, since DuckDB reads it as the home folder followed by `analyst`, where the check reads that user's home folder. The server resolves the path, with every link followed and every `..` folded, checks that path, and hands it to DuckDB. DuckDB has nothing left to resolve in it, so it opens the file checked. A `database` the system cannot open as written is refused: one with a part that does not exist, one through more links than the system follows, or one with a separator or `..` after a file. DuckDB also replays the file's write-ahead log as it opens the file, and after a crash during a checkpoint it replays a second log, so the path of each is held to the roots too, after links are followed. Each is the resolved path with `.wal`, `.wal.checkpoint`, or `.wal.recovery` put before the first `?`, or at the end when the path holds none. So a link has its logs beside the file the link leads to, `L?/x.duckdb` has its log at `L.wal?/x.duckdb`, in another folder, and a `?` in the working directory moves the logs of a relative path.
- DuckDB opens a file's path again after the server checks it, so a tool opens the file and its logs only by a path with no link in it. It refuses a log that is a link, even one inside a root. The server opens one DuckDB file at a time. On Linux and macOS, it confirms that a descriptor DuckDB opened just then names the file it checked, and on every system that each log is as it was. Otherwise it closes the connection before any statement runs. A descriptor that another tool call already holds never counts, even one of the same file through a hard link. A server that cannot read its list of open files, such as on Linux without `/proc` mounted, opens no DuckDB file. On macOS, the server must be allowed to list each folder on the file's path, not only to pass through it.
- A tool call that waits more than 60 seconds for another call's DuckDB file to open fails. A file that never finishes opening, such as a pipe an agent puts in its place after the check, makes every later DuckDB read fail that way until the server restarts.
- Three swaps remain unseen. A log put beside the file and removed again while DuckDB opens it replays, and so does a log reached through a folder swapped for a link and back in that time, such as `L.wal?` beside `L?`. A file swapped in and back while DuckDB opens it passes when, at that moment, a query in another tool call opens the checked file, such as through a hard link, or through the file's own folder after it is renamed. On Windows, which lists no descriptors, a link swapped in and back while DuckDB opens the file passes. A hard link inside a root to a file elsewhere is that file to every check, so keep a root free of hard links you did not make.
- Two tool calls cannot read a DuckDB file by one path at the same time: the second fails. Run it again once the first ends.
- A query on a DuckDB file that needs more memory than the machine gives it spills to disk. On Linux and macOS, a tool spills into a folder the server creates in the system's temporary folder as the call opens its first DuckDB file, never beside the file, and removes it when the call ends. Set `TMPDIR` to move it. Keep that folder out of the roots, since an agent that can write under a root could put a link in it. On Windows, such a query fails instead, as a bullet below says.
- A DuckDB `database` such as `sqlite:x.db` or `postgres_scanner:host=db`, with two or more letters, digits, or underscores before its first colon and no `//` after it, is refused. DuckDB would open it through an extension, which loads before the sandbox applies and reads a file or a host the check does not see. The command line refuses it too.
- A tool passes on only the `storage_options` keys it can check: credentials, a region, an endpoint, a bucket, and an account name. It refuses any other key, such as a service host, a proxy, a key file, or an emulator switch, since a reader can take one as a place to connect. That includes `aws_allow_http`. A tool also refuses an endpoint written with a scheme unless it starts with `https://` in lowercase, since Polars and PyIceberg take the scheme from the endpoint and would send the credentials and the rows unencrypted. That refuses `http://minio.internal:9000` and `http:minio.internal` in any case, and `HTTPS://minio.internal:9000`, which the AWS SDK in PyArrow reads as a host name. This holds for every host, `localhost` and `127.0.0.1` too, so a file, a Delta Lake table, or an Iceberg table at an endpoint that serves plain HTTP, as a local MinIO often does, fails on the server. Serve the store over HTTPS, or read the data under a root. An endpoint with no scheme, such as `minio.internal:9000`, passes: PyIceberg then reaches it over HTTPS, and Polars and Delta Lake fail before they connect. A bucket or a container named `https`, with no `:`, is a name and passes too. A tool also refuses an endpoint with anything after its host and port but a single `/`. That covers a path in any spelling, such as `https://minio.internal/other` or `minio.internal:9000/other`, a query, and a fragment. Polars, Delta Lake, and PyIceberg put an endpoint's path before the bucket or the container. So `https://minio.internal/other` with `s3://lake/t` would read bucket `other` with the same credential, though only `lake` was allowed. A store served under a path, such as one behind a reverse proxy or an Azurite emulator with its account in the path, is refused too. Serve the store at its host's root, or read it with `veridelta run`. The rule covers endpoints only: a file whose `path` is an `http://` URL is still read over plain HTTP, so its rows travel unencrypted.
- A tool refuses a BigQuery side's `credentials_path`, since a key file names the host its sign-in goes to. Give the server credentials through its environment instead, such as `GOOGLE_APPLICATION_CREDENTIALS`. The server does not read the key file that variable names, so it does not mask the key's fields.
- Snowflake's driver opens a side's `private_key_path` on this machine before it connects, so a tool holds the key file to the roots as it holds a data path. It expands a leading `~` and reads a relative path from the working directory, as the driver does. The driver drops a leading `./` before it expands `~`, so `./~/k.pem` is the key in the home folder, and the tool reads it that way too. It refuses the side before it connects when the key file lies outside every root, or on Windows on another machine. The message names the setting and not the path. Keep the key file under a root, or ask the person who started the server to add its folder with `--root`.
- A tool refuses a database `uri` with an `@` outside its login, a `#`, or a `\`, a space, or a control character before its database, since a driver can read its host from a different place than the check does.
- A tool refuses a database `uri` that ConnectorX reads as BigQuery, such as `bigquery:///sa.json`. Its path names a key file on this machine, and the key file names the host its sign-in goes to. Use a source of `type: bigquery` instead.
- A tool refuses a `redshift-iam://` database `uri`, which signs in through AWS at hosts its options choose. Write it as `redshift://` with a user and a password.
- A tool refuses a database `uri` parameter that names a TLS file on this machine, which the server does not check: `sslcert`, `sslkey`, and `sslrootcert` for Postgres, `ssl-ca` for MySQL, and `trust_server_certificate_ca` for SQL Server.
- A tool refuses an Oracle `uri` with `alias`, which looks its host up in a file on this machine. ConnectorX writes an Oracle path into the connect string, so a tool passes on only a service name, with at most a server type and an instance, as in `/sales` or `/sales:dedicated/sales1`, and refuses any other path, such as `/sales,other:1521/x`.
- ConnectorX decodes a whole `trino://` uri before it finds the host, so the check reads that uri decoded, with the `password` field in it. A tool refuses a Trino `uri` whose decoded user or password holds a character such as `@`, `/`, `#`, or a space.
- The check covers the hosts the configuration names, and the host of each file a Delta or Iceberg table's log or metadata names. A host that an allowed host redirects a request to is not checked. Allow only hosts you trust.
- A configuration sends its credentials, a `${NAME}` from the server's environment included, to the hosts it names. A host on this machine or its network could also answer with what the agent should not reach. That is why no host is allowed by default, so allow only hosts you trust. A reader can also reach a host the configuration does not name, such as Azure's managed identity endpoint when only a client id is given, and send it what the reader sends there. A host cannot hold a `${NAME}` value. The refusal names the setting.
- A run writes the rows that differ to the configuration's `output_path`, so `run_comparison` and `read_discrepancies` refuse one outside every root before they read a row. Each file the run writes replaces a link already at its name instead of following it, so a link in that folder cannot lead a write outside the roots.
- A side's `query` runs as written, with the configuration's credentials, so a tool that would run one refuses unless the server is started with `--allow-queries`. Even then, on Linux and macOS, a DuckDB file's connection reads other files, attaches databases, and loads extensions only from under the roots, after DuckDB follows the links in a file's path. Before it refuses a path outside the roots, DuckDB reads each link in it, which reaches no other machine unless the system mounts one when the path is looked up, as an automounter can under `/net`. On Windows, DuckDB's own check of the roots lets some paths out, such as a drive-relative path with a lowercase letter, `c:..\x.csv`. It also resolves a share path, which reaches the machine the path names, before it refuses it. So on Windows, a DuckDB file's connection opens no other file at all. A `query`, or a view stored in the file, that reads a file or attaches a database fails with a `Permission Error`. So does one that needs more memory than DuckDB has, which DuckDB would write to disk, and so does a [comparison in place](pushdown.md#duckdb-and-motherduck) that does. Read such a file as a side of its own. On every system, DuckDB's persistent secrets are off, so the connection neither reads a secret stored in the home folder nor stores or drops one. A MotherDuck connection is not held to the roots.
- A tool returns findings, counts, and column names. It never returns the configuration, and it returns a value from the data only on a server started with `--allow-row-values`. A password inside an error is masked, as on the command line.
- On the server, a `${NAME}` may stand only as the whole value of a credential setting. These are the `password`, `private_key_passphrase`, `access_token`, and `motherduck_token` fields, and the storage options that hold a secret key, a token, or an account key. A secret key or a token, in a storage option, `access_token`, or `motherduck_token`, must be letters, digits, `+`, `/`, `=`, `.`, `_`, `~`, and `-`, since a service can repeat a value it rejects in another form: S3 writes a session token back XML-escaped and trimmed. An account key must be base64 as an encoder writes it, since the reader's error for any other names a character of it. An access key id, a client id, a tenant id, and the Snowflake `user` may be one too, as a login name of letters, digits, `.`, `_`, `@`, and `-` that starts with a letter or a digit. An Azure SAS token must keep its shape: `name=value` parameters joined by `&`. A database `uri` takes one as its whole user or its whole password, of letters, digits, `.`, `_`, `~`, and `-`. A SQLite side takes none in `password`, since SQLite has no password and the driver reads it into the file's path.
- A `uri` user from a `${NAME}` holds at most 32 characters. A database can cut a longer user name before it writes the name into an error, and the mask matches only a whole value. No supported database cuts a name of 32 characters or fewer: MySQL keeps 32 characters, PostgreSQL 63 bytes, Redshift 127 bytes, and SQL Server and Oracle 128. Write a longer user in the file, or put a token in the side's `password` setting.
- A value of 1 to 3 characters is refused in a `password`, a `private_key_passphrase`, a secret key or a token, an account key, and a `uri` password, since the server could not mask it. A login name, a `uri` user, and a SAS token have no such floor, and one shorter than 4 characters is not masked, so it can come back as it is. A login name or a user is an identifier a service may echo, not a secret, and a SAS token that short holds no `sig`. An empty value passes in every setting, since it holds nothing to mask. A value with a byte outside UTF-8 is refused in every setting, since a driver cannot send it as text, and a reader could repeat it in a form no mask matches.
- A tool refuses a `${NAME}` anywhere else, or with other text around it, before it reads any data. That covers a `path`, a `table_uri`, a host, a reader option, and a `query`. The refusal names the setting and the variable. A reader can repeat a setting's value encoded, cut, or joined to other text, in forms no mask matches. Write such a value in the file, or run the configuration with `veridelta run`.
- The server masks every value of 4 or more characters that it has expanded since it started, in every answer, in any case. It masks each value as written, percent-encoded, and form-encoded, with `~` as `%7E`, as object_store writes an OAuth request's body. It also masks each value as its bytes in hex, as S3 writes a request it could not verify. It masks only a whole value in one of these forms, never a part, such as the head of a name a database cut. The one exception is a SAS token's `sig`, which it masks on its own, as written, decoded, and encoded, since Azure writes the token's parameters back in another order. A reader masks its side's password in a driver's message too, as on the command line. The server's mask runs before it, so a password of one letter written in the file cannot cut an expanded value into pieces.
- The server also masks the credentials its readers take from its environment with no `${NAME}` in the configuration. These are the AWS keys and tokens, and the Azure account keys, SAS tokens, tokens, client ids, client secrets, and tenant ids. They also include `GOOGLE_SERVICE_ACCOUNT_KEY`, `GOOGLE_BEARER_TOKEN`, `MSI_SECRET`, `IDENTITY_HEADER`, `HF_TOKEN`, and `MOTHERDUCK_TOKEN`. The server masks the keys and tokens that deltalake's OpenDAL stores read for Alibaba Cloud OSS, Tencent Cloud COS, Huawei Cloud OBS, and Azure too. Examples are `OSS_ACCESS_KEY_ID`, `OSS_SESSION_TOKEN`, `TENCENTCLOUD_SECRET_KEY`, `HUAWEI_CLOUD_SECURITY_TOKEN`, `AZBLOB_ACCOUNT_KEY`, and `SYSTEM_ACCESSTOKEN`. No configuration reaches those stores on the server yet, since each needs an `opendal.` storage option, which the server refuses. It also masks the tokens, client ids, and client secrets that deltalake's Unity Catalog reader reads for a `uc://` table. Examples are `DATABRICKS_TOKEN`, `UNITY_ACCESS_TOKEN`, and `DATABRICKS_CLIENT_SECRET`. The server reads each such variable once, as it starts, in the shapes a `${NAME}` in the same setting needs. It does not start when one lacks its shape, and the error names the variable. It finds a variable under each name a reader takes it by: after `AWS_`, `AZURE_`, `GOOGLE_`, `UNITY_`, or `DATABRICKS_`, a name in any case counts, such as `AZURE_Client_Secret` or `DATABRICKS_Token`. It reads the token files that `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`, `AZURE_FEDERATED_TOKEN_FILE`, `ALIBABA_CLOUD_OIDC_TOKEN_FILE`, `TENCENTCLOUD_WEB_IDENTITY_TOKEN_FILE`, and `TKE_IDENTITY_TOKEN_FILE` name at each call, since such a file rotates, and masks the token each file holds then, not one it held before. It reads only a regular file. A named pipe, such as one `<(get-token)` makes, is never read, since a pipe with no writer would hold the server and a pipe gives its token only to the first process that reads it, which must be the reader.
- The server never sees a credential a reader finds on its own. That covers a profile or a credentials file, SSO, `credential_process`, an instance or container role, managed identity, the `az` CLI, and the key file `GOOGLE_APPLICATION_CREDENTIALS` names. So the server blanks the elements of an S3 error that repeat a credential, whatever they hold: `Token-n`, `AWSAccessKeyId`, `CanonicalRequest`, `StringToSign`, and `SignatureProvided`. Any other echo of such a credential comes back as written with `--allow-row-values`. Data whose cells hold such an element loses the element's content too.
- After that masking, a URL in a failed call's message or in a finding comes back without its query, even with quotes in its path. A URL that starts inside a masked value keeps its query. A reader can quote a URL it signed, such as each request PyArrow signs with an Azure SAS token, and the Azure SDK writes the token's parameters back in another order than the variable holds them. So the `sig` parameter of a SAS token from a variable is masked on its own too, as written, decoded, and encoded.
- Masking hides text, not effects. Whether a service accepts a credential, and how long a call takes, can still tell an agent something about a value. So can the mask itself. A configuration can put any variable in a credential setting, and an answer then masks each text equal to its value, such as a column name. The same holds for each credential a reader takes from the server's environment. So the mask confirms a guess of a whole value, which lets an agent test guesses at a short or common value. Other than a SAS token's `sig`, it masks no part of a value, so a guess at a part, such as its first characters, comes back as written and confirms nothing. It masks a whole value inside other words too, for as long as the server runs: a `uri` user `postgres` turns `postgresql://` into `***ql://`. A placeholder credential in the environment does the same: `AWS_ACCESS_KEY_ID=test` masks every `test`. A refusal of a value's shape says whether the value has that shape, such as base64 or 1 to 3 characters, but never quotes it.
- With `--allow-row-values`, `read_discrepancies` returns at most `--max-rows` rows per call, 50 by default. `propose_value_maps` returns at most that many value map entries, counting the entries a column's rule already has, and a proposal comes back whole or not at all. `suggest_rules` returns at most that many example keys, and a suggestion comes back whole or not at all.
- With `schemas`, a check reads each side's columns as `veridelta validate --schemas` does, and without it a check opens no data. `describe_schema` reads one side's columns the same way. For a warehouse table, it runs the schema probe a run starts with.
- A host may start the server with only some of the user's environment variables. A `${NAME}` that the configuration references must reach the server, through the host's `env` setting for it if need be, or the check reports it unset. `allow_missing_env` checks a file without them, and still refuses a `${NAME}` outside a credential setting.

A call that fails returns its error's type and message, as `run --json` prints them, such as `ConfigError` for a path outside the roots. Polars, a file reader, or a database driver can quote a value from the data in its message. On a server started without `--allow-row-values`, the call leaves out such a message and returns the error's type and a hint. `validate_config` leaves such a message out of a finding the same way, since reading a JSON file's columns parses the whole file. The same holds for a Delta Lake or Iceberg table that cannot be listed or opened, since deltalake or PyIceberg can quote a file it opened, such as a JSON file named as the table's metadata. A refusal of a file that a table's log or metadata names, outside the roots, quotes that file's path only on a server started with `--allow-row-values`, since the path is a value from the table, such as a partition's value in a folder name. `veridelta run` and `veridelta validate` print the whole message. A message Veridelta writes names columns and counts, and the call returns it on any server. [Serving tools to an agent](cli.md#serving-tools-to-an-agent) lists the command's flags.

## Where row values appear

The summary holds counts and column names only. These outputs hold values from the data:

- the discrepancy files a run writes to `output_path`;
- the [HTML report](results.md#html-report);
- a [Markdown summary](results.md#markdown-summary) with `--markdown-max-rows` above zero;
- the proposals `veridelta crosswalk` prints;
- the example keys `veridelta suggest` prints;
- what `read_discrepancies`, `propose_value_maps`, and `suggest_rules` return, on an MCP server started with `--allow-row-values`;
- the message of an error that Polars, a file reader, a Delta Lake or Iceberg reader, or a database driver raises, which `veridelta run` and `veridelta validate` print and an MCP server returns only when started with `--allow-row-values`;
- the path of a file that a Delta Lake or Iceberg table's log or metadata names, which an MCP server quotes in a refusal only when started with `--allow-row-values`.

A pair compared in place, such as two warehouse tables, brings back counts and primary keys only, unless it fetches a [row sample](pushdown.md#row-samples).

## Writing a configuration

`veridelta schema` prints the JSON Schema of the configuration file. Check a draft against it, then run `veridelta validate`.

## Checking the output

`veridelta schema run` prints the JSON Schema of what `veridelta run --json` prints. `veridelta schema validate`, `crosswalk`, `suggest`, `baseline`, and `error` print the others. The docs site serves the same files; see [Printing the schema](cli.md#printing-the-schema). A script or an agent can check what it parses against them.

On a pull request, the comment the [GitHub Action](ci.md#github-actions) keeps ends with the run's summary as JSON, in an HTML comment that readers never see. It holds counts and the column names the comment shows, never a value, and [Markdown summary](results.md#markdown-summary) shows how to parse it.

## Fixing drift in a loop

An agent that changes a pipeline can check its own work against the data the pipeline produced before, and keep fixing until the two match:

1. Change the pipeline. Push the change to the pull request, or run the pipeline locally.
2. Let the comparison run: the [GitHub Action](ci.md#github-actions) on the pull request, or `veridelta run --json` locally.
3. Read the result and its exit code. On the pull request, parse the JSON at the end of the Action's comment, as [Markdown summary](results.md#markdown-summary) shows. Locally, read what `run --json` prints. Both hold `added_count`, `removed_count`, and `changed_count`. `column_mismatches` holds every drifting column in `run --json`, and in the comment the ones its table lists.
4. Find the cause in the change, not in the data. The columns that drift, and how many rows each one changes, point to the code that writes them. With the MCP server started with `--allow-row-values`, `read_discrepancies` returns the rows that differ.
5. Fix the change, and go back to step 1.

Stop when the run matches, and report the counts. Stop too when the drift that remains is what the user asked for, such as a new rounding, and say which columns it is in. Never add a rule, or raise `threshold`, to make a run pass unless the user agrees: a rule changes what counts as a match, for every later run too. When the user accepts the drift that remains, `veridelta run --save-baseline accepted.json` records it, and later runs pass `--baseline accepted.json`, so they fail only on new drift; see [Accepting drift](cli.md#accepting-drift).

## Docs for language models

The site publishes two plain-text files for language models, as the [llms.txt proposal](https://llmstxt.org/) describes:

- [`llms.txt`](https://veridelta.github.io/veridelta/llms.txt) links every page, each with its opening sentence.
- [`llms-full.txt`](https://veridelta.github.io/veridelta/llms-full.txt) holds every page of prose in one file, this one included.

## Agent skill

The steps above are also an agent skill, in [`skills/veridelta/SKILL.md`](https://github.com/Veridelta/veridelta/blob/main/skills/veridelta/SKILL.md). An agent that follows the Agent Skills standard loads it from a skills folder in the project. This installs it where Claude Code reads skills; for another agent, use the folder its documentation names:

```bash
mkdir -p .claude/skills/veridelta
curl -fsSL -o .claude/skills/veridelta/SKILL.md https://raw.githubusercontent.com/Veridelta/veridelta/main/skills/veridelta/SKILL.md
```

To pin the skill to a release, put the release's tag in place of `main`.
