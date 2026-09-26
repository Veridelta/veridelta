# D step 0 results (2026-09-26, polars 1.40.1, connectorx 0.4.6, Python 3.12, -W error: no warnings)
- URI: "sqlite://" + urllib.parse.quote(abs_path) -> sqlite:///tmp/x.db works on POSIX.
- Full-read dtypes (declared types): INTEGER->Int64, REAL->Float64, TEXT->String, DATE->Date,
  DATETIME->Datetime(us, None), BOOLEAN->Boolean, NUMERIC->Float64 (value 1.25). NULLs preserved.
- Zero-row probe (`SELECT * FROM "t" WHERE 1 = 0`): same, EXCEPT NUMERIC->String. So zero-row probes
  are not type-faithful on SQLite (matters for S6 validate --schemas: warn / compare names only).
- Quoted "Mixed" table works; unquoted `mixed` also works on SQLite (case-insensitive).
- Trailing `;` works on SQLite.
- Column with no declared type and leading NULL: RuntimeError "Cannot infer type from null for SQLite".
- Missing SQLite file: connectorx CREATES an empty file, then errors "no such table: t" (RuntimeError).
  => refuse a sqlite path that is not an existing file before reading.
- Errors are RuntimeError (Polars re-raises type(err)(scrubbed) from err; __cause__ RuntimeError).
- Unreachable postgres: r2d2 logs ERROR lines to stderr (no password) and times out after ~25 s,
  RuntimeError "timed out waiting for connection: ... Connection refused".
