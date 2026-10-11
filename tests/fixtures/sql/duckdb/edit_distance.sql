-- case: edit_distance
-- dialect: duckdb

-- statement: probe
SELECT * FROM "shop"."orders_v1" WHERE 1 = 0

-- statement: probe
SELECT * FROM "shop"."orders_v2" WHERE 1 = 0

-- statement: compile
-- refused: ConfigError: max_levenshtein_distance cannot be pushed down to duckdb: its levenshtein counts UTF-8 bytes, not characters, so an accented letter counts as two edits. Compare locally instead (pushdown: false).
