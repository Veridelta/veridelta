-- case: edit_distance
-- dialect: postgres

-- statement: probe
SELECT * FROM "shop"."orders_v1" WHERE 1 = 0

-- statement: probe
SELECT * FROM "shop"."orders_v2" WHERE 1 = 0

-- statement: compile
-- refused: ConfigError: max_levenshtein_distance cannot be pushed down to postgres: its levenshtein needs the fuzzystrmatch extension and refuses text longer than 255 characters. Compare locally instead (pushdown: false).
