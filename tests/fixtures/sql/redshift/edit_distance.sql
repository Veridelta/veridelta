-- case: edit_distance
-- dialect: redshift

-- statement: probe
SELECT * FROM "shop"."orders_v1" WHERE 1 = 0

-- statement: probe
SELECT * FROM "shop"."orders_v2" WHERE 1 = 0

-- statement: compile
-- refused: ConfigError: max_levenshtein_distance cannot be pushed down to redshift: it has no edit distance function. Compare locally instead (pushdown: false).
