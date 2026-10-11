-- case: regex_groups
-- dialect: redshift

-- statement: probe
SELECT * FROM "shop"."orders_v1" WHERE 1 = 0

-- statement: probe
SELECT * FROM "shop"."orders_v2" WHERE 1 = 0

-- statement: compile
-- refused: ConfigError: regex_replace cannot be pushed down to redshift with the replacement '$2$1': its REGEXP_REPLACE documents no way to refer to a group or to escape a character in the replacement. Use a replacement of plain text, or compare locally instead (pushdown: false).
