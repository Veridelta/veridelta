-- case: datetime_formats
-- dialect: redshift

-- statement: probe
SELECT * FROM "shop"."orders_v1" WHERE 1 = 0

-- statement: probe
SELECT * FROM "shop"."orders_v2" WHERE 1 = 0

-- statement: compile
-- refused: ConfigError: datetime_format cannot be pushed down to redshift: its TO_TIMESTAMP fails on a value it cannot read, where a local run returns NULL, so one bad row would fail the whole statement. Compare locally instead (pushdown: false).
