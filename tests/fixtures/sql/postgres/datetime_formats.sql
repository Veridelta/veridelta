-- case: datetime_formats
-- dialect: postgres

-- statement: probe
SELECT * FROM "shop"."orders_v1" WHERE 1 = 0

-- statement: probe
SELECT * FROM "shop"."orders_v2" WHERE 1 = 0

-- statement: compile
-- refused: ConfigError: datetime_format cannot be pushed down to postgres: it has no parse that returns NULL for a value it cannot read, as a local run does, so one bad row would fail the whole statement. Compare locally instead (pushdown: false).
