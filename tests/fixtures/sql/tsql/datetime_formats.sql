-- case: datetime_formats
-- dialect: tsql

-- statement: probe
SELECT * FROM [shop].[orders_v1] WHERE 1 = 0

-- statement: probe
SELECT * FROM [shop].[orders_v2] WHERE 1 = 0

-- statement: compile
-- refused: ConfigError: datetime_format cannot be pushed down to tsql: its TRY_CONVERT reads a fixed list of numbered styles, not a format string, so most formats have no spelling. Compare locally instead (pushdown: false).
