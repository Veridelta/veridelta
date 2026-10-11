-- case: regex_plain
-- dialect: tsql

-- statement: probe
SELECT * FROM [shop].[orders_v1] WHERE 1 = 0

-- statement: probe
SELECT * FROM [shop].[orders_v2] WHERE 1 = 0

-- statement: compile
-- refused: ConfigError: regex_replace cannot be pushed down to tsql: Synapse and SQL Server before 2025 have no REGEXP_REPLACE. Compare locally instead (pushdown: false).
