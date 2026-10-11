-- case: edit_distance
-- dialect: tsql

-- statement: probe
SELECT * FROM [shop].[orders_v1] WHERE 1 = 0

-- statement: probe
SELECT * FROM [shop].[orders_v2] WHERE 1 = 0

-- statement: compile
-- refused: ConfigError: max_levenshtein_distance cannot be pushed down to tsql: Synapse and SQL Server before 2025 have no edit distance function. Compare locally instead (pushdown: false).
