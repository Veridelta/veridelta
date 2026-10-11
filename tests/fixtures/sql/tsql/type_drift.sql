-- case: type_drift
-- dialect: tsql

-- statement: probe
SELECT * FROM [shop].[orders_v1] WHERE 1 = 0

-- statement: probe
SELECT * FROM [shop].[orders_v2] WHERE 1 = 0

-- statement: duplicates source
SELECT COALESCE(SUM([_veridelta_rows]), 0) AS [_veridelta_total] FROM (SELECT COUNT_BIG(*) AS [_veridelta_rows] FROM (SELECT [src].[id] AS [id] FROM [shop].[orders_v1] AS [src]) AS [_veridelta_keys] GROUP BY [id] HAVING COUNT_BIG(*) > 1) AS [_veridelta_duplicates]

-- statement: duplicates target
SELECT COALESCE(SUM([_veridelta_rows]), 0) AS [_veridelta_total] FROM (SELECT COUNT_BIG(*) AS [_veridelta_rows] FROM (SELECT [tgt].[id] AS [id] FROM [shop].[orders_v2] AS [tgt]) AS [_veridelta_keys] GROUP BY [id] HAVING COUNT_BIG(*) > 1) AS [_veridelta_duplicates]

-- statement: count source
SELECT COUNT_BIG(*) AS [_veridelta_total] FROM [shop].[orders_v1]

-- statement: count target
SELECT COUNT_BIG(*) AS [_veridelta_total] FROM [shop].[orders_v2]

-- statement: changed
WITH [_src_normalized] AS (SELECT [src].[id] AS [id], [src].[units] AS [units], [src].[kept] AS [kept], [src].[forgiven] AS [forgiven] FROM [shop].[orders_v1] AS [src]), [_tgt_normalized] AS (SELECT [tgt].[id] AS [id], [tgt].[units] AS [units], CAST([tgt].[kept] AS NVARCHAR(MAX)) COLLATE Latin1_General_100_BIN2 AS [kept], CAST([tgt].[forgiven] AS NVARCHAR(MAX)) COLLATE Latin1_General_100_BIN2 AS [forgiven] FROM [shop].[orders_v2] AS [tgt]) SELECT [src].[id] FROM [_src_normalized] AS [src] INNER JOIN [_tgt_normalized] AS [tgt] ON [src].[id] = [tgt].[id] WHERE NOT ((CASE WHEN ([src].[units] IS NULL AND [tgt].[units] IS NULL) THEN 1 ELSE 0 END = 1) AND (CASE WHEN ([src].[kept] IS NULL AND [tgt].[kept] IS NULL) THEN 1 ELSE 0 END = 1) AND (CASE WHEN ([src].[forgiven] IS NULL AND [tgt].[forgiven] IS NULL) THEN 1 ELSE 0 END = 1))

-- statement: added
WITH [_src_normalized] AS (SELECT [src].[id] AS [id] FROM [shop].[orders_v1] AS [src]), [_tgt_normalized] AS (SELECT [tgt].[id] AS [id] FROM [shop].[orders_v2] AS [tgt]) SELECT [tgt].[id] FROM [_src_normalized] AS [src] RIGHT JOIN [_tgt_normalized] AS [tgt] ON [src].[id] = [tgt].[id] WHERE [src].[id] IS NULL

-- statement: missing
WITH [_src_normalized] AS (SELECT [src].[id] AS [id] FROM [shop].[orders_v1] AS [src]), [_tgt_normalized] AS (SELECT [tgt].[id] AS [id] FROM [shop].[orders_v2] AS [tgt]) SELECT [src].[id] FROM [_src_normalized] AS [src] LEFT JOIN [_tgt_normalized] AS [tgt] ON [src].[id] = [tgt].[id] WHERE [tgt].[id] IS NULL

-- statement: columns
WITH [_src_normalized] AS (SELECT [src].[id] AS [id], [src].[units] AS [units], [src].[kept] AS [kept], [src].[forgiven] AS [forgiven] FROM [shop].[orders_v1] AS [src]), [_tgt_normalized] AS (SELECT [tgt].[id] AS [id], [tgt].[units] AS [units], CAST([tgt].[kept] AS NVARCHAR(MAX)) COLLATE Latin1_General_100_BIN2 AS [kept], CAST([tgt].[forgiven] AS NVARCHAR(MAX)) COLLATE Latin1_General_100_BIN2 AS [forgiven] FROM [shop].[orders_v2] AS [tgt]) SELECT SUM(CAST(CASE WHEN (CASE WHEN ([src].[units] IS NULL AND [tgt].[units] IS NULL) THEN 1 ELSE 0 END = 1) THEN 0 ELSE 1 END AS BIGINT)) AS [units], SUM(CAST(CASE WHEN (CASE WHEN ([src].[kept] IS NULL AND [tgt].[kept] IS NULL) THEN 1 ELSE 0 END = 1) THEN 0 ELSE 1 END AS BIGINT)) AS [kept], SUM(CAST(CASE WHEN (CASE WHEN ([src].[forgiven] IS NULL AND [tgt].[forgiven] IS NULL) THEN 1 ELSE 0 END = 1) THEN 0 ELSE 1 END AS BIGINT)) AS [forgiven] FROM [_src_normalized] AS [src] INNER JOIN [_tgt_normalized] AS [tgt] ON [src].[id] = [tgt].[id]

-- statement: sample
-- renames: {"_veridelta_key_0": "id", "_veridelta_source_0": "units_source", "_veridelta_target_0": "units_target", "_veridelta_match_0": "units_is_match", "_veridelta_source_1": "kept_source", "_veridelta_target_1": "kept_target", "_veridelta_match_1": "kept_is_match", "_veridelta_source_2": "forgiven_source", "_veridelta_target_2": "forgiven_target", "_veridelta_match_2": "forgiven_is_match"}
WITH [_src_normalized] AS (SELECT [src].[id] AS [id], [src].[units] AS [units], [src].[kept] AS [kept], [src].[forgiven] AS [forgiven] FROM [shop].[orders_v1] AS [src]), [_tgt_normalized] AS (SELECT [tgt].[id] AS [id], [tgt].[units] AS [units], CAST([tgt].[kept] AS NVARCHAR(MAX)) COLLATE Latin1_General_100_BIN2 AS [kept], CAST([tgt].[forgiven] AS NVARCHAR(MAX)) COLLATE Latin1_General_100_BIN2 AS [forgiven] FROM [shop].[orders_v2] AS [tgt]) SELECT TOP (3) [src].[id] AS [_veridelta_key_0], [src].[units] AS [_veridelta_source_0], [tgt].[units] AS [_veridelta_target_0], CASE WHEN ([src].[units] IS NULL AND [tgt].[units] IS NULL) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS [_veridelta_match_0], [src].[kept] AS [_veridelta_source_1], [tgt].[kept] AS [_veridelta_target_1], CASE WHEN ([src].[kept] IS NULL AND [tgt].[kept] IS NULL) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS [_veridelta_match_1], [src].[forgiven] AS [_veridelta_source_2], [tgt].[forgiven] AS [_veridelta_target_2], CASE WHEN ([src].[forgiven] IS NULL AND [tgt].[forgiven] IS NULL) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS [_veridelta_match_2] FROM [_src_normalized] AS [src] INNER JOIN [_tgt_normalized] AS [tgt] ON [src].[id] = [tgt].[id] WHERE NOT ((CASE WHEN ([src].[units] IS NULL AND [tgt].[units] IS NULL) THEN 1 ELSE 0 END = 1) AND (CASE WHEN ([src].[kept] IS NULL AND [tgt].[kept] IS NULL) THEN 1 ELSE 0 END = 1) AND (CASE WHEN ([src].[forgiven] IS NULL AND [tgt].[forgiven] IS NULL) THEN 1 ELSE 0 END = 1)) ORDER BY [_veridelta_key_0]
