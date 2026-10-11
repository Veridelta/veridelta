-- case: casts
-- dialect: snowflake

-- statement: probe
SELECT * FROM "shop"."orders_v1" WHERE 1 = 0

-- statement: probe
SELECT * FROM "shop"."orders_v2" WHERE 1 = 0

-- statement: duplicates source
SELECT COALESCE(SUM("_veridelta_rows"), 0) AS "_veridelta_total" FROM (SELECT COUNT(*) AS "_veridelta_rows" FROM (SELECT "src"."id" AS "id" FROM "shop"."orders_v1" AS "src") AS "_veridelta_keys" GROUP BY "id" HAVING COUNT(*) > 1) AS "_veridelta_duplicates"

-- statement: duplicates target
SELECT COALESCE(SUM("_veridelta_rows"), 0) AS "_veridelta_total" FROM (SELECT COUNT(*) AS "_veridelta_rows" FROM (SELECT "tgt"."id" AS "id" FROM "shop"."orders_v2" AS "tgt") AS "_veridelta_keys" GROUP BY "id" HAVING COUNT(*) > 1) AS "_veridelta_duplicates"

-- statement: count source
SELECT COUNT(*) AS "_veridelta_total" FROM "shop"."orders_v1"

-- statement: count target
SELECT COUNT(*) AS "_veridelta_total" FROM "shop"."orders_v2"

-- statement: changed
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", CASE WHEN LENGTH(CAST("src"."zip" AS VARCHAR)) >= 5 THEN CAST("src"."zip" AS VARCHAR) WHEN SUBSTR(CAST("src"."zip" AS VARCHAR), 1, 1) IN ('-', '+') THEN SUBSTR(CAST("src"."zip" AS VARCHAR), 1, 1) || LPAD(SUBSTR(CAST("src"."zip" AS VARCHAR), 2), 4, '0') ELSE LPAD(CAST("src"."zip" AS VARCHAR), 5, '0') END AS "zip", CAST(CASE WHEN "src"."rounded" < 0 THEN CEIL("src"."rounded") ELSE FLOOR("src"."rounded") END AS BIGINT) AS "rounded", CAST("src"."ratio" AS FLOAT) AS "ratio", CAST("src"."label" AS VARCHAR) AS "label", CAST("src"."flag" AS BOOLEAN) AS "flag", CAST("src"."day" AS DATE) AS "day", CAST("src"."stamp" AS TIMESTAMP_NTZ) AS "stamp" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", CASE WHEN LENGTH(CAST("tgt"."zip" AS VARCHAR)) >= 5 THEN CAST("tgt"."zip" AS VARCHAR) WHEN SUBSTR(CAST("tgt"."zip" AS VARCHAR), 1, 1) IN ('-', '+') THEN SUBSTR(CAST("tgt"."zip" AS VARCHAR), 1, 1) || LPAD(SUBSTR(CAST("tgt"."zip" AS VARCHAR), 2), 4, '0') ELSE LPAD(CAST("tgt"."zip" AS VARCHAR), 5, '0') END AS "zip", CAST("tgt"."rounded" AS BIGINT) AS "rounded", CAST("tgt"."ratio" AS FLOAT) AS "ratio", CAST("tgt"."label" AS VARCHAR) AS "label", CAST("tgt"."flag" AS BOOLEAN) AS "flag", CAST("tgt"."day" AS DATE) AS "day", CAST("tgt"."stamp" AS TIMESTAMP_NTZ) AS "stamp" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."id" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE NOT (COALESCE(EQUAL_NULL("src"."zip", "tgt"."zip"), FALSE) AND COALESCE(EQUAL_NULL("src"."rounded", "tgt"."rounded"), FALSE) AND COALESCE(EQUAL_NULL("src"."ratio", "tgt"."ratio"), FALSE) AND COALESCE(EQUAL_NULL("src"."label", "tgt"."label"), FALSE) AND COALESCE(EQUAL_NULL("src"."flag", "tgt"."flag"), FALSE) AND COALESCE(EQUAL_NULL("src"."day", "tgt"."day"), FALSE) AND COALESCE(EQUAL_NULL("src"."stamp", "tgt"."stamp"), FALSE))

-- statement: added
WITH "_src_normalized" AS (SELECT "src"."id" AS "id" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id" FROM "shop"."orders_v2" AS "tgt") SELECT "tgt"."id" FROM "_src_normalized" AS "src" RIGHT JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE "src"."id" IS NULL

-- statement: missing
WITH "_src_normalized" AS (SELECT "src"."id" AS "id" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."id" FROM "_src_normalized" AS "src" LEFT JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE "tgt"."id" IS NULL

-- statement: columns
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", CASE WHEN LENGTH(CAST("src"."zip" AS VARCHAR)) >= 5 THEN CAST("src"."zip" AS VARCHAR) WHEN SUBSTR(CAST("src"."zip" AS VARCHAR), 1, 1) IN ('-', '+') THEN SUBSTR(CAST("src"."zip" AS VARCHAR), 1, 1) || LPAD(SUBSTR(CAST("src"."zip" AS VARCHAR), 2), 4, '0') ELSE LPAD(CAST("src"."zip" AS VARCHAR), 5, '0') END AS "zip", CAST(CASE WHEN "src"."rounded" < 0 THEN CEIL("src"."rounded") ELSE FLOOR("src"."rounded") END AS BIGINT) AS "rounded", CAST("src"."ratio" AS FLOAT) AS "ratio", CAST("src"."label" AS VARCHAR) AS "label", CAST("src"."flag" AS BOOLEAN) AS "flag", CAST("src"."day" AS DATE) AS "day", CAST("src"."stamp" AS TIMESTAMP_NTZ) AS "stamp" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", CASE WHEN LENGTH(CAST("tgt"."zip" AS VARCHAR)) >= 5 THEN CAST("tgt"."zip" AS VARCHAR) WHEN SUBSTR(CAST("tgt"."zip" AS VARCHAR), 1, 1) IN ('-', '+') THEN SUBSTR(CAST("tgt"."zip" AS VARCHAR), 1, 1) || LPAD(SUBSTR(CAST("tgt"."zip" AS VARCHAR), 2), 4, '0') ELSE LPAD(CAST("tgt"."zip" AS VARCHAR), 5, '0') END AS "zip", CAST("tgt"."rounded" AS BIGINT) AS "rounded", CAST("tgt"."ratio" AS FLOAT) AS "ratio", CAST("tgt"."label" AS VARCHAR) AS "label", CAST("tgt"."flag" AS BOOLEAN) AS "flag", CAST("tgt"."day" AS DATE) AS "day", CAST("tgt"."stamp" AS TIMESTAMP_NTZ) AS "stamp" FROM "shop"."orders_v2" AS "tgt") SELECT SUM(CASE WHEN COALESCE(EQUAL_NULL("src"."zip", "tgt"."zip"), FALSE) THEN 0 ELSE 1 END) AS "zip", SUM(CASE WHEN COALESCE(EQUAL_NULL("src"."rounded", "tgt"."rounded"), FALSE) THEN 0 ELSE 1 END) AS "rounded", SUM(CASE WHEN COALESCE(EQUAL_NULL("src"."ratio", "tgt"."ratio"), FALSE) THEN 0 ELSE 1 END) AS "ratio", SUM(CASE WHEN COALESCE(EQUAL_NULL("src"."label", "tgt"."label"), FALSE) THEN 0 ELSE 1 END) AS "label", SUM(CASE WHEN COALESCE(EQUAL_NULL("src"."flag", "tgt"."flag"), FALSE) THEN 0 ELSE 1 END) AS "flag", SUM(CASE WHEN COALESCE(EQUAL_NULL("src"."day", "tgt"."day"), FALSE) THEN 0 ELSE 1 END) AS "day", SUM(CASE WHEN COALESCE(EQUAL_NULL("src"."stamp", "tgt"."stamp"), FALSE) THEN 0 ELSE 1 END) AS "stamp" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id"
