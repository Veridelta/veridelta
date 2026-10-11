-- case: value_maps
-- dialect: duckdb

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
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", CASE WHEN "src"."status" = 'A' THEN 'Active' ELSE "src"."status" END AS "status", "src"."tier" AS "tier" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."status" AS "status", "tgt"."tier" AS "tier" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."id" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE NOT (COALESCE("src"."status" IS NOT DISTINCT FROM "tgt"."status", FALSE) AND COALESCE("src"."tier" IS NOT DISTINCT FROM "tgt"."tier", FALSE))

-- statement: added
WITH "_src_normalized" AS (SELECT "src"."id" AS "id" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id" FROM "shop"."orders_v2" AS "tgt") SELECT "tgt"."id" FROM "_src_normalized" AS "src" RIGHT JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE "src"."id" IS NULL

-- statement: missing
WITH "_src_normalized" AS (SELECT "src"."id" AS "id" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."id" FROM "_src_normalized" AS "src" LEFT JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE "tgt"."id" IS NULL

-- statement: columns
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", CASE WHEN "src"."status" = 'A' THEN 'Active' ELSE "src"."status" END AS "status", "src"."tier" AS "tier" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."status" AS "status", "tgt"."tier" AS "tier" FROM "shop"."orders_v2" AS "tgt") SELECT SUM(CASE WHEN COALESCE("src"."status" IS NOT DISTINCT FROM "tgt"."status", FALSE) THEN 0 ELSE 1 END) AS "status", SUM(CASE WHEN COALESCE("src"."tier" IS NOT DISTINCT FROM "tgt"."tier", FALSE) THEN 0 ELSE 1 END) AS "tier" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id"

-- statement: value_maps
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", CASE WHEN "src"."status" = 'A' THEN 'Active' ELSE "src"."status" END AS "status", "src"."tier" AS "tier" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."status" AS "status", "tgt"."tier" AS "tier" FROM "shop"."orders_v2" AS "tgt"), "_veridelta_joined" AS (SELECT "src"."status" AS "_veridelta_source_0", "tgt"."status" AS "_veridelta_target_0", "src"."tier" AS "_veridelta_source_1", "tgt"."tier" AS "_veridelta_target_1" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE hash("src"."id") % 1000000 < 250000) SELECT "_veridelta_column", "_veridelta_source_value", "_veridelta_target_value", CAST("_veridelta_value_rows" AS BIGINT) AS "_veridelta_value_rows", CAST("_veridelta_agreeing_rows" AS BIGINT) AS "_veridelta_agreeing_rows" FROM (SELECT "_veridelta_column", "_veridelta_source_value", "_veridelta_target_value", "_veridelta_agreeing_rows", SUM("_veridelta_agreeing_rows") OVER (PARTITION BY "_veridelta_column", "_veridelta_source_value") AS "_veridelta_value_rows" FROM (SELECT "_veridelta_column", "_veridelta_source_value", "_veridelta_target_value", COUNT(*) AS "_veridelta_agreeing_rows" FROM (SELECT 0 AS "_veridelta_column", "_veridelta_source_0" AS "_veridelta_source_value", "_veridelta_target_0" AS "_veridelta_target_value" FROM "_veridelta_joined" WHERE "_veridelta_source_0" IS NOT NULL AND "_veridelta_source_0" NOT IN ('Active') UNION ALL SELECT 1 AS "_veridelta_column", "_veridelta_source_1" AS "_veridelta_source_value", "_veridelta_target_1" AS "_veridelta_target_value" FROM "_veridelta_joined" WHERE "_veridelta_source_1" IS NOT NULL) AS "_veridelta_pairs" GROUP BY "_veridelta_column", "_veridelta_source_value", "_veridelta_target_value") AS "_veridelta_groups") AS "_veridelta_counts" WHERE "_veridelta_target_value" <> "_veridelta_source_value" AND "_veridelta_agreeing_rows" >= 5 AND 2 * "_veridelta_agreeing_rows" > "_veridelta_value_rows"
