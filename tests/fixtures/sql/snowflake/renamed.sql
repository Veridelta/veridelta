-- case: renamed
-- dialect: snowflake

-- statement: probe
SELECT * FROM "shop"."orders_v1" WHERE 1 = 0

-- statement: probe
SELECT * FROM "shop"."orders_v2" WHERE 1 = 0

-- statement: duplicates source
SELECT COALESCE(SUM("_veridelta_rows"), 0) AS "_veridelta_total" FROM (SELECT COUNT(*) AS "_veridelta_rows" FROM (SELECT "src"."customer_id" AS "cust_id" FROM "shop"."orders_v1" AS "src") AS "_veridelta_keys" GROUP BY "cust_id" HAVING COUNT(*) > 1) AS "_veridelta_duplicates"

-- statement: duplicates target
SELECT COALESCE(SUM("_veridelta_rows"), 0) AS "_veridelta_total" FROM (SELECT COUNT(*) AS "_veridelta_rows" FROM (SELECT "tgt"."cust_id" AS "cust_id" FROM "shop"."orders_v2" AS "tgt") AS "_veridelta_keys" GROUP BY "cust_id" HAVING COUNT(*) > 1) AS "_veridelta_duplicates"

-- statement: count source
SELECT COUNT(*) AS "_veridelta_total" FROM "shop"."orders_v1"

-- statement: count target
SELECT COUNT(*) AS "_veridelta_total" FROM "shop"."orders_v2"

-- statement: changed
WITH "_src_normalized" AS (SELECT "src"."customer_id" AS "cust_id", LOWER("src"."full_name") AS "name" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."cust_id" AS "cust_id", LOWER("tgt"."name") AS "name" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."cust_id" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."cust_id" = "tgt"."cust_id" WHERE NOT (COALESCE(EQUAL_NULL("src"."name", "tgt"."name"), FALSE))

-- statement: added
WITH "_src_normalized" AS (SELECT "src"."customer_id" AS "cust_id" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."cust_id" AS "cust_id" FROM "shop"."orders_v2" AS "tgt") SELECT "tgt"."cust_id" FROM "_src_normalized" AS "src" RIGHT JOIN "_tgt_normalized" AS "tgt" ON "src"."cust_id" = "tgt"."cust_id" WHERE "src"."cust_id" IS NULL

-- statement: missing
WITH "_src_normalized" AS (SELECT "src"."customer_id" AS "cust_id" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."cust_id" AS "cust_id" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."cust_id" FROM "_src_normalized" AS "src" LEFT JOIN "_tgt_normalized" AS "tgt" ON "src"."cust_id" = "tgt"."cust_id" WHERE "tgt"."cust_id" IS NULL

-- statement: columns
WITH "_src_normalized" AS (SELECT "src"."customer_id" AS "cust_id", LOWER("src"."full_name") AS "name" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."cust_id" AS "cust_id", LOWER("tgt"."name") AS "name" FROM "shop"."orders_v2" AS "tgt") SELECT SUM(CASE WHEN COALESCE(EQUAL_NULL("src"."name", "tgt"."name"), FALSE) THEN 0 ELSE 1 END) AS "name" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."cust_id" = "tgt"."cust_id"

-- statement: sample
-- renames: {"_veridelta_key_0": "cust_id", "_veridelta_source_0": "name_source", "_veridelta_target_0": "name_target", "_veridelta_match_0": "name_is_match"}
WITH "_src_normalized" AS (SELECT "src"."customer_id" AS "cust_id", LOWER("src"."full_name") AS "name" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."cust_id" AS "cust_id", LOWER("tgt"."name") AS "name" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."cust_id" AS "_veridelta_key_0", "src"."name" AS "_veridelta_source_0", "tgt"."name" AS "_veridelta_target_0", COALESCE(EQUAL_NULL("src"."name", "tgt"."name"), FALSE) AS "_veridelta_match_0" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."cust_id" = "tgt"."cust_id" WHERE NOT (COALESCE(EQUAL_NULL("src"."name", "tgt"."name"), FALSE)) ORDER BY "_veridelta_key_0" LIMIT 2
