-- case: baseline_keys
-- dialect: redshift

-- statement: probe
SELECT * FROM "shop"."orders_v1" WHERE 1 = 0

-- statement: probe
SELECT * FROM "shop"."orders_v2" WHERE 1 = 0

-- statement: duplicates source
SELECT COALESCE(SUM("_veridelta_rows"), 0) AS "_veridelta_total" FROM (SELECT COUNT(*) AS "_veridelta_rows" FROM (SELECT "src"."id" AS "id", "src"."code" AS "code", "src"."day" AS "day", "src"."at" AS "at", "src"."on" AS "on" FROM "shop"."orders_v1" AS "src") AS "_veridelta_keys" GROUP BY "id", COLLATE("code" || '|', 'case_sensitive'), "day", "at", "on" HAVING COUNT(*) > 1) AS "_veridelta_duplicates"

-- statement: duplicates target
SELECT COALESCE(SUM("_veridelta_rows"), 0) AS "_veridelta_total" FROM (SELECT COUNT(*) AS "_veridelta_rows" FROM (SELECT "tgt"."id" AS "id", "tgt"."code" AS "code", "tgt"."day" AS "day", "tgt"."at" AS "at", "tgt"."on" AS "on" FROM "shop"."orders_v2" AS "tgt") AS "_veridelta_keys" GROUP BY "id", COLLATE("code" || '|', 'case_sensitive'), "day", "at", "on" HAVING COUNT(*) > 1) AS "_veridelta_duplicates"

-- statement: count source
SELECT COUNT(*) AS "_veridelta_total" FROM "shop"."orders_v1"

-- statement: count target
SELECT COUNT(*) AS "_veridelta_total" FROM "shop"."orders_v2"

-- statement: changed
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", "src"."code" AS "code", "src"."day" AS "day", "src"."at" AS "at", "src"."on" AS "on", "src"."total" AS "total" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."code" AS "code", "tgt"."day" AS "day", "tgt"."at" AS "at", "tgt"."on" AS "on", "tgt"."total" AS "total" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."id", "src"."code", "src"."day", "src"."at", "src"."on" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" AND COLLATE("src"."code" || '|', 'case_sensitive') = COLLATE("tgt"."code" || '|', 'case_sensitive') AND "src"."day" = "tgt"."day" AND "src"."at" = "tgt"."at" AND "src"."on" = "tgt"."on" WHERE NOT (COALESCE(("src"."total" = "tgt"."total" OR ("src"."total" IS NULL AND "tgt"."total" IS NULL)), FALSE))

-- statement: added
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", "src"."code" AS "code", "src"."day" AS "day", "src"."at" AS "at", "src"."on" AS "on" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."code" AS "code", "tgt"."day" AS "day", "tgt"."at" AS "at", "tgt"."on" AS "on" FROM "shop"."orders_v2" AS "tgt") SELECT "tgt"."id", "tgt"."code", "tgt"."day", "tgt"."at", "tgt"."on" FROM "_src_normalized" AS "src" RIGHT JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" AND COLLATE("src"."code" || '|', 'case_sensitive') = COLLATE("tgt"."code" || '|', 'case_sensitive') AND "src"."day" = "tgt"."day" AND "src"."at" = "tgt"."at" AND "src"."on" = "tgt"."on" WHERE "src"."id" IS NULL AND "src"."code" IS NULL AND "src"."day" IS NULL AND "src"."at" IS NULL AND "src"."on" IS NULL

-- statement: missing
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", "src"."code" AS "code", "src"."day" AS "day", "src"."at" AS "at", "src"."on" AS "on" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."code" AS "code", "tgt"."day" AS "day", "tgt"."at" AS "at", "tgt"."on" AS "on" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."id", "src"."code", "src"."day", "src"."at", "src"."on" FROM "_src_normalized" AS "src" LEFT JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" AND COLLATE("src"."code" || '|', 'case_sensitive') = COLLATE("tgt"."code" || '|', 'case_sensitive') AND "src"."day" = "tgt"."day" AND "src"."at" = "tgt"."at" AND "src"."on" = "tgt"."on" WHERE "tgt"."id" IS NULL AND "tgt"."code" IS NULL AND "tgt"."day" IS NULL AND "tgt"."at" IS NULL AND "tgt"."on" IS NULL

-- statement: columns
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", "src"."code" AS "code", "src"."day" AS "day", "src"."at" AS "at", "src"."on" AS "on", "src"."total" AS "total" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."code" AS "code", "tgt"."day" AS "day", "tgt"."at" AS "at", "tgt"."on" AS "on", "tgt"."total" AS "total" FROM "shop"."orders_v2" AS "tgt") SELECT SUM(CASE WHEN COALESCE(("src"."total" = "tgt"."total" OR ("src"."total" IS NULL AND "tgt"."total" IS NULL)), FALSE) THEN 0 ELSE 1 END) AS "total" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" AND COLLATE("src"."code" || '|', 'case_sensitive') = COLLATE("tgt"."code" || '|', 'case_sensitive') AND "src"."day" = "tgt"."day" AND "src"."at" = "tgt"."at" AND "src"."on" = "tgt"."on"

-- statement: flags
-- renames: {"_veridelta_key_0": "id", "_veridelta_key_1": "code", "_veridelta_key_2": "day", "_veridelta_key_3": "at", "_veridelta_key_4": "on", "_veridelta_flag_0": "total_is_match"}
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", "src"."code" AS "code", "src"."day" AS "day", "src"."at" AS "at", "src"."on" AS "on", "src"."total" AS "total" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."code" AS "code", "tgt"."day" AS "day", "tgt"."at" AS "at", "tgt"."on" AS "on", "tgt"."total" AS "total" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."id" AS "_veridelta_key_0", "src"."code" AS "_veridelta_key_1", "src"."day" AS "_veridelta_key_2", "src"."at" AS "_veridelta_key_3", "src"."on" AS "_veridelta_key_4", COALESCE(("src"."total" = "tgt"."total" OR ("src"."total" IS NULL AND "tgt"."total" IS NULL)), FALSE) AS "_veridelta_flag_0" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" AND COLLATE("src"."code" || '|', 'case_sensitive') = COLLATE("tgt"."code" || '|', 'case_sensitive') AND "src"."day" = "tgt"."day" AND "src"."at" = "tgt"."at" AND "src"."on" = "tgt"."on" WHERE NOT (COALESCE(("src"."total" = "tgt"."total" OR ("src"."total" IS NULL AND "tgt"."total" IS NULL)), FALSE)) AND (("src"."id" = 1 AND COLLATE("src"."code" || '|', 'case_sensitive') = COLLATE('O''Brien' || '|', 'case_sensitive') AND "src"."day" = CAST('2026-10-06' AS DATE) AND "src"."at" = CAST('2026-10-06T09:30:00' AS TIMESTAMP) AND "src"."on" = TRUE) OR ("src"."id" = 2 AND COLLATE("src"."code" || '|', 'case_sensitive') = COLLATE('C:\\path\\' || '|', 'case_sensitive') AND "src"."day" = CAST('2026-01-02' AS DATE) AND "src"."at" IS NULL AND "src"."on" = FALSE) OR ("src"."id" IS NULL AND "src"."code" IS NULL AND "src"."day" IS NULL AND "src"."at" = CAST('2026-01-02 03:04:05' AS TIMESTAMP) AND "src"."on" IS NULL))
