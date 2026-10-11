-- case: type_drift
-- dialect: redshift

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
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", "src"."units" AS "units", "src"."kept" AS "kept", "src"."forgiven" AS "forgiven" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."units" AS "units", "tgt"."kept" AS "kept", "tgt"."forgiven" AS "forgiven" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."id" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE NOT (COALESCE(("src"."units" IS NULL AND "tgt"."units" IS NULL), FALSE) AND COALESCE(("src"."kept" IS NULL AND "tgt"."kept" IS NULL), FALSE) AND COALESCE(("src"."forgiven" IS NULL AND "tgt"."forgiven" IS NULL), FALSE))

-- statement: added
WITH "_src_normalized" AS (SELECT "src"."id" AS "id" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id" FROM "shop"."orders_v2" AS "tgt") SELECT "tgt"."id" FROM "_src_normalized" AS "src" RIGHT JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE "src"."id" IS NULL

-- statement: missing
WITH "_src_normalized" AS (SELECT "src"."id" AS "id" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."id" FROM "_src_normalized" AS "src" LEFT JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE "tgt"."id" IS NULL

-- statement: columns
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", "src"."units" AS "units", "src"."kept" AS "kept", "src"."forgiven" AS "forgiven" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."units" AS "units", "tgt"."kept" AS "kept", "tgt"."forgiven" AS "forgiven" FROM "shop"."orders_v2" AS "tgt") SELECT SUM(CASE WHEN COALESCE(("src"."units" IS NULL AND "tgt"."units" IS NULL), FALSE) THEN 0 ELSE 1 END) AS "units", SUM(CASE WHEN COALESCE(("src"."kept" IS NULL AND "tgt"."kept" IS NULL), FALSE) THEN 0 ELSE 1 END) AS "kept", SUM(CASE WHEN COALESCE(("src"."forgiven" IS NULL AND "tgt"."forgiven" IS NULL), FALSE) THEN 0 ELSE 1 END) AS "forgiven" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id"

-- statement: sample
-- renames: {"_veridelta_key_0": "id", "_veridelta_source_0": "units_source", "_veridelta_target_0": "units_target", "_veridelta_match_0": "units_is_match", "_veridelta_source_1": "kept_source", "_veridelta_target_1": "kept_target", "_veridelta_match_1": "kept_is_match", "_veridelta_source_2": "forgiven_source", "_veridelta_target_2": "forgiven_target", "_veridelta_match_2": "forgiven_is_match"}
WITH "_src_normalized" AS (SELECT "src"."id" AS "id", "src"."units" AS "units", "src"."kept" AS "kept", "src"."forgiven" AS "forgiven" FROM "shop"."orders_v1" AS "src"), "_tgt_normalized" AS (SELECT "tgt"."id" AS "id", "tgt"."units" AS "units", "tgt"."kept" AS "kept", "tgt"."forgiven" AS "forgiven" FROM "shop"."orders_v2" AS "tgt") SELECT "src"."id" AS "_veridelta_key_0", "src"."units" AS "_veridelta_source_0", "tgt"."units" AS "_veridelta_target_0", COALESCE(("src"."units" IS NULL AND "tgt"."units" IS NULL), FALSE) AS "_veridelta_match_0", "src"."kept" AS "_veridelta_source_1", "tgt"."kept" AS "_veridelta_target_1", COALESCE(("src"."kept" IS NULL AND "tgt"."kept" IS NULL), FALSE) AS "_veridelta_match_1", "src"."forgiven" AS "_veridelta_source_2", "tgt"."forgiven" AS "_veridelta_target_2", COALESCE(("src"."forgiven" IS NULL AND "tgt"."forgiven" IS NULL), FALSE) AS "_veridelta_match_2" FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON "src"."id" = "tgt"."id" WHERE NOT (COALESCE(("src"."units" IS NULL AND "tgt"."units" IS NULL), FALSE) AND COALESCE(("src"."kept" IS NULL AND "tgt"."kept" IS NULL), FALSE) AND COALESCE(("src"."forgiven" IS NULL AND "tgt"."forgiven" IS NULL), FALSE)) ORDER BY "_veridelta_key_0" LIMIT 3
