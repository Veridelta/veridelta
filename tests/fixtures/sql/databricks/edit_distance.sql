-- case: edit_distance
-- dialect: databricks

-- statement: probe
SELECT * FROM `shop`.`orders_v1` WHERE 1 = 0

-- statement: probe
SELECT * FROM `shop`.`orders_v2` WHERE 1 = 0

-- statement: duplicates source
SELECT COALESCE(SUM(`_veridelta_rows`), 0) AS `_veridelta_total` FROM (SELECT COUNT(*) AS `_veridelta_rows` FROM (SELECT `src`.`id` AS `id` FROM `shop`.`orders_v1` AS `src`) AS `_veridelta_keys` GROUP BY `id` HAVING COUNT(*) > 1) AS `_veridelta_duplicates`

-- statement: duplicates target
SELECT COALESCE(SUM(`_veridelta_rows`), 0) AS `_veridelta_total` FROM (SELECT COUNT(*) AS `_veridelta_rows` FROM (SELECT `tgt`.`id` AS `id` FROM `shop`.`orders_v2` AS `tgt`) AS `_veridelta_keys` GROUP BY `id` HAVING COUNT(*) > 1) AS `_veridelta_duplicates`

-- statement: count source
SELECT COUNT(*) AS `_veridelta_total` FROM `shop`.`orders_v1`

-- statement: count target
SELECT COUNT(*) AS `_veridelta_total` FROM `shop`.`orders_v2`

-- statement: changed
WITH `_src_normalized` AS (SELECT `src`.`id` AS `id`, `src`.`name` AS `name` FROM `shop`.`orders_v1` AS `src`), `_tgt_normalized` AS (SELECT `tgt`.`id` AS `id`, `tgt`.`name` AS `name` FROM `shop`.`orders_v2` AS `tgt`) SELECT `src`.`id` FROM `_src_normalized` AS `src` INNER JOIN `_tgt_normalized` AS `tgt` ON `src`.`id` = `tgt`.`id` WHERE NOT (COALESCE((`src`.`name` IS NULL AND `tgt`.`name` IS NULL) OR ((`src`.`name` = `tgt`.`name` OR levenshtein(`src`.`name`, `tgt`.`name`) <= 2)), FALSE))

-- statement: added
WITH `_src_normalized` AS (SELECT `src`.`id` AS `id` FROM `shop`.`orders_v1` AS `src`), `_tgt_normalized` AS (SELECT `tgt`.`id` AS `id` FROM `shop`.`orders_v2` AS `tgt`) SELECT `tgt`.`id` FROM `_src_normalized` AS `src` RIGHT JOIN `_tgt_normalized` AS `tgt` ON `src`.`id` = `tgt`.`id` WHERE `src`.`id` IS NULL

-- statement: missing
WITH `_src_normalized` AS (SELECT `src`.`id` AS `id` FROM `shop`.`orders_v1` AS `src`), `_tgt_normalized` AS (SELECT `tgt`.`id` AS `id` FROM `shop`.`orders_v2` AS `tgt`) SELECT `src`.`id` FROM `_src_normalized` AS `src` LEFT JOIN `_tgt_normalized` AS `tgt` ON `src`.`id` = `tgt`.`id` WHERE `tgt`.`id` IS NULL

-- statement: columns
WITH `_src_normalized` AS (SELECT `src`.`id` AS `id`, `src`.`name` AS `name` FROM `shop`.`orders_v1` AS `src`), `_tgt_normalized` AS (SELECT `tgt`.`id` AS `id`, `tgt`.`name` AS `name` FROM `shop`.`orders_v2` AS `tgt`) SELECT SUM(CASE WHEN COALESCE((`src`.`name` IS NULL AND `tgt`.`name` IS NULL) OR ((`src`.`name` = `tgt`.`name` OR levenshtein(`src`.`name`, `tgt`.`name`) <= 2)), FALSE) THEN 0 ELSE 1 END) AS `name` FROM `_src_normalized` AS `src` INNER JOIN `_tgt_normalized` AS `tgt` ON `src`.`id` = `tgt`.`id`
