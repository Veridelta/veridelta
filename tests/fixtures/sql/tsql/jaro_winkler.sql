-- case: jaro_winkler
-- dialect: tsql

-- statement: plan
-- refused: ConfigError: Column 'name' sets min_jaro_winkler_similarity, which warehouse pushdown cannot evaluate the way a local run does: Snowflake's JAROWINKLER_SIMILARITY ignores case and returns a whole number from 0 to 100, and Databricks has no Jaro-Winkler function. Use max_levenshtein_distance, which compiles to SQL, or compare file or lakehouse copies of these tables locally.
