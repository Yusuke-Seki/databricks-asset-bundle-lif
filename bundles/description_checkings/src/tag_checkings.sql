WITH current_tags AS (
  SELECT
    LOWER(catalog)                AS catalog_name,
    LOWER(schema)                 AS schema_name,
    LOWER(COALESCE(table, ''))    AS table_name,
    table_category                AS table_category,
    COALESCE(tag_key, '')         AS tag_key,
    COALESCE(tag_value, '')       AS tag_value
  FROM
    dataplatform_public_published_dev.datasteward.tag_settings
  WHERE
    catalog RLIKE '_dev$'
),
actual_tags AS (
  SELECT
    LOWER(catalog_name) AS catalog_name,
    LOWER(schema_name)  AS schema_name,
    ''                  AS table_name,
    0                   AS table_category,
    tag_name            AS tag_key,
    tag_value           AS tag_value
  FROM
    system.information_schema.schema_tags
  WHERE TRUE
    AND catalog_name NOT IN ("system", "samples")
    AND schema_name  NOT IN ("information_schema")

  UNION ALL

  SELECT
    LOWER(tags.catalog_name) AS catalog_name,
    LOWER(tags.schema_name)  AS schema_name,
    LOWER(tags.table_name)   AS table_name,
    CASE tables.table_type
      WHEN "MANAGED" THEN 1
      WHEN "MATERIALIZED_VIEW" THEN 2
      WHEN "VIEW" THEN 3
      ELSE NULL
    END AS table_category,
    tags.tag_name            AS tag_key,
    tags.tag_value           AS tag_value
  FROM
    system.information_schema.table_tags AS tags
    LEFT JOIN system.information_schema.tables tables
    ON tags.catalog_name = tables.table_catalog
    AND tags.schema_name = tables.table_schema
    AND tags.table_name = tables.table_name
  WHERE TRUE
    AND catalog_name NOT IN ("system", "samples")
    AND schema_name  NOT IN ("information_schema")
)
SELECT
  COALESCE(actual_tags.catalog_name, current_tags.catalog_name) AS catalog_name,
  COALESCE(actual_tags.schema_name, current_tags.schema_name) AS schema_name,
  COALESCE(actual_tags.table_name, current_tags.table_name) AS table_name,
  COALESCE(actual_tags.table_category, current_tags.table_category) AS table_category,
  actual_tags.tag_key AS actual_tag_key,
  actual_tags.tag_value AS actual_tag_value,
  current_tags.tag_key AS current_tag_key,
  current_tags.tag_value AS current_tag_value,
  CASE
    WHEN actual_tags.tag_key   IS NULL AND current_tags.tag_key IS NOT NULL THEN 1
    WHEN current_tags.tag_key  IS NULL AND actual_tags.tag_key  IS NOT NULL THEN 2
    WHEN (actual_tags.tag_key = current_tags.tag_key) AND (actual_tags.tag_value = current_tags.tag_value) THEN 0
    ELSE 3
END AS status
FROM 
  actual_tags
  FULL OUTER JOIN current_tags
  ON actual_tags.catalog_name = current_tags.catalog_name
  AND actual_tags.schema_name = current_tags.schema_name
  AND actual_tags.table_name = current_tags.table_name
  AND actual_tags.table_category = current_tags.table_category
  AND actual_tags.tag_key = current_tags.tag_key
