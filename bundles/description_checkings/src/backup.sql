CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.clear_str(str STRING)
RETURNS STRING
RETURN 
  REGEXP_REPLACE(str, '^[\\u0009\\u0020\\u3000]+|[\\u0009\\u0020\\u3000]+$', '');


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.normalize_text(prefix_text STRING, text STRING)
RETURNS STRING
RETURN CONCAT(prefix_text, REGEXP_REPLACE(dataplatform_public_published_dev.datasteward.clear_str(text), '(?<!  )\\r?\\n', CONCAT('  ', CHR(10))), "  \n");


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.normalize_url(prefix_text STRING, link_text STRING, url STRING)
RETURNS STRING
RETURN CONCAT(prefix_text, "[", link_text, "](", dataplatform_public_published_dev.datasteward.clear_str(url), ")  \n");


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.generate_description_schema(name_ja STRING, explanation STRING)
RETURNS STRING
RETURN
  CONCAT(
    CONCAT("データソース名/システム名：", dataplatform_public_published_dev.datasteward.clear_str(name_ja), "  \n"),
    CONCAT("Overview：このスキーマに含まれるデータは、主に上記のシステムから取り込まれています。", "  \n"),
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(explanation) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_text("データソース概要：", explanation)
      ELSE ""
    END,
    "連絡先：[#sys-データ基盤の相談や情報共有](https://www.youtube.com/)"
  );


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.generate_description_table(name_ja STRING, explanation STRING, type_conversion STRING, rule STRING, cron_schedule STRING, query STRING, reference STRING, link STRING)
RETURNS STRING
RETURN
  CONCAT(
    CONCAT("TableName：", dataplatform_public_published_dev.datasteward.clear_str(name_ja), "  \n"),
    "Overview：  \n",
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(explanation) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_text("", explanation)
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(type_conversion) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_text("", type_conversion)
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(rule) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_text("", rule)
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(cron_schedule) != ""
      THEN CONCAT("更新頻度：", dataplatform_public_published_dev.datasteward.clear_str(cron_schedule), "  \n")
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(link) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_url("元テーブル仕様書：", "link", link)
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(query) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_url("作成クエリ：", "query", query)
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(reference) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_url("対応チケット：", "reference", reference)
      ELSE ""
    END,
    "連絡先：[#sys-データ基盤の相談や情報共有](https://www.youtube.com/)"
  );


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.generate_description_view(name_ja STRING, cron_schedule STRING, reference STRING)
RETURNS STRING
RETURN
  CONCAT(
    CONCAT("TableName：", dataplatform_public_published_dev.datasteward.clear_str(name_ja), "  \n"),
    "Overview：元テーブル名から必要なカラムを取り出したビュー。詳細を確認したい場合は、元テーブルを参照してください。  \n",
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(cron_schedule) != ""
      THEN CONCAT("更新頻度：", dataplatform_public_published_dev.datasteward.clear_str(cron_schedule), "  \n")
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(reference) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_url("対応チケット：", "reference", reference)
      ELSE ""
    END,
    "連絡先：[#sys-データ基盤の相談や情報共有](https://www.youtube.com/)"
  );


WITH description_settings AS (
  SELECT
    LOWER(catalog)                AS catalog_name,
    LOWER(schema)                 AS schema_name,
    LOWER(COALESCE(table, ''))    AS table_name,
    COALESCE(name_ja, '')         AS name_ja,
    table_category                AS table_category,
    COALESCE(cron_schedule, '')   AS cron_schedule,
    COALESCE(explanation, '')     AS explanation,
    COALESCE(type_conversion, '') AS type_conversion,
    COALESCE(rule, '')            AS rule,
    COALESCE(link, '')            AS link,
    COALESCE(query, '')           AS query,
    COALESCE(reference, '')       AS reference
  FROM
    dataplatform_public_published_dev.datasteward.description_settings
  WHERE
    catalog RLIKE '_dev$'
),
actual_description AS (
  SELECT
    LOWER(catalog_name) AS catalog_name,
    LOWER(schema_name)  AS schema_name,
    NULL                AS table_name,
    0                   AS table_category,
    comment             AS description
  FROM
    system.information_schema.schemata
  WHERE TRUE
    AND catalog_name NOT IN ("system", "samples")
    AND schema_name NOT IN ("information_schema")

  UNION ALL

  SELECT
    LOWER(table_catalog) AS catalog_name,
    LOWER(table_schema)  AS schema_name,
    LOWER(table_name)    AS table_name,
    CASE table_type
      WHEN "MANAGED" THEN 1
      WHEN "MATERIALIZED_VIEW" THEN 2
      WHEN "VIEW" THEN 3
      ELSE NULL
    END AS table_category,
    comment             AS description
  FROM
    system.information_schema.tables
  WHERE TRUE
    AND table_catalog NOT IN ("system", "samples")
    AND table_schema NOT IN ("information_schema")
  ORDER BY 
    catalog_name,
    schema_name,
    table_name
),
current_descriptions AS (
  SELECT *, dataplatform_public_published_dev.datasteward.generate_description_schema(name_ja, explanation) AS description
  FROM description_settings
  WHERE table_category = 0

  UNION ALL

  SELECT *, dataplatform_public_published_dev.datasteward.generate_description_table(name_ja, explanation, type_conversion, rule, cron_schedule, query, reference, link) AS description
  FROM description_settings
  WHERE table_category = 1 AND catalog_name LIKE '%staging%'

  UNION ALL

  SELECT *, dataplatform_public_published_dev.datasteward.generate_description_table(name_ja, explanation, type_conversion, rule, cron_schedule, query, reference, "") AS description
  FROM description_settings
  WHERE table_category = 1 AND (catalog_name LIKE '%conformed%' OR catalog_name LIKE '%published%')

  UNION ALL

  SELECT *, dataplatform_public_published_dev.datasteward.generate_description_view(name_ja, cron_schedule, reference) AS description
  FROM description_settings
  WHERE table_category = 2 OR table_category = 3
)
SELECT 
  COALESCE(actual_description.catalog_name, current_descriptions.catalog_name) AS catalog_name,
  COALESCE(actual_description.schema_name, current_descriptions.schema_name) AS schema_name,
  COALESCE(actual_description.table_name, current_descriptions.table_name) AS table_name,
  COALESCE(actual_description.table_category, current_descriptions.table_category) AS table_category,
  current_descriptions.name_ja,
  current_descriptions.cron_schedule,
  current_descriptions.explanation,
  current_descriptions.type_conversion,
  current_descriptions.rule,
  current_descriptions.link,
  current_descriptions.query,
  current_descriptions.reference,
  actual_description.description AS actual_description,
  current_descriptions.description AS current_description,
  CASE
    WHEN actual_description.table_category   IS NULL AND current_descriptions.table_category IS NOT NULL THEN 1
    WHEN current_descriptions.table_category IS NULL AND actual_description.table_category   IS NOT NULL THEN 2
    WHEN actual_description.description = current_descriptions.description THEN 0
    ELSE 3
  END AS status
FROM 
  actual_description
  FULL OUTER JOIN current_descriptions
    ON actual_description.catalog_name = current_descriptions.catalog_name
    AND actual_description.schema_name = current_descriptions.schema_name
    AND actual_description.table_name = current_descriptions.table_name
    AND actual_description.table_category = current_descriptions.table_category
