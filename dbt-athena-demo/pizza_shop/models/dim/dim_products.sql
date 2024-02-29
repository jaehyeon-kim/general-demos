{{
  config(
    materialized = 'table',
    table_type='iceberg',
    format='parquet',
    table_properties={
      'optimize_rewrite_delete_file_threshold': '2'
    })
}}
WITH src_products AS (
  SELECT
    product_key,
    product_id,
    name,
    description,
    price,
    category,
    image,
    CAST(created_at AS TIMESTAMP(6)) AS created_at
  FROM {{ ref('src_products') }}
)
SELECT
    *, 
    created_at AS valid_from,
    COALESCE(
      LEAD(created_at, 1) OVER (PARTITION BY product_id ORDER BY created_at), 
      CAST('2199-12-31' AS TIMESTAMP(6))
    ) AS valid_to
FROM src_products