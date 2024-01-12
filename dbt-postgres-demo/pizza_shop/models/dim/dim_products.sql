{{
  config(
    materialized = 'table',
    )
}}
WITH src_products AS (
  SELECT * FROM {{ ref('src_products') }}
)
SELECT
    *, 
    created_at AS valid_from,
    COALESCE(
      LEAD(created_at, 1) OVER (PARTITION BY product_id ORDER BY created_at), 
      '2199-12-31'::TIMESTAMP
    ) AS valid_to
FROM src_products