{{
  config(
    materialized = 'table',
    table_type='iceberg',
    format='parquet',
    table_properties={
      'optimize_rewrite_delete_file_threshold': '2'
    })
}}
WITH src_users AS (
  SELECT
    user_key,
    user_id,
    first_name,
    last_name,
    email,
    residence,
    latitude,
    longitude,
    CAST(created_at AS TIMESTAMP(6)) AS created_at
  FROM {{ ref('src_users') }}
)
SELECT
    *, 
    created_at AS valid_from,
    COALESCE(
      LEAD(created_at, 1) OVER (PARTITION BY user_id ORDER BY created_at), 
      CAST('2199-12-31' AS TIMESTAMP(6))
    ) AS valid_to
FROM src_users