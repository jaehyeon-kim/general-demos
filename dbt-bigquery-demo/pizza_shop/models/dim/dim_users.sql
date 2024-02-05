{{
  config(
    materialized = 'table',
    )
}}
WITH src_users AS (
  SELECT * FROM {{ ref('src_users') }}
)
SELECT
    *, 
    created_at AS valid_from,
    COALESCE(
      LEAD(created_at, 1) OVER (PARTITION BY user_id ORDER BY created_at), 
      CAST('2199-12-31' AS DATETIME)
    ) AS valid_to
FROM src_users