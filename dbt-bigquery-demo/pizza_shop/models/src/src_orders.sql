WITH raw_orders AS (
  SELECT * FROM {{ source('raw', 'orders') }}
)
SELECT
  id AS order_id,
  user_id,
  items,
  created_at
FROM raw_orders