{{
  config(
    materialized = 'incremental',
    partition_by = {
      'field': 'order_date',
      'data_type': 'date',
      'granularity': 'day',
      'time_ingestion_partitioning': true
    })
}}
WITH dim_products AS (
  SELECT * FROM {{ ref('dim_products') }}
), dim_users AS (
  SELECT * FROM {{ ref('dim_users') }}
), expanded_orders AS (
  SELECT 
    order_id,
    user_id,
    CAST(JSON_EXTRACT_SCALAR(json , '$.product_id') AS INTEGER) AS product_id,
    CAST(JSON_EXTRACT_SCALAR(json , '$.quantity') AS INTEGER) AS quantity,
    created_at
  FROM {{ ref('src_orders') }} AS t,
    UNNEST(JSON_EXTRACT_ARRAY(t.items , '$')) AS json
)
SELECT
  o.order_id,
  ARRAY_AGG(
    STRUCT(p.product_key AS key, o.product_id AS id, p.name, p.price, o.quantity, p.description, p.category, p.image)
  ) as product,
  STRUCT(u.user_key AS key, o.user_id AS id, u.first_name, u.last_name, u.email, u.residence, u.latitude, u.longitude) AS user,
  o.created_at,
  EXTRACT(DATE FROM o.created_at) AS order_date
FROM expanded_orders o
JOIN dim_products p 
  ON o.product_id = p.product_id
    AND o.created_at >= p.valid_from
    AND o.created_at < p.valid_to
JOIN dim_users u 
  ON o.user_id = u.user_id
    AND o.created_at >= u.valid_from
    AND o.created_at < u.valid_to
{% if is_incremental() %}
  WHERE o.created_at > (SELECT max(created_at) from {{ this }})
{% endif %}
GROUP BY 
  o.order_id, 
  u.user_key, 
  o.user_id, 
  u.first_name, 
  u.last_name, 
  u.email, 
  u.residence, 
  u.latitude, 
  u.longitude, 
  o.created_at
