{{
  config(
    materialized = 'incremental',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['month(created_at)'],
    incremental_strategy='append',
    unique_key='order_id',
    table_properties={
      'optimize_rewrite_delete_file_threshold': '2'
    })
}}
WITH dim_products AS (
  SELECT * FROM {{ ref('dim_products') }}
), dim_users AS (
  SELECT * FROM {{ ref('dim_users') }}
), expanded_orders AS (
  SELECT 
    o.order_id,
    o.user_id,
    row.product_id,
    row.quantity,
    CAST(created_at AS TIMESTAMP(6)) AS created_at
  FROM {{ ref('src_orders') }} AS o
  CROSS JOIN UNNEST(CAST(JSON_PARSE(o.items) as ARRAY(ROW(product_id INTEGER, quantity INTEGER)))) as t(row)
)
SELECT
  o.order_id,
  ARRAY_AGG(
    CAST(
      ROW(p.product_key, o.product_id, p.name, p.price, o.quantity, p.description, p.category, p.image) 
        AS ROW(key VARCHAR, id BIGINT, name VARCHAR, price DOUBLE, quantity INTEGER, description VARCHAR, category VARCHAR, image VARCHAR))
  ) AS product,
  CAST(
    ROW(u.user_key, o.user_id, u.first_name, u.last_name, u.email, u.residence, u.latitude, u.longitude) 
      AS ROW(key VARCHAR, id BIGINT, first_name VARCHAR, last_name VARCHAR, email VARCHAR, residence VARCHAR, latitude DOUBLE, longitude DOUBLE)) AS user,
  o.created_at
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
