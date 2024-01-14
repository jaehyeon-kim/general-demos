{{
  config(
    materialized = 'incremental'
    )
}}
WITH dim_products AS (
  SELECT * FROM {{ ref('dim_products') }}
), dim_users AS (
  SELECT * FROM {{ ref('dim_users') }}
), src_orders AS (
  SELECT 
    order_id,
    user_id,
    jsonb_array_elements(items) AS order_item,
    created_at    
  FROM {{ ref('src_orders') }}
), expanded_orders AS (
  SELECT 
    order_id,
    user_id,
    (order_item ->> 'product_id')::INT AS product_id,
    (order_item ->> 'quantity')::INT AS quantity,    
    created_at
  FROM src_orders
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['order_id', 'p.product_id', 'u.user_id']) }} as order_key,
  p.product_key,
  u.user_key,
  o.order_id,
  o.user_id,
  o.product_id,
  o.quantity,
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
  WHERE o.created_at > (SELECT created_at from {{ this }} ORDER BY created_at DESC LIMIT 1)
{% endif %}