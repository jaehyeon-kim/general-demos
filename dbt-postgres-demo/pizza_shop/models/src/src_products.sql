WITH raw_products AS (
  SELECT * FROM {{ source('raw', 'products') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['name', 'description', 'price', 'category', 'image']) }} as product_key,
  id AS product_id,
  name,
  description,
  price,
  category,
  image,
  created_at
FROM raw_products
