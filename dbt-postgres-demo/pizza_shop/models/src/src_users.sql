WITH raw_users AS (
  SELECT * FROM {{ source('raw', 'users') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name', 'email', 'residence', 'lat', 'lon']) }} as user_key,
  id AS user_id,
  first_name,
  last_name,
  email,
  residence,
  lat AS latitude,
  lon AS longitude,
  created_at
FROM raw_users