SELECT *
FROM {{ ref('fct_reviews') }} r
JOIN {{ ref('dim_listings_cleansed') }} l ON r.listing_id = l.listing_id
WHERE l.created_at >= r.review_date
LIMIT 10