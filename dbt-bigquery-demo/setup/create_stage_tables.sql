DROP TABLE IF EXISTS pizza_shop.staging_users;
DROP TABLE IF EXISTS pizza_shop.staging_products;
DROP TABLE IF EXISTS pizza_shop.staging_orders;

CREATE TABLE pizza_shop.staging_users
(
    id INTEGER,
    first_name STRING,
    last_name STRING,
    email STRING,
    residence STRING,
    lat DECIMAL(10, 8),
    lon DECIMAL(10, 8),
    created_at DATETIME
);

CREATE TABLE pizza_shop.staging_products
(
    id INTEGER,
    name STRING,
    description STRING,
    price FLOAT64,
    category STRING,
    image STRING,
    created_at DATETIME
);

CREATE TABLE pizza_shop.staging_orders
(
    id INTEGER,
    user_id INTEGER,
    items JSON,
    created_at DATETIME
);