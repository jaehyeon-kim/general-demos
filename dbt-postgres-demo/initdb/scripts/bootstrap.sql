-- // create schemas and grant permission

CREATE SCHEMA staging;
GRANT ALL ON SCHEMA staging TO devuser;

CREATE SCHEMA dev;
GRANT ALL ON SCHEMA dev TO devuser;

-- // create tables

DROP TABLE IF EXISTS staging.users;
DROP TABLE IF EXISTS staging.products;
DROP TABLE IF EXISTS staging.orders;

CREATE TABLE staging.users
(
    id SERIAL,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    residence VARCHAR(500),
    lat DECIMAL(10, 8),
    lon DECIMAL(10, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.products
(
    id SERIAL,
    name VARCHAR(100),
    description VARCHAR(500),
    price FLOAT,
    category VARCHAR(100),
    image VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.orders
(
    id SERIAL,
    user_id INT,
    items JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- // copy data

COPY staging.users(first_name, last_name, email, residence, lat, lon)
FROM '/tmp/users.csv' DELIMITER ',' CSV HEADER;

COPY staging.products(name, description, price, category, image)
FROM '/tmp/products.csv' DELIMITER ',' CSV HEADER;

COPY staging.orders(user_id, items)
FROM '/tmp/orders.csv' DELIMITER ',' CSV HEADER;

--
-- // for airflow metadata
--

CREATE DATABASE airflow;
CREATE ROLE airflow 
LOGIN 
PASSWORD 'airflow';
GRANT ALL ON DATABASE airflow TO airflow;