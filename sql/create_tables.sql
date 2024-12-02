CREATE TABLE assessment_dataset.users_table (
    user_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    age INT64,
    street STRING,
    city STRING,
    postal_code STRING
);

CREATE TABLE assessment_dataset.products_table (
    product_id STRING,
    name STRING,
    category STRING,
    brand STRING,
    price FLOAT64
);

CREATE TABLE assessment_dataset.carts_table (
    cart_id STRING,
    user_id STRING,
    product_id STRING,
    quantity INT64,
    price FLOAT64,
    total_cart_value FLOAT64
);