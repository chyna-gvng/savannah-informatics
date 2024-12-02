-- User Summary
CREATE TABLE assessment_dataset.user_summary AS
SELECT
    u.user_id,
    u.first_name,
    SUM(c.total_cart_value) AS total_spent,
    SUM(c.quantity) AS total_items,
    u.age,
    u.city
FROM
    assessment_dataset.users_table u
JOIN
    assessment_dataset.carts_table c ON u.user_id = c.user_id
GROUP BY
    u.user_id, u.first_name, u.age, u.city;

-- Category Summary
CREATE TABLE assessment_dataset.category_summary AS
SELECT
    p.category,
    SUM(c.total_cart_value) AS total_sales,
    SUM(c.quantity) AS total_items_sold
FROM
    assessment_dataset.carts_table c
JOIN
    assessment_dataset.products_table p ON c.product_id = p.product_id
GROUP BY
    p.category;

-- Cart Details
CREATE TABLE assessment_dataset.cart_details AS
SELECT
    c.cart_id,
    c.user_id,
    c.product_id,
    c.quantity,
    p.price,
    c.total_cart_value
FROM
    assessment_dataset.carts_table c
JOIN
    assessment_dataset.products_table p ON c.product_id = p.product_id;