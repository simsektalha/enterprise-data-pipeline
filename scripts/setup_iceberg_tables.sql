-- Setup Iceberg tables for ACID transactions
-- This script creates sample Iceberg tables with different configurations

-- Create Iceberg catalog
CREATE SCHEMA IF NOT EXISTS iceberg.default;

-- Create orders table with Iceberg format
CREATE TABLE iceberg.default.orders (
    order_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    quantity INTEGER,
    price DECIMAL(10,2),
    order_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) WITH (
    format = 'PARQUET',
    location = 's3://data-lake/iceberg/orders'
);

-- Create customers table with Iceberg format
CREATE TABLE iceberg.default.customers (
    customer_id BIGINT,
    name VARCHAR(255),
    email VARCHAR(255),
    country VARCHAR(100),
    registration_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) WITH (
    format = 'PARQUET',
    location = 's3://data-lake/iceberg/customers'
);

-- Create products table with Iceberg format
CREATE TABLE iceberg.default.products (
    product_id BIGINT,
    name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2),
    description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) WITH (
    format = 'PARQUET',
    location = 's3://data-lake/iceberg/products'
);

-- Create daily_revenue table with Iceberg format (partitioned by date)
CREATE TABLE iceberg.default.daily_revenue (
    revenue_date DATE,
    total_orders BIGINT,
    total_revenue DECIMAL(15,2),
    avg_order_value DECIMAL(10,2),
    unique_customers BIGINT,
    created_at TIMESTAMP
) WITH (
    format = 'PARQUET',
    location = 's3://data-lake/iceberg/daily_revenue',
    partitioning = ARRAY['revenue_date']
);

-- Create customer_cohorts table with Iceberg format
CREATE TABLE iceberg.default.customer_cohorts (
    cohort_month VARCHAR(7),
    customer_id BIGINT,
    first_order_date DATE,
    cohort_size BIGINT,
    retention_rate DECIMAL(5,4),
    created_at TIMESTAMP
) WITH (
    format = 'PARQUET',
    location = 's3://data-lake/iceberg/customer_cohorts',
    partitioning = ARRAY['cohort_month']
);

-- Insert sample data into orders table
INSERT INTO iceberg.default.orders VALUES
(1, 101, 1001, 2, 99.99, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-01-01 10:00:00'),
(2, 102, 1002, 1, 149.99, DATE '2024-01-01', TIMESTAMP '2024-01-01 11:00:00', TIMESTAMP '2024-01-01 11:00:00'),
(3, 101, 1003, 3, 79.99, DATE '2024-01-02', TIMESTAMP '2024-01-02 09:00:00', TIMESTAMP '2024-01-02 09:00:00'),
(4, 103, 1001, 1, 99.99, DATE '2024-01-02', TIMESTAMP '2024-01-02 14:00:00', TIMESTAMP '2024-01-02 14:00:00'),
(5, 102, 1004, 2, 199.99, DATE '2024-01-03', TIMESTAMP '2024-01-03 16:00:00', TIMESTAMP '2024-01-03 16:00:00');

-- Insert sample data into customers table
INSERT INTO iceberg.default.customers VALUES
(101, 'John Doe', 'john.doe@email.com', 'US', DATE '2023-12-01', TIMESTAMP '2023-12-01 00:00:00', TIMESTAMP '2023-12-01 00:00:00'),
(102, 'Jane Smith', 'jane.smith@email.com', 'CA', DATE '2023-11-15', TIMESTAMP '2023-11-15 00:00:00', TIMESTAMP '2023-11-15 00:00:00'),
(103, 'Bob Johnson', 'bob.johnson@email.com', 'UK', DATE '2023-10-20', TIMESTAMP '2023-10-20 00:00:00', TIMESTAMP '2023-10-20 00:00:00');

-- Insert sample data into products table
INSERT INTO iceberg.default.products VALUES
(1001, 'Laptop Pro', 'Electronics', 999.99, 'High-performance laptop', TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-01-01 00:00:00'),
(1002, 'Wireless Mouse', 'Electronics', 29.99, 'Ergonomic wireless mouse', TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-01-01 00:00:00'),
(1003, 'Office Chair', 'Furniture', 199.99, 'Comfortable office chair', TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-01-01 00:00:00'),
(1004, 'Monitor 4K', 'Electronics', 399.99, '27-inch 4K monitor', TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-01-01 00:00:00');

-- Show created tables
SHOW TABLES FROM iceberg.default;



