-- Data quality tests for the enterprise data pipeline

-- Test that orders table has no null order_ids
select count(*) as failures
from {{ ref('stg_orders') }}
where order_id is null

-- Test that customers table has no null customer_ids
select count(*) as failures
from {{ ref('stg_customers') }}
where customer_id is null

-- Test that products table has no null product_ids
select count(*) as failures
from {{ ref('stg_products') }}
where product_id is null

-- Test that order amounts are positive
select count(*) as failures
from {{ ref('stg_orders') }}
where total_amount <= 0

-- Test that product prices are positive
select count(*) as failures
from {{ ref('stg_products') }}
where price <= 0

-- Test that order quantities are positive
select count(*) as failures
from {{ ref('stg_orders') }}
where quantity <= 0
