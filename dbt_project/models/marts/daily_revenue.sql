-- Daily revenue mart
-- This model provides daily revenue metrics for business intelligence

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

daily_metrics as (
    select
        order_date,
        count(*) as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_order_value,
        min(total_amount) as min_order_value,
        max(total_amount) as max_order_value,
        sum(case when order_status_category = 'completed' then total_amount else 0 end) as completed_revenue,
        sum(case when order_status_category = 'cancelled' then total_amount else 0 end) as cancelled_revenue,
        count(case when order_status_category = 'completed' then 1 end) as completed_orders,
        count(case when order_status_category = 'cancelled' then 1 end) as cancelled_orders,
        count(case when order_status_category = 'in_progress' then 1 end) as in_progress_orders
    from orders
    group by order_date
),

customer_metrics as (
    select
        order_date,
        count(distinct case when c.country_cleaned = 'US' then o.customer_id end) as us_customers,
        count(distinct case when c.country_cleaned = 'CA' then o.customer_id end) as ca_customers,
        count(distinct case when c.country_cleaned = 'UK' then o.customer_id end) as uk_customers,
        count(distinct case when c.country_cleaned not in ('US', 'CA', 'UK') then o.customer_id end) as other_customers
    from orders o
    left join customers c on o.customer_id = c.customer_id
    group by order_date
),

final as (
    select
        dm.order_date,
        dm.total_orders,
        dm.unique_customers,
        dm.total_revenue,
        dm.avg_order_value,
        dm.min_order_value,
        dm.max_order_value,
        dm.completed_revenue,
        dm.cancelled_revenue,
        dm.completed_orders,
        dm.cancelled_orders,
        dm.in_progress_orders,
        -- Customer metrics
        cm.us_customers,
        cm.ca_customers,
        cm.uk_customers,
        cm.other_customers,
        -- Calculated metrics
        case 
            when dm.total_orders > 0 
            then (dm.completed_orders::decimal / dm.total_orders) * 100 
            else 0 
        end as completion_rate,
        case 
            when dm.total_orders > 0 
            then (dm.cancelled_orders::decimal / dm.total_orders) * 100 
            else 0 
        end as cancellation_rate,
        case 
            when dm.unique_customers > 0 
            then dm.total_revenue / dm.unique_customers 
            else 0 
        end as revenue_per_customer,
        -- Metadata
        current_timestamp as dbt_updated_at,
        'marts' as dbt_model_type
    from daily_metrics dm
    left join customer_metrics cm on dm.order_date = cm.order_date
)

select * from final



