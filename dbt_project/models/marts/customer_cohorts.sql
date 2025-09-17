-- Customer cohorts mart
-- This model provides customer cohort analysis for retention insights

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- Get first order date for each customer
customer_first_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        count(*) as total_orders,
        sum(total_amount) as total_spent
    from orders
    group by customer_id
),

-- Get monthly cohorts
monthly_cohorts as (
    select
        customer_id,
        first_order_date,
        total_orders,
        total_spent,
        date_trunc('month', first_order_date) as cohort_month,
        extract(year from first_order_date) as cohort_year,
        extract(month from first_order_date) as cohort_month_num
    from customer_first_orders
),

-- Calculate retention for each cohort
cohort_retention as (
    select
        mc.customer_id,
        mc.first_order_date,
        mc.total_orders,
        mc.total_spent,
        mc.cohort_month,
        mc.cohort_year,
        mc.cohort_month_num,
        -- Check if customer made orders in subsequent months
        case when exists (
            select 1 from orders o 
            where o.customer_id = mc.customer_id 
            and date_trunc('month', o.order_date) > mc.cohort_month
        ) then 1 else 0 end as is_retained,
        -- Calculate months since first order
        case when exists (
            select 1 from orders o 
            where o.customer_id = mc.customer_id 
            and date_trunc('month', o.order_date) = date_trunc('month', mc.first_order_date + interval '1 month')
        ) then 1 else 0 end as month_1_retained,
        case when exists (
            select 1 from orders o 
            where o.customer_id = mc.customer_id 
            and date_trunc('month', o.order_date) = date_trunc('month', mc.first_order_date + interval '2 month')
        ) then 1 else 0 end as month_2_retained,
        case when exists (
            select 1 from orders o 
            where o.customer_id = mc.customer_id 
            and date_trunc('month', o.order_date) = date_trunc('month', mc.first_order_date + interval '3 month')
        ) then 1 else 0 end as month_3_retained
    from monthly_cohorts mc
),

-- Aggregate cohort metrics
cohort_metrics as (
    select
        cohort_month,
        cohort_year,
        cohort_month_num,
        count(*) as cohort_size,
        count(case when is_retained = 1 then 1 end) as retained_customers,
        count(case when month_1_retained = 1 then 1 end) as month_1_retained_customers,
        count(case when month_2_retained = 1 then 1 end) as month_2_retained_customers,
        count(case when month_3_retained = 1 then 1 end) as month_3_retained_customers,
        avg(total_orders) as avg_orders_per_customer,
        avg(total_spent) as avg_spent_per_customer,
        sum(total_spent) as total_cohort_revenue
    from cohort_retention
    group by cohort_month, cohort_year, cohort_month_num
),

final as (
    select
        cohort_month,
        cohort_year,
        cohort_month_num,
        cohort_size,
        retained_customers,
        month_1_retained_customers,
        month_2_retained_customers,
        month_3_retained_customers,
        avg_orders_per_customer,
        avg_spent_per_customer,
        total_cohort_revenue,
        -- Calculate retention rates
        case 
            when cohort_size > 0 
            then (retained_customers::decimal / cohort_size) * 100 
            else 0 
        end as overall_retention_rate,
        case 
            when cohort_size > 0 
            then (month_1_retained_customers::decimal / cohort_size) * 100 
            else 0 
        end as month_1_retention_rate,
        case 
            when cohort_size > 0 
            then (month_2_retained_customers::decimal / cohort_size) * 100 
            else 0 
        end as month_2_retention_rate,
        case 
            when cohort_size > 0 
            then (month_3_retained_customers::decimal / cohort_size) * 100 
            else 0 
        end as month_3_retention_rate,
        -- Metadata
        current_timestamp as dbt_updated_at,
        'marts' as dbt_model_type
    from cohort_metrics
)

select * from final
