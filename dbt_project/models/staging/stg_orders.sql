-- Staging model for orders data
-- This model cleans and standardizes the raw orders data

with source_data as (
    select * from {{ source('raw_data', 'orders') }}
),

cleaned_orders as (
    select
        order_id,
        customer_id,
        product_id,
        quantity,
        price,
        order_date,
        status,
        created_at,
        updated_at,
        -- Calculate derived fields
        quantity * price as total_amount,
        case 
            when status in ('delivered', 'shipped') then 'completed'
            when status = 'cancelled' then 'cancelled'
            else 'in_progress'
        end as order_status_category,
        -- Add data quality flags
        case 
            when order_id is null then true
            else false
        end as is_order_id_null,
        case 
            when customer_id is null then true
            else false
        end as is_customer_id_null,
        case 
            when price <= 0 then true
            else false
        end as is_price_invalid,
        case 
            when quantity <= 0 then true
            else false
        end as is_quantity_invalid
    from source_data
),

final as (
    select
        order_id,
        customer_id,
        product_id,
        quantity,
        price,
        order_date,
        status,
        created_at,
        updated_at,
        total_amount,
        order_status_category,
        -- Data quality flags
        is_order_id_null,
        is_customer_id_null,
        is_price_invalid,
        is_quantity_invalid,
        -- Metadata
        current_timestamp as dbt_updated_at,
        'staging' as dbt_model_type
    from cleaned_orders
)

select * from final



