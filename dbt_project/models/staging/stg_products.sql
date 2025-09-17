-- Staging model for products data
-- This model cleans and standardizes the raw products data

with source_data as (
    select * from {{ source('raw_data', 'products') }}
),

cleaned_products as (
    select
        product_id,
        name,
        category,
        price,
        description,
        created_at,
        updated_at,
        -- Clean and standardize data
        trim(upper(name)) as name_cleaned,
        trim(upper(category)) as category_cleaned,
        trim(description) as description_cleaned,
        -- Add data quality flags
        case 
            when product_id is null then true
            else false
        end as is_product_id_null,
        case 
            when name is null or trim(name) = '' then true
            else false
        end as is_name_invalid,
        case 
            when category is null or trim(category) = '' then true
            else false
        end as is_category_invalid,
        case 
            when price <= 0 then true
            else false
        end as is_price_invalid
    from source_data
),

final as (
    select
        product_id,
        name,
        category,
        price,
        description,
        created_at,
        updated_at,
        name_cleaned,
        category_cleaned,
        description_cleaned,
        -- Data quality flags
        is_product_id_null,
        is_name_invalid,
        is_category_invalid,
        is_price_invalid,
        -- Metadata
        current_timestamp as dbt_updated_at,
        'staging' as dbt_model_type
    from cleaned_products
)

select * from final
