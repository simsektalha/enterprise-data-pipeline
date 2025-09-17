-- Staging model for customers data
-- This model cleans and standardizes the raw customers data

with source_data as (
    select * from {{ source('raw_data', 'customers') }}
),

cleaned_customers as (
    select
        customer_id,
        name,
        email,
        country,
        registration_date,
        created_at,
        updated_at,
        -- Clean and standardize data
        trim(upper(name)) as name_cleaned,
        trim(lower(email)) as email_cleaned,
        trim(upper(country)) as country_cleaned,
        -- Extract name parts
        split_part(trim(name), ' ', 1) as first_name,
        case 
            when position(' ' in trim(name)) > 0 
            then split_part(trim(name), ' ', 2)
            else null
        end as last_name,
        -- Add data quality flags
        case 
            when customer_id is null then true
            else false
        end as is_customer_id_null,
        case 
            when name is null or trim(name) = '' then true
            else false
        end as is_name_invalid,
        case 
            when email is null or email not like '%@%' then true
            else false
        end as is_email_invalid,
        case 
            when country is null or trim(country) = '' then true
            else false
        end as is_country_invalid
    from source_data
),

final as (
    select
        customer_id,
        name,
        email,
        country,
        registration_date,
        created_at,
        updated_at,
        name_cleaned,
        email_cleaned,
        country_cleaned,
        first_name,
        last_name,
        -- Data quality flags
        is_customer_id_null,
        is_name_invalid,
        is_email_invalid,
        is_country_invalid,
        -- Metadata
        current_timestamp as dbt_updated_at,
        'staging' as dbt_model_type
    from cleaned_customers
)

select * from final
