-- Starburst-specific utility macros

-- Macro to create Iceberg table with proper configuration
{% macro create_iceberg_table(table_name, columns, partition_by=none) %}
    create table if not exists {{ table_name }} (
        {% for column in columns %}
            {{ column.name }} {{ column.type }}{% if not loop.last %},{% endif %}
        {% endfor %}
    ) with (
        format = 'PARQUET',
        location = 's3://data-lake/iceberg/{{ table_name }}'
        {% if partition_by %}
        ,partitioning = ARRAY[{{ partition_by | join(',') }}]
        {% endif %}
    )
{% endmacro %}

-- Macro to optimize Iceberg table
{% macro optimize_iceberg_table(table_name) %}
    call iceberg.system.rewrite_data_files('{{ table_name }}')
{% endmacro %}

-- Macro to expire old snapshots
{% macro expire_iceberg_snapshots(table_name, older_than_days=30) %}
    call iceberg.system.expire_snapshots(
        '{{ table_name }}', 
        timestamp '{{ (modules.datetime.datetime.now() - modules.datetime.timedelta(days=older_than_days)).strftime("%Y-%m-%d %H:%M:%S") }}'
    )
{% endmacro %}

-- Macro to get table statistics
{% macro get_table_stats(table_name) %}
    select 
        '{{ table_name }}' as table_name,
        count(*) as row_count,
        min(created_at) as min_created_at,
        max(created_at) as max_created_at
    from {{ table_name }}
{% endmacro %}

-- Macro to check data quality
{% macro check_data_quality(table_name, column_name, check_type) %}
    {% if check_type == 'not_null' %}
        select count(*) as null_count
        from {{ table_name }}
        where {{ column_name }} is null
    {% elif check_type == 'unique' %}
        select count(*) as duplicate_count
        from (
            select {{ column_name }}, count(*)
            from {{ table_name }}
            group by {{ column_name }}
            having count(*) > 1
        )
    {% elif check_type == 'accepted_values' %}
        select count(*) as invalid_count
        from {{ table_name }}
        where {{ column_name }} not in ({{ var('accepted_values') | join(',') }})
    {% endif %}
{% endmacro %}



