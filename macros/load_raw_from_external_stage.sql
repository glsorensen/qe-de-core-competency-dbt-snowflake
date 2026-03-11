{% macro load_raw_from_external_stage(
    stage_name,
    file_format_name,
    stage_subpath='',
    truncate_before_load=true,
    on_error='continue',
    force=false
) %}
    {#
    Load Bronze/raw tables from a Snowflake external stage.

    Example:
      dbt run-operation load_raw_from_external_stage --args '{"stage_name":"my_ext_stage","file_format_name":"my_csv_ff"}'
    #}

    {% if not stage_name %}
        {{ exceptions.raise_compiler_error("stage_name is required") }}
    {% endif %}

    {% if not file_format_name %}
        {{ exceptions.raise_compiler_error("file_format_name is required") }}
    {% endif %}

    {% set normalized_stage_name = stage_name
        | replace('.STAGES.', '.')
        | replace('.stages.', '.')
    %}
    {% set normalized_file_format_name = file_format_name
        | replace('.FILE_FORMATS.', '.')
        | replace('.file_formats.', '.')
        | replace('.FILE FORMATS.', '.')
        | replace('.file formats.', '.')
    %}

    {% set stage_ref = normalized_stage_name if normalized_stage_name.startswith('@') else '@' ~ normalized_stage_name %}
    {% set stage_location = stage_ref ~ stage_subpath %}
    {% set truncate_sql = "TRUNCATE TABLE IF EXISTS raw." %}
    {% set force_sql = 'TRUE' if force else 'FALSE' %}

    {{ log("Preparing raw schema and tables...", info=True) }}
    {{ log("Using stage: " ~ stage_location, info=True) }}
    {{ log("Using file format: " ~ normalized_file_format_name, info=True) }}

    {% do run_query("CREATE SCHEMA IF NOT EXISTS raw") %}

    {% do run_query("\n        CREATE TABLE IF NOT EXISTS raw.customers (\n            customer_id NUMBER,\n            first_name VARCHAR,\n            last_name VARCHAR,\n            email VARCHAR,\n            phone VARCHAR,\n            address VARCHAR,\n            city VARCHAR,\n            state VARCHAR,\n            zip_code VARCHAR,\n            created_at DATE\n        )\n    ") %}

    {% do run_query("\n        CREATE TABLE IF NOT EXISTS raw.orders (\n            order_id NUMBER,\n            customer_id NUMBER,\n            product_id NUMBER,\n            quantity NUMBER,\n            order_date DATE,\n            total_amount NUMBER(18,2),\n            status VARCHAR\n        )\n    ") %}

    {% do run_query("\n        CREATE TABLE IF NOT EXISTS raw.products (\n            product_id NUMBER,\n            product_name VARCHAR,\n            category VARCHAR,\n            price NUMBER(18,2),\n            stock_quantity NUMBER,\n            description VARCHAR\n        )\n    ") %}

    {% if truncate_before_load %}
        {{ log("Truncating raw tables before load...", info=True) }}
        {% do run_query(truncate_sql ~ "customers") %}
        {% do run_query(truncate_sql ~ "orders") %}
        {% do run_query(truncate_sql ~ "products") %}
    {% endif %}

    {{ log("Copying files from " ~ stage_location ~ "...", info=True) }}

    {% set copy_customers_sql %}
        COPY INTO raw.customers
        FROM (
            SELECT
                $1::NUMBER,
                $2::VARCHAR,
                $3::VARCHAR,
                $4::VARCHAR,
                $5::VARCHAR,
                $6::VARCHAR,
                $7::VARCHAR,
                $8::VARCHAR,
                $9::VARCHAR,
                $10::DATE
            FROM {{ stage_location }}
        )
        FILE_FORMAT = (FORMAT_NAME = '{{ normalized_file_format_name }}')
        PATTERN = '.*customers.*[.]csv'
        ON_ERROR = '{{ on_error }}'
        FORCE = {{ force_sql }}
    {% endset %}
    {% do run_query(copy_customers_sql) %}

    {% set copy_orders_sql %}
        COPY INTO raw.orders
        FROM (
            SELECT
                $1::NUMBER,
                $2::NUMBER,
                $3::NUMBER,
                $4::NUMBER,
                $5::DATE,
                $6::NUMBER(18,2),
                $7::VARCHAR
            FROM {{ stage_location }}
        )
        FILE_FORMAT = (FORMAT_NAME = '{{ normalized_file_format_name }}')
        PATTERN = '.*orders.*[.]csv'
        ON_ERROR = '{{ on_error }}'
        FORCE = {{ force_sql }}
    {% endset %}
    {% do run_query(copy_orders_sql) %}

    {% set copy_products_sql %}
        COPY INTO raw.products
        FROM (
            SELECT
                $1::NUMBER,
                $2::VARCHAR,
                $3::VARCHAR,
                $4::NUMBER(18,2),
                $5::NUMBER,
                $6::VARCHAR
            FROM {{ stage_location }}
        )
        FILE_FORMAT = (FORMAT_NAME = '{{ normalized_file_format_name }}')
        PATTERN = '.*products.*[.]csv'
        ON_ERROR = '{{ on_error }}'
        FORCE = {{ force_sql }}
    {% endset %}
    {% do run_query(copy_products_sql) %}

    {{ log("Raw external stage load complete.", info=True) }}
{% endmacro %}