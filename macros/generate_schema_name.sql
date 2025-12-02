{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
    ============================================================================
    MACRO: generate_schema_name (override)
    ============================================================================
    Custom schema name generation for medallion architecture.

    This macro overrides dbt's default schema naming to cleanly implement
    the Bronze → Silver → Gold medallion architecture pattern.

    Default dbt behavior:
        - dev: target_schema + '_' + custom_schema
        - prod: custom_schema

    Our medallion behavior:
        - Always use custom_schema if provided (bronze, silver, gold)
        - Falls back to target_schema if no custom schema

    Usage in dbt_project.yml:
        models:
          my_project:
            bronze:
              +schema: bronze
            silver:
              +schema: silver
            gold:
              +schema: gold

    This creates clean schema names like:
        - dbt_learning.bronze
        - dbt_learning.silver
        - dbt_learning.gold
    ============================================================================
    #}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
