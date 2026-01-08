{% test single_primary_supplier(model, column_name) %}
{#
    Custom generic test that validates each product has exactly one primary supplier.
    
    This test checks that when is_primary_supplier = TRUE, there should only be
    one such row per product_id. If any product has 0 or multiple primary suppliers,
    the test will fail and return those product_ids.
    
    Usage in schema.yml:
        - single_primary_supplier:
            column_name: product_id
#}

WITH primary_supplier_counts AS (
    SELECT
        {{ column_name }} AS product_id,
        COUNT(*) AS primary_count
    FROM {{ model }}
    WHERE is_primary_supplier = TRUE
    GROUP BY {{ column_name }}
),

violations AS (
    SELECT
        product_id,
        primary_count,
        CASE
            WHEN primary_count = 0 THEN 'No primary supplier'
            WHEN primary_count > 1 THEN 'Multiple primary suppliers'
        END AS violation_type
    FROM primary_supplier_counts
    WHERE primary_count != 1
)

SELECT *
FROM violations

{% endtest %}
