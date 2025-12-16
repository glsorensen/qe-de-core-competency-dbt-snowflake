{% test single_primary_supplier(model) %}
-- =============================================================================
-- CUSTOM TEST: Ensure only one primary supplier per product
-- =============================================================================
-- This test FAILS if any product has more than one primary supplier
-- =============================================================================

SELECT
    product_id,
    COUNT(*) AS primary_count
FROM {{ model }}
WHERE is_primary_supplier = TRUE
GROUP BY product_id
HAVING COUNT(*) > 1

{% endtest %}
