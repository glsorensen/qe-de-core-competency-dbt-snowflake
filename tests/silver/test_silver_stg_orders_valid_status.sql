-- Silver Layer: Staging Orders Valid Status
-- Validates that order status values are only valid values

SELECT
    order_id,
    ORDER_STATUS
FROM {{ ref('stg_orders') }}
WHERE ORDER_STATUS NOT IN ('completed', 'pending', 'processing')
