-- Silver Layer: Staging Orders Valid Status
-- Validates that order status values are only valid values

SELECT
    order_id,
    status
FROM {{ ref('stg_orders') }}
WHERE status NOT IN ('completed', 'pending', 'processing')
