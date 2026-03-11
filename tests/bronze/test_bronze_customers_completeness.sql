-- Bronze Layer: Customers Completeness Tests
-- Validates that raw customer data has required fields populated

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at
FROM {{ source('raw', 'customers') }}
WHERE
    customer_id IS NULL
    OR first_name IS NULL
    OR last_name IS NULL
    OR email IS NULL
    OR created_at IS NULL
