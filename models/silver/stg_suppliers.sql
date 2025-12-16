{{ config(materialized='view') }}

-- =============================================================================
-- SILVER LAYER - Staging Suppliers
-- =============================================================================
-- Clean and standardize supplier data with derived columns
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'suppliers') }}
),

cleaned AS (
    SELECT
        -- Primary key
        supplier_id,

        -- Cleaned attributes
        TRIM(supplier_name) AS supplier_name,
        LOWER(TRIM(contact_email)) AS contact_email,
        UPPER(TRIM(country)) AS country,

        -- Numeric fields
        lead_time_days,
        -- Validate reliability score is between 0 and 1
        CASE
            WHEN reliability_score < 0 THEN 0
            WHEN reliability_score > 1 THEN 1
            ELSE reliability_score
        END AS reliability_score,
        payment_terms_days,

        -- Boolean fields
        COALESCE(is_preferred, FALSE) AS is_preferred,

        -- Date fields
        contract_start_date,
        contract_end_date,

        -- Derived: Is contract currently active?
        CASE
            WHEN contract_end_date IS NULL THEN TRUE
            WHEN contract_end_date >= CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_active_contract,

        -- Derived: Days remaining on contract (NULL if no end date)
        CASE
            WHEN contract_end_date IS NULL THEN NULL
            WHEN contract_end_date < CURRENT_DATE THEN 0
            ELSE DATEDIFF(day, CURRENT_DATE, contract_end_date)
        END AS contract_days_remaining,

        -- Derived: Lead time category
        CASE
            WHEN lead_time_days <= 7 THEN 'fast'
            WHEN lead_time_days <= 14 THEN 'medium'
            ELSE 'slow'
        END AS lead_time_category

    FROM source
)

SELECT * FROM cleaned
