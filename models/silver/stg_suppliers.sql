{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'suppliers') }}
),

transformed AS (
    SELECT
        -- Primary Key
        supplier_id,
        
        -- Attributes
        TRIM(supplier_name) AS supplier_name,
        LOWER(TRIM(contact_email)) AS contact_email,
        UPPER(TRIM(country)) AS country,
        lead_time_days,
        reliability_score,
        payment_terms_days,
        is_preferred,
        
        -- Date fields
        contract_start_date::DATE AS contract_start_date,
        contract_end_date::DATE AS contract_end_date,
        
        -- Calculated: Is contract currently active?
        CASE
            WHEN contract_end_date IS NULL THEN TRUE
            WHEN contract_end_date >= CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_active_contract,
        
        -- Calculated: Days remaining in contract (NULL if no end date = ongoing)
        CASE
            WHEN contract_end_date IS NULL THEN NULL
            ELSE DATEDIFF('day', CURRENT_DATE, contract_end_date)
        END AS contract_days_remaining,
        
        -- Calculated: Lead time category
        CASE
            WHEN lead_time_days <= 7 THEN 'fast'
            WHEN lead_time_days <= 14 THEN 'medium'
            ELSE 'slow'
        END AS lead_time_category,
        
        -- Calculated: Days as supplier
        DATEDIFF('day', contract_start_date, CURRENT_DATE) AS days_as_supplier,
        
        -- Metadata
        CURRENT_TIMESTAMP AS _loaded_at

    FROM source
)

SELECT * FROM transformed
