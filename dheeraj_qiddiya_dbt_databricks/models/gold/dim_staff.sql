{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'dimension', 'staff']
    )
}}

{#
    dim_staff — Staff / Employee dimension

    Sourced from silver_staff.
    staff_key = silver staff_sk (deterministic UUID via dbt_utils).
#}

WITH dim_staff AS (

    SELECT

        -- Surrogate Key (from silver)
        staff_sk                                                 AS staff_key,

        -- Natural Key
        source_employee_id,

        -- Attributes
        department,
        nationality,
        is_active,

        -- Derived (from silver)
        staff_status,

        -- Audit
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM {{ ref('silver_staff') }}

)

SELECT * FROM dim_staff
