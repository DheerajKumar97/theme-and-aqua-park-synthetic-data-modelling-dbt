-- ═══════════════════════════════════════════════════════════════════════════
-- [9] Soft Delete / Inactive Dimension Check
-- ═══════════════════════════════════════════════════════════════════════════
-- Detects fact rows linked to INACTIVE staff members.
-- This is a WARNING (not error) — staff may have been active at transaction time.
-- Zero rows = PASS. >0 rows = WARNING (review needed).
-- ═══════════════════════════════════════════════════════════════════════════

{{ config(severity='warn') }}

SELECT
    'fact_transaction'      AS fact_table,
    ft.transaction_key      AS fact_key,
    ft.staff_key,
    ds.source_employee_id,
    ds.staff_status
FROM {{ ref('fact_transaction') }}           ft
INNER JOIN {{ ref('dim_staff') }}            ds
    ON ds.staff_key = ft.staff_key
WHERE ds.is_active = FALSE

UNION ALL

SELECT
    'fact_ride_operations'  AS fact_table,
    fr.ride_ops_key         AS fact_key,
    fr.staff_key,
    ds.source_employee_id,
    ds.staff_status
FROM {{ ref('fact_ride_operations') }}       fr
INNER JOIN {{ ref('dim_staff') }}            ds
    ON ds.staff_key = fr.staff_key
WHERE ds.is_active = FALSE
