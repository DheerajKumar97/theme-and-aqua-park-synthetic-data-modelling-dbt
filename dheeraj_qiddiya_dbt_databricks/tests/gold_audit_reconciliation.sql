-- ═══════════════════════════════════════════════════════════════════════════
-- [12] Audit & Reconciliation Check
-- ═══════════════════════════════════════════════════════════════════════════
-- Validates gold fact row counts vs silver source counts.
-- A drop > 5% indicates orphan data loss from INNER JOINs.
-- Zero rows = PASS. >0 rows = FAIL (investigate data loss).
-- ═══════════════════════════════════════════════════════════════════════════

{{ config(severity='warn') }}

WITH reconciliation AS (

    SELECT
        'fact_transaction' AS fact_table,
        (SELECT COUNT(*) FROM {{ ref('fact_transaction') }})     AS gold_count,
        (SELECT COUNT(*) FROM {{ ref('silver_transactions') }}
         WHERE transaction_date IS NOT NULL)                     AS silver_count

    UNION ALL

    SELECT
        'fact_gate_event',
        (SELECT COUNT(*) FROM {{ ref('fact_gate_event') }}),
        (SELECT COUNT(*) FROM {{ ref('silver_gate_events') }}
         WHERE event_date IS NOT NULL)

    UNION ALL

    SELECT
        'fact_ride_operations',
        (SELECT COUNT(*) FROM {{ ref('fact_ride_operations') }}),
        (SELECT COUNT(*) FROM {{ ref('silver_ride_opeartions') }}
         WHERE poll_date IS NOT NULL)

    UNION ALL

    SELECT
        'fact_customer_feedback',
        (SELECT COUNT(*) FROM {{ ref('fact_customer_feedback') }}),
        (SELECT COUNT(*) FROM {{ ref('silver_feedback') }})

    UNION ALL

    SELECT
        'fact_weather_observation',
        (SELECT COUNT(*) FROM {{ ref('fact_weather_observation') }}),
        (SELECT COUNT(*) FROM {{ ref('silver_weather') }}
         WHERE weather_date IS NOT NULL AND weather_code IS NOT NULL)

)

SELECT
    fact_table,
    gold_count,
    silver_count,
    ROUND((gold_count * 100.0 / NULLIF(silver_count, 0)), 2) AS retention_pct
FROM reconciliation
WHERE gold_count < CAST(silver_count * 0.95 AS BIGINT)
