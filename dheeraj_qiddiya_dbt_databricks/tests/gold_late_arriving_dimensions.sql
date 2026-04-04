-- ═══════════════════════════════════════════════════════════════════════════
-- [3] Late Arriving Dimension Handling Check
-- ═══════════════════════════════════════════════════════════════════════════
-- Detects any fact rows where a MANDATORY FK resolved to NULL.
-- Since we use INNER JOINs, this should never happen — but validates
-- the join strategy is working correctly.
-- Zero rows = PASS (confirms INNER JOIN strategy is airtight).
-- ═══════════════════════════════════════════════════════════════════════════

WITH checks AS (

    -- ── fact_transaction ─────────────────────────────────────────────────

    SELECT
        'fact_transaction — NULL date_key'              AS validation,
        COUNT(*)                                        AS violation_count
    FROM {{ ref('fact_transaction') }}
    WHERE date_key IS NULL

    UNION ALL

    SELECT
        'fact_transaction — NULL park_key',
        COUNT(*)
    FROM {{ ref('fact_transaction') }}
    WHERE park_key IS NULL

    UNION ALL

    SELECT
        'fact_transaction — NULL product_key',
        COUNT(*)
    FROM {{ ref('fact_transaction') }}
    WHERE product_key IS NULL

    -- ── fact_gate_event ──────────────────────────────────────────────────

    UNION ALL

    SELECT
        'fact_gate_event — NULL date_key',
        COUNT(*)
    FROM {{ ref('fact_gate_event') }}
    WHERE date_key IS NULL

    UNION ALL

    SELECT
        'fact_gate_event — NULL park_key',
        COUNT(*)
    FROM {{ ref('fact_gate_event') }}
    WHERE park_key IS NULL

    UNION ALL

    SELECT
        'fact_gate_event — NULL gate_key',
        COUNT(*)
    FROM {{ ref('fact_gate_event') }}
    WHERE gate_key IS NULL

    -- ── fact_ride_operations ─────────────────────────────────────────────

    UNION ALL

    SELECT
        'fact_ride_operations — NULL date_key',
        COUNT(*)
    FROM {{ ref('fact_ride_operations') }}
    WHERE date_key IS NULL

    UNION ALL

    SELECT
        'fact_ride_operations — NULL park_key',
        COUNT(*)
    FROM {{ ref('fact_ride_operations') }}
    WHERE park_key IS NULL

    UNION ALL

    SELECT
        'fact_ride_operations — NULL ride_key',
        COUNT(*)
    FROM {{ ref('fact_ride_operations') }}
    WHERE ride_key IS NULL

    -- ── fact_weather_observation ─────────────────────────────────────────

    UNION ALL

    SELECT
        'fact_weather_observation — NULL date_key',
        COUNT(*)
    FROM {{ ref('fact_weather_observation') }}
    WHERE date_key IS NULL

    UNION ALL

    SELECT
        'fact_weather_observation — NULL park_key',
        COUNT(*)
    FROM {{ ref('fact_weather_observation') }}
    WHERE park_key IS NULL

    UNION ALL

    SELECT
        'fact_weather_observation — NULL weather_key',
        COUNT(*)
    FROM {{ ref('fact_weather_observation') }}
    WHERE weather_key IS NULL

    -- ── fact_customer_feedback ───────────────────────────────────────────

    UNION ALL

    SELECT
        'fact_customer_feedback — NULL park_key',
        COUNT(*)
    FROM {{ ref('fact_customer_feedback') }}
    WHERE park_key IS NULL

)

SELECT
    validation,
    violation_count
FROM checks
WHERE violation_count > 0