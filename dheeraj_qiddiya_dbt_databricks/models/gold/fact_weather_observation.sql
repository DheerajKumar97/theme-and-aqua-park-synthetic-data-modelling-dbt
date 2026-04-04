{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'fact', 'weather']
    )
}}

{#
    fact_weather_observation — Weather telemetry fact table

    Grain: one row per weather poll record (hourly).

    FK Join Strategy (100% RI):
        INNER JOIN → date_key, park_key, weather_key  (all NOT NULL — orphan guard)
#}

WITH silver_wx AS (

    SELECT *
    FROM {{ ref('silver_weather') }}
    WHERE weather_date IS NOT NULL
      AND weather_code IS NOT NULL

),

fact_weather_observation AS (

    SELECT

        -- Fact Surrogate Key
        REGEXP_REPLACE(
            {{ dbt_utils.generate_surrogate_key(
                ['w.park_code', 'w.weather_timestamp']
            ) }},
            '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
            '$1-$2-$3-$4-$5'
        )                                                        AS weather_obs_key,

        -- ═══ Dimension FKs ═══

        -- Date (INNER — mandatory)
        d.date_key,

        -- Park (INNER — mandatory)
        p.park_key,

        -- Weather Condition (INNER — mandatory)
        wd.weather_key,

        -- ═══ Timestamp ═══
        w.weather_timestamp                                      AS poll_datetime,

        -- ═══ Measures — Temperature ═══
        w.temperature_c,
        w.feels_like_c,
        w.temperature_min_c,
        w.temperature_max_c,
        w.temperature_range,

        -- ═══ Measures — Atmosphere ═══
        w.humidity_pct,
        w.wind_speed_ms,
        w.wind_direction_deg,
        w.uv_index,
        w.visibility_m,
        w.rainfall_1h_mm,
        w.air_quality_index,

        -- ═══ Derived Attributes (from silver) ═══
        w.temperature_category,
        w.is_raining,
        w.air_quality_status,

        -- ═══ Audit ═══
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM silver_wx w

    -- ── ALL NOT NULL FKs → INNER JOIN (orphan protection) ────────────────
    INNER JOIN {{ ref('dim_date') }} d
        ON d.full_date = w.weather_date

    INNER JOIN {{ ref('dim_park') }} p
        ON p.park_code = w.park_code

    INNER JOIN {{ ref('dim_weather') }} wd
        ON wd.weather_code = w.weather_code

)

SELECT * FROM fact_weather_observation
