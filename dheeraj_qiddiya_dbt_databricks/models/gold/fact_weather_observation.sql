{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'fact', 'weather']
    )
}}

WITH silver_wx AS (

    -- Resolve all columns cleanly before joining,
    -- so surrogate key macro never needs table aliases
    SELECT
        weather_sk,
        park_code,
        weather_date,
        weather_timestamp,
        weather_code,
        temperature_c,
        feels_like_c,
        temperature_min_c,
        temperature_max_c,
        temperature_range,
        humidity_pct,
        wind_speed_ms,
        wind_direction_deg,
        uv_index,
        visibility_m,
        rainfall_1h_mm,
        air_quality_index,
        temperature_category,
        is_raining,
        air_quality_status
    FROM {{ ref('silver_weather') }}
    WHERE weather_date IS NOT NULL
      AND weather_code IS NOT NULL

),

fact_weather_observation AS (

    SELECT

        -- Fact Surrogate Key (reuse silver SK — same grain)
        w.weather_sk                                             AS weather_obs_key,

        -- ═══ Dimension FKs ═══
        d.date_key,
        p.park_key,
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

        -- ═══ Derived Attributes ═══
        w.temperature_category,
        w.is_raining,
        w.air_quality_status,

        -- ═══ Audit ═══
        CURRENT_TIMESTAMP()                                      AS gold_loaded_at

    FROM silver_wx w

    INNER JOIN {{ ref('dim_date') }} d
        ON d.full_date = w.weather_date

    INNER JOIN {{ ref('dim_park') }} p
        ON p.park_code = w.park_code

    INNER JOIN {{ ref('dim_weather') }} wd
        ON wd.weather_code = w.weather_code

)

SELECT * FROM fact_weather_observation
