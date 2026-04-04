{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'dimension', 'date']
    )
}}

{#
    dim_date — Calendar dimension (2025-01-01 → 2026-12-31)

    date_key : INT YYYYMMDD format (e.g. 20260403)
    date_iso : MM/DD/YYYY display   (e.g. 04/03/2026)
    is_weekend: KSA weekend = Friday + Saturday
#}

WITH date_spine AS (

    SELECT EXPLODE(
        SEQUENCE(DATE('2025-01-01'), DATE('2026-12-31'), INTERVAL 1 DAY)
    ) AS dt

),

dim_date AS (

    SELECT

        -- Primary Key (YYYYMMDD)
        CAST(DATE_FORMAT(dt, 'yyyyMMdd') AS INT)                AS date_key,

        -- Display format
        DATE_FORMAT(dt, 'MM/dd/yyyy')                           AS date_iso,

        -- Calendar attributes
        dt                                                       AS full_date,
        YEAR(dt)                                                 AS year,
        QUARTER(dt)                                              AS quarter,
        MONTH(dt)                                                AS month,
        DATE_FORMAT(dt, 'MMMM')                                  AS month_name,
        DAY(dt)                                                  AS day,
        DATE_FORMAT(dt, 'EEEE')                                  AS weekday_name,
        DAYOFWEEK(dt)                                            AS day_of_week,
        DAYOFYEAR(dt)                                            AS day_of_year,

        -- KSA weekend: Friday + Saturday
        CASE
            WHEN DATE_FORMAT(dt, 'EEEE') IN ('Friday', 'Saturday') THEN TRUE
            ELSE FALSE
        END                                                      AS is_weekend

    FROM date_spine

)

SELECT * FROM dim_date
