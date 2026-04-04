SELECT 1 AS failing_row
WHERE (SELECT COUNT(*) FROM {{ ref('dim_park') }}) != 2