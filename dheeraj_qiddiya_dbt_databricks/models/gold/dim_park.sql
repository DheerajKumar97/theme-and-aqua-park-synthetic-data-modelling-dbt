{{
    config(
        materialized = 'table',
        schema       = 'gold',
        tags         = ['gold', 'dimension', 'park']
    )
}}

{#
    dim_park — Park dimension (static, 2 rows)

    park_key : INT  (1 = Six Flags Qiddiya, 2 = Aqua Arabia)
    park_code: business key used across all silver models
#}

SELECT 1 AS park_key, 'SF' AS park_code, 'Six Flags Qiddiya' AS park_name
UNION ALL
SELECT 2 AS park_key, 'AQ' AS park_code, 'Aqua Arabia'       AS park_name
