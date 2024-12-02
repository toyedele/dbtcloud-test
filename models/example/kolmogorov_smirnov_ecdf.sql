-- models/kolmogorov_smirnov_ecdf.sql

WITH base_data AS (
    SELECT 
        INSURED_PERSON_AGE,
        ROW_NUMBER() OVER (ORDER BY INSURED_PERSON_AGE) AS rank,
        COUNT(*) OVER () AS total_count
    FROM {{ ref('features_2211') }}
),

ecdf_data AS (
    SELECT
        INSURED_PERSON_AGE,
        CAST(rank AS FLOAT) / total_count AS empirical_cdf -- Compute ECDF
    FROM base_data
)

SELECT * 
FROM ecdf_data
ORDER BY INSURED_PERSON_AGE
