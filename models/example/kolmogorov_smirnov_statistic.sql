-- models/kolmogorov_smirnov_statistic.sql

WITH comparison AS (
    SELECT
        ecdf_data.INSURED_PERSON_AGE,
        ecdf_data.empirical_cdf,
        theoretical_cdf.theoretical_cdf,
        ABS(ecdf_data.empirical_cdf - theoretical_cdf.theoretical_cdf) AS cdf_diff -- Absolute difference
    FROM {{ ref('kolmogorov_smirnov_ecdf') }} ecdf_data
    JOIN {{ ref('kolmogorov_smirnov_theoretical_cdf') }} theoretical_cdf
    ON ecdf_data.INSURED_PERSON_AGE = theoretical_cdf.INSURED_PERSON_AGE
)

SELECT 
    INSURED_PERSON_AGE,
    empirical_cdf,
    theoretical_cdf,
    cdf_diff,
    MAX(cdf_diff) OVER () AS ks_statistic -- K-S statistic: max absolute difference
FROM comparison
ORDER BY INSURED_PERSON_AGE
