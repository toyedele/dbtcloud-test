-- models/kolmogorov_smirnov_theoretical_cdf.sql

WITH theoretical_cdf AS (
    SELECT 
        ecdf.INSURED_PERSON_AGE,
        ecdf.empirical_cdf,
        -- Replace 40 (mean) and 10 (stddev) with desired parameters
        0.5 * (1 + SIGN(ecdf.INSURED_PERSON_AGE - 40) * 
            (1 - EXP(-SQUARE((ecdf.INSURED_PERSON_AGE - 40) / 10) / 2))) AS theoretical_cdf
    FROM {{ ref('kolmogorov_smirnov_ecdf') }} ecdf
)

SELECT * 
FROM theoretical_cdf
ORDER BY INSURED_PERSON_AGE
