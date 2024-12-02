-- models/aggregations/reporting_subject_type_distribution_stats.sql

WITH base_data AS (
    SELECT 
        DATE_TRUNC('day', LOADED_AT)::date AS time_period,
        REPORTING_SUBJECT_TYPE,
        PREMIUM,
        FUEL_TYPE,
        LICENCE_TYPE,
        CLAIM_3Y_AT_FAULT_NOT_WINDSCREEN_NIL_COST_COUNT,
        HAS_NCD_PROTECTION,
        INSURED_PERSON_AGE
    FROM {{ ref('features_2211') }} -- Reference the dbt source table
),

distribution_stats AS (
    SELECT
        time_period,
        REPORTING_SUBJECT_TYPE,
        
        -- PREMIUM Distribution
        AVG(PREMIUM) AS avg_premium,
        MIN(PREMIUM) AS min_premium,
        MAX(PREMIUM) AS max_premium,
        STDDEV(PREMIUM) AS stddev_premium,
        
        -- FUEL_TYPE Distribution
        COUNT(CASE WHEN FUEL_TYPE = 'DIESEL' THEN 1 ELSE NULL END) AS diesel_count,
        COUNT(CASE WHEN FUEL_TYPE = 'PETROL' THEN 1 ELSE NULL END) AS petrol_count,
        COUNT(CASE WHEN FUEL_TYPE = 'HYBRID ELECTRIC' THEN 1 ELSE NULL END) AS hybrid_electric_count,
        
        -- LICENCE_TYPE Distribution
        COUNT(CASE WHEN LICENCE_TYPE = 'DRIVING_LICENCE_KIND_FULL_UK_CAR' THEN 1 ELSE NULL END) AS full_uk_car_count,
        COUNT(CASE WHEN LICENCE_TYPE = 'DRIVING_LICENCE_KIND_PROVISIONAL_UK_CAR' THEN 1 ELSE NULL END) AS provisional_uk_car_count,

        -- CLAIMS Distribution
        AVG(CLAIM_3Y_AT_FAULT_NOT_WINDSCREEN_NIL_COST_COUNT) AS avg_claim_count,
        MAX(CLAIM_3Y_AT_FAULT_NOT_WINDSCREEN_NIL_COST_COUNT) AS max_claim_count,
        
        -- HAS_NCD_PROTECTION Distribution
        COUNT(CASE WHEN HAS_NCD_PROTECTION = TRUE THEN 1 ELSE NULL END) AS ncd_protected_count,
        COUNT(CASE WHEN HAS_NCD_PROTECTION = FALSE THEN 1 ELSE NULL END) AS not_ncd_protected_count,
        
        -- INSURED_PERSON_AGE Distribution
        AVG(INSURED_PERSON_AGE) AS avg_insured_age,
        MIN(INSURED_PERSON_AGE) AS min_insured_age,
        MAX(INSURED_PERSON_AGE) AS max_insured_age
    FROM base_data
    GROUP BY time_period, REPORTING_SUBJECT_TYPE
)

SELECT * FROM distribution_stats
ORDER BY time_period, REPORTING_SUBJECT_TYPE
