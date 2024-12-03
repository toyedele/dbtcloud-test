{{ config(materialized='table') }}

with source as (
    select * from {{ ref('quotes_features') }} 
    where loaded_at is not null
)

, rolling_window AS (
    SELECT
        a.PRODUCT,
        a.LOADED_AT,
        b.INSURED_PERSON_AGE
    FROM source a
    JOIN source b
        ON a.PRODUCT = b.PRODUCT
        AND b.LOADED_AT BETWEEN a.LOADED_AT - INTERVAL '10 DAYS' AND a.LOADED_AT
),
final AS (
    SELECT
        PRODUCT,
        LOADED_AT,
        approx_percentile(insured_person_age, 0) as p_min,
        approx_percentile(insured_person_age, 0.25) as percentile_25, 
        approx_percentile(insured_person_age, 0.5) as percentile_50,
        approx_percentile(insured_person_age, 0.75) as percentile_75,
        approx_percentile(insured_person_age, 1.0) as p_max
        
    FROM rolling_window
    GROUP BY PRODUCT, LOADED_AT
    
)

select * from 
final
-- product_percentile --where products = 'bvi' order by 2
-- count_win_perc