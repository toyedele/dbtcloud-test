{{ config(materialized='table') }}

with source as (
    select * from {{ ref('quotes_features') }} 
    where loaded_at is not null
)
, 
product_percentile as (
    select
        product,
        loaded_at,
        -- insured_person_age,
        -- make dynamic
        approx_percentile(insured_person_age, 0) as p_min,
        approx_percentile(insured_person_age, 0.25) as percentile_25, 
        approx_percentile(insured_person_age, 0.5) as percentile_50,
        approx_percentile(insured_person_age, 0.75) as percentile_75,
        approx_percentile(insured_person_age, 1.0) as p_max
    from source
    group by product, loaded_at
)

, rolling_avg as (
    select
        product,
        loaded_at,
        avg(p_min) over (
              partition by product 
              order by loaded_at 
             RANGE BETWEEN INTERVAL '10 DAYS' preceding and current row
            ) as percentile_min_avg,
        avg(percentile_25) OVER (
              partition by product 
              order by loaded_at 
             RANGE BETWEEN INTERVAL '10 DAYS' preceding and current row
            ) as percentile_25_avg,
        avg(percentile_50) OVER (
              partition by product 
              order by loaded_at 
             RANGE BETWEEN INTERVAL '10 DAYS' preceding and current row
            ) as percentile_50_avg,
        avg(percentile_75) OVER (
              partition by product 
              order by loaded_at 
             RANGE BETWEEN INTERVAL '10 DAYS' preceding and current row
            ) as percentile_75_avg,
        avg(p_max) OVER (
              partition by product 
              order by loaded_at 
             RANGE BETWEEN INTERVAL '10 DAYS' preceding and current row
            ) as percentile_max_avg,
    from product_percentile
)

select * from 
rolling_avg
-- product_percentile --where products = 'bvi' order by 2
-- count_win_perc