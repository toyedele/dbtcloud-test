{{ config(materialized='table') }}

with source as (
    select * from {{ ref('quotes_features') }} 
    where loaded_at is not null
)

, rolling_window as (
    select
        a.product,
        a.loaded_at,
        b.insured_person_age
    from source a
    join source b
        on a.product = b.product
        and b.loaded_at between a.loaded_at - interval '10 days' and a.loaded_at
),

rolling_percentile as (
    select
        product,
        loaded_at,
        approx_percentile(insured_person_age, 0) as rolling_p_min,
        approx_percentile(insured_person_age, 0.25) as rolling_percentile_25, 
        approx_percentile(insured_person_age, 0.5) as rolling_percentile_50,
        approx_percentile(insured_person_age, 0.75) as rolling_percentile_75,
        approx_percentile(insured_person_age, 1.0) as rolling_p_max
        
    from rolling_window
    group by product, loaded_at
    
)

, daily_percentile as (
    select
        product,
        loaded_at,
        approx_percentile(insured_person_age, 0) as daily_p_min,
        approx_percentile(insured_person_age, 0.25) as daily_percentile_25, 
        approx_percentile(insured_person_age, 0.5) as daily_percentile_50,
        approx_percentile(insured_person_age, 0.75) as daily_percentile_75,
        approx_percentile(insured_person_age, 1.0) as daily_p_max
        
    from source
    group by product, loaded_at

)

, final as (
    select
        r.product,
        r.loaded_at,
        r.rolling_p_min,
        r.rolling_percentile_25, 
        r.rolling_percentile_50,
        r.rolling_percentile_75,
        r.rolling_p_max,
        d.daily_p_min,
        d.daily_percentile_25, 
        d.daily_percentile_50,
        d.daily_percentile_75,
        d.daily_p_max
    from rolling_percentile as r
    left join daily_percentile as d
        on r.product = d.product
        and r.loaded_at = d.loaded_at
    
)

select * from 
final
-- product_percentile --where products = 'bvi' order by 2
-- count_win_perc