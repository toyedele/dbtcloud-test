{{ config(materialized='table') }}

with source as (
    select * from {{ ref('quotes_features') }} 
    where loaded_at is not null
)

, count_window_fuel as (
    select distinct
        product,
        loaded_at,
        fuel_type,
        count(*) over (partition by product, fuel_type
                        ORDER BY loaded_at
                        RANGE BETWEEN INTERVAL '10 DAYS' PRECEDING AND CURRENT ROW 
                    ) as rolling_10_day_count,
    from source
)

select * from count_window_fuel
-- where product = 'taxi' and loaded_at in ('2024-08-28', '2024-08-30')
-- order by loaded_at
