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
                    ) as rolling_10_day_fuel_count
        -- count(*) over (partition by fuel_type
        --                 ORDER BY loaded_at
        --                 RANGE BETWEEN INTERVAL '10 DAYS' PRECEDING AND CURRENT ROW 
        --             ) as rolling_10_day_total_count,
        -- 100 * (rolling_10_day_fuel_count / rolling_10_day_total_count) as rolling_10_day_fuel_percent
    from source
)

select * from count_window_fuel
where product = 'taxi' and loaded_at in ('2024-11-16', '2024-11-17')  --('2024-08-28', '2024-08-30')
order by loaded_at
