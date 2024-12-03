{{ config(materialized='table') }}

with source as (
    select * from {{ ref('quotes_features') }} 
    where loaded_at is not null
)

, fuel_type_distribution as (
    select
       -- *, 
        product,
        loaded_at, 
        case 
            when fuel_type = 'HYBRID ELECTRIC' then 1
            when fuel_type = 'DIESEL' then 2
            when fuel_type = 'PETROL' then 3
            when fuel_type = 'ELECTRICITY' then 4
            when fuel_type = 'GAS BI-FUEL' then 5
            else null
        end as fuel_dist
    from source
)

, count_window_fuel as (
    select distinct
        product,
        loaded_at,
        fuel_dist,
        case 
            when fuel_dist = 1 then 'HYBRID ELECTRIC'
            when fuel_dist = 2 then 'DIESEL'
            when fuel_dist = 3 then 'PETROL'
            when fuel_dist = 4 then 'ELECTRICITY'
            when fuel_dist = 5 then 'GAS BI-FUEL'
            else null
        end as fuel_type,
        -- fuel,
        -- make dynamic
        count(*) over (partition by product, loaded_at, fuel_dist ) as count_win_fuel,
        count(*) over (partition by product, loaded_at) as count_win_total,
        100 * (count_win_fuel / count_win_total) as percentage_fuel
    from fuel_type_distribution
)

select * from count_window_fuel