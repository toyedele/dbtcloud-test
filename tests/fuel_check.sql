with source as (
    select * from {{ ref('age_distribution') }} 
)

, fuel_source as (
    select * from {{ ref('fuel_type_distribution') }} 
)

-- , t_0 as (
--     select * from source where loaded_at = (select max(loaded_at) from source)
-- )

-- , t_7 as (
--     select * from source where loaded_at = (select max(loaded_at) from source) - 7
-- )

, t_0_fuel as (
    select * from fuel_source where loaded_at = (select max(loaded_at) from fuel_source)
)

, t_7_fuel as (
    select * from fuel_source where loaded_at = (select max(loaded_at) from fuel_source) - 7
)

-- , variance_check as (
--     select
--         t_0.product,
--         t_0.loaded_at,
--         t_0.percentile_position,
--         round((t_7.perc - t_0.perc),0) as variance_value
--     from t_0
--     left join t_7 on t_0.product = t_7.product and t_0.percentile_position = t_7.percentile_position
-- )

, variance_check_fuel as (
    select
        t_0.product,
        t_0.loaded_at,
        t_0.fuel_type,
        100 *(round((t_7.rolling_10_day_count - t_0.rolling_10_day_count)/t_7.rolling_10_day_count ,0)) as variance_value_fuel
    from t_0_fuel as t_0
    left join t_7_fuel as t_7 on t_0.product = t_7.product and t_0.fuel_type = t_7.fuel_type
)

-- , final as (
--    select distinct
--         v.product,
--         v.loaded_at,
--         v.percentile_position,
--         v.variance_value as percentage_diff,
--         vf.fuel_type,
--         vf.variance_value_fuel as percentage_diff_fuel
--     from variance_check as v
--     left join variance_check_fuel as vf 
--         on v.product = vf.product 
--         and v.loaded_at = vf.loaded_at
-- )

select * from 
-- final
variance_check_fuel
where abs(variance_value_fuel) > 10 --or abs(percentage_diff_fuel) > 10