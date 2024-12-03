with source as (
    select * from {{ ref('age_distribution_rolling') }} 
)

-- , fuel_source as (
--     select * from {{ ref('fuel_type_distribution') }} 
-- )

, t_0 as (
    select * from source where loaded_at = (select max(loaded_at) from source)
)

, t_7 as (
    select * from source where loaded_at = (select max(loaded_at) from source) - 7
)

-- , t_0_fuel as (
--     select * from fuel_source where loaded_at = (select max(loaded_at) from fuel_source)
-- )

-- , t_7_fuel as (
--     select * from fuel_source where loaded_at = (select max(loaded_at) from fuel_source) - 7
-- )

, variance_check as (
    select
        t_0.product,
        t_0.loaded_at,
        round(100 * (t_7.p_min - t_0.p_min) /t_7.p_min,0) as variance_value_min,
        round(100 * (t_7.percentile_25 - t_0.percentile_25)/t_7.percentile_25 ,0) as variance_value_percentile_25,
        round(100 * (t_7.percentile_50 - t_0.percentile_50)/t_7.percentile_50,0) as variance_value_percentile_50,
        round(100 * (t_7.percentile_75 - t_0.percentile_75)/t_7.percentile_75,0) as variance_value_percentile_75,
        round(100 * (t_7.p_max - t_0.p_max)/t_7.p_max,0) as variance_value_max

    from t_0
    left join t_7 on t_0.product = t_7.product
)



select * from 
variance_check
where 
    abs(variance_value_min) > 10 or abs(variance_value_percentile_25) > 10
    or abs(variance_value_percentile_50) > 10 or abs(variance_value_percentile_75) > 10
    or abs(variance_value_max) > 10 