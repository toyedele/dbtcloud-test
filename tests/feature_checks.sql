with source as (
    select * from {{ ref('age_distribution_rolling') }} 
)

, fuel_source as (
    select * from {{ ref('fuel_type_distribution') }} 
)

, t_0 as (
    select * from source where loaded_at = (select max(loaded_at) from source)
)

, t_7 as (
    select * from source where loaded_at = (select max(loaded_at) from source) - 7
)

, t_0_fuel as (
    select * from fuel_source where loaded_at = (select max(loaded_at) from fuel_source)
)

, t_7_fuel as (
    select * from fuel_source where loaded_at = (select max(loaded_at) from fuel_source) - 7
)

, variance_check as (
    select
        t_0.product,
        t_0.loaded_at,
        'insured_person_age' as feature,
        round(100 * (t_7.p_min - t_0.p_min) /t_7.p_min,0) as variance_value_min,
        round(100 * (t_7.percentile_25 - t_0.percentile_25)/t_7.percentile_25 ,0) as variance_value_percentile_25,
        round(100 * (t_7.percentile_50 - t_0.percentile_50)/t_7.percentile_50,0) as variance_value_percentile_50,
        round(100 * (t_7.percentile_75 - t_0.percentile_75)/t_7.percentile_75,0) as variance_value_percentile_75,
        round(100 * (t_7.p_max - t_0.p_max)/t_7.p_max,0) as variance_value_max

    from t_0
    left join t_7 on t_0.product = t_7.product
)

, variance_check_fuel as (
    select
        t_0.product,
        t_0.loaded_at,
        'fuel_type' as feature,
        t_0.fuel_type as measure,
        100 *(round((t_7.rolling_10_day_fuel_count - t_0.rolling_10_day_fuel_count)/t_7.rolling_10_day_fuel_count ,0)) as percent_diff
    from t_0_fuel as t_0
    left join t_7_fuel as t_7 on t_0.product = t_7.product and t_0.fuel_type = t_7.fuel_type
)

, final as (
    select *
    from variance_check
    unpivot (percent_diff for measures in (variance_value_min, variance_value_percentile_25, 
    variance_value_percentile_50, variance_value_percentile_75, variance_value_max))

    union 

    select * from variance_check_fuel
)

select * from 
final
-- where abs(percent_diff) > 10