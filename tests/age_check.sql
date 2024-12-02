with source as (
    select * from {{ ref('age_distribution') }} 
)

, t_0 as (
    select * from source where loaded_at = (select max(loaded_at) from source)
)

, t_7 as (
    select * from source where loaded_at = (select max(loaded_at) from source) - 7
)

, variance_check as (
    select
        t_0.product,
        t_0.loaded_at,
        t_0.percentile_position,
        t_7.perc as t_7,
        t_0.perc as t_0,
        round((t_7.perc - t_0.perc),0) as variance_value
    from t_0
    left join t_7 on t_0.product = t_7.product and t_0.percentile_position = t_7.percentile_position
)

, final as (
    select * from variance_check 
    where abs(variance_value) > 10
)

select * from final