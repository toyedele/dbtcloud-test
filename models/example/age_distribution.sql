with source as (
    select * from {{ ref('quotes_features') }} 
    where loaded_at is not null
)
, 
product_percentile as (
    select
        product as products,
        loaded_at as loaded_ats,
        -- make dynamic
        approx_percentile(insured_person_age, 0.25) as percentile_25, 
        approx_percentile(insured_person_age, 0.5) as percentile_50,
        approx_percentile(insured_person_age, 0.75) as percentile_75
    from source
    group by products, loaded_ats
)

, distribution as (
    select
        *, 
        s.product as s_p,
        s.loaded_at as s_l,
        -- make dynamic
        case 
            when s.product = pp.products and s.loaded_at = pp.loaded_ats and s.insured_person_age < pp.percentile_25 then 1
            when s.product = pp.products and s.loaded_at = pp.loaded_ats and s.insured_person_age between pp.percentile_25 and percentile_50 then 2
            when s.product = pp.products and s.loaded_at = pp.loaded_ats and s.insured_person_age between pp.percentile_50 and percentile_75 then 3
            when s.product = pp.products and s.loaded_at = pp.loaded_ats and s.insured_person_age > percentile_75 then 4
            else null
        end as dist
    from source as s
    left join product_percentile as pp on s.product = pp.products and s.loaded_at = pp.loaded_ats
)

, count_window as (
    select distinct
        product,
        loaded_at,
        dist,
        -- make dynamic
        count(*) over (partition by product, loaded_at, dist ) as count_win,
        count(*) over (partition by product, loaded_at) as count_win_total
    from distribution
)

, count_win_perc as (
    select
        product,
        loaded_at,
        dist,
        -- insured_person_age,
        -- make dynamic
        case 
            when dist = 1 then 'percentile_25'
            when dist = 2 then 'percentile_25_50'
            when dist = 3 then 'percentile_50_75'
            when dist = 4 then 'percentile_75'
            else null
        end as percentile_position,
        100 * (count_win / count_win_total) as perc
    from count_window
)

select * from count_win_perc