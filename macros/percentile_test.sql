-- macros/percentile_test.sql

{% macro test_percentile(model, column_name, min_value, max_value) %}

with percentiles as (
    select
        approx_percentile({{ column_name }}, 0.25) as percentile_25,
        approx_percentile({{ column_name }}, 0.50) as median,
        approx_percentile({{ column_name }}, 0.75) as percentile_75
    from {{ model }}
)

, validation as (
    select
        case
            when percentile_25 < {{ min_value }} or percentile_25 > {{ max_value }} then 1 else 0
        end as invalid_percentile_25,
        case
            when median < {{ min_value }} or median > {{ max_value }} then 1 else 0
        end as invalid_median,
        case
            when percentile_75 < {{ min_value }} or percentile_75 > {{ max_value }} then 1 else 0
        end as invalid_percentile_75
    from percentiles
)

select
    case
        when sum(invalid_percentile_25 + invalid_median + invalid_percentile_75) > 0 then 1
        -- else 1
    end as test_result
from validation

{% endmacro %}
