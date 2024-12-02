
-- Use the `ref` function to select from other models
with source as (
    select *
    from {{ ref('my_first_dbt_model') }}
)

select
    id
    , age
    , product
    , has_ncd
    , loaded_at::date as loaded_at
from source

