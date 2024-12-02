{{ config(materialized='table') }}

with source as (
    select * from {{ ref('quotes_features') }}
)

select
    *
from source
where product = 'pvi'
