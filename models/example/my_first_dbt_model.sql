
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    select 1 as id, 18 as age, 'taxi' as product, 'yes' as has_ncd, '2023-07-09' as loaded_at
    union all
    select null as id, 25 as age, 'car' as product, 'no' as has_ncd, '2023-10-09' as loaded_at
    union all 
    select 2 as id, 25 as age, 'delivery' as product, 'no' as has_ncd, '2023-11-09' as loaded_at
    union all 
    select 3 as id, null as age, 'delivery' as product, 'yes' as has_ncd, '2023-11-09' as loaded_at

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
