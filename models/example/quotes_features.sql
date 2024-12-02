{{ config(materialized='table') }}

with source as (
    select * from {{ ref('features_2211') }}
)

select
    quote_id,
    reporting_subject_type as product,
    premium_total as premium,
    has_ncd_protection as has_ncd,
    insured_person_age,
    vehicle_fuel_type as fuel_type,
    licence_type_abi as licence_type,
    insured_person_claim_l3y_at_fault_not_windscreen_nil_cost_count as claim_3y_at_fault_not_windscreen_nil_cost_count,
    loaded_at::date as loaded_at
from source
