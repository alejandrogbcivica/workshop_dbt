{{ config(
    materialized='table'
    ) }}

with stage as (
    select *
    from {{ ref('stg_tpch__nation') }}
)

select *
from stage
