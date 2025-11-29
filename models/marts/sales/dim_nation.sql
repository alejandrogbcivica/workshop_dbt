{{ config(
    materialized='table'
    ) }}

with stage as (
    select *
    from {{ ref('stg_tpch__nation') }}
)

select
    id_nation
    , id_region
    , nation_key
    , region_key
    , nation_name
    , nation_comment as example
    , convert_timezone('UTC', current_timestamp()) as staged_at_utc
from stage
