{{ config(
    materialized='table'
    ) }}

with stage as (
    select *
    from {{ ref('stg_tpch__region') }}
)

select
    id_region
    , region_name
    , region_comment
    , convert_timezone('UTC', current_timestamp()) as staged_at_utc
from stage
