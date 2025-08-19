{{ config(
    materialized='table'
    ) }}

with stage as (
    select *
    from {{ ref('stg_tpch__part') }}
)

select
    id_part
    , part_name
    , manufacturer
    , brand
    , part_type
    , size_qty
    , container_type
    , retail_price_usd
    , part_comment
    , convert_timezone('UTC', current_timestamp()) as staged_at_utc
from stage
