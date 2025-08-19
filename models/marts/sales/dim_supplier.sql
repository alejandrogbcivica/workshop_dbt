{{ config(
    materialized='table'
    ) }}

with stage as (
    select *
    from {{ ref('stg_tpch__supplier') }}
)

select distinct
    id_supplier
    , id_nation
    , supplier_name
    , supplier_address
    , supplier_phone
    , account_balance_usd
    , supplier_comment
    , convert_timezone('UTC', current_timestamp()) as staged_at_utc
from stage
