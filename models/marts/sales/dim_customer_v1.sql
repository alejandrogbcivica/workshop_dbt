{{ config(
    materialized='table'
    ) }}

with stage as (
    select *
    from {{ ref('stg_tpch__customer') }}
)

select distinct
    id_customer
    , id_nation
    , customer_name
    , customer_address
    , customer_phone
    , account_balance_usd
    , marketing_segment
    , customer_comment
    , convert_timezone('UTC', current_timestamp()) as staged_at_utc
from stage
