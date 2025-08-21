{{ config(
    materialized='table'
    ) }}

with stage as (
    select *
    from {{ ref('stg_tpch__customer') }}
)

, enriched as (
    select
        id_customer
        , id_nation
        , customer_name
        , customer_address
        , customer_phone
        , account_balance_usd
        , marketing_segment
        , customer_comment
        , case
            when account_balance_usd >= 10000 then 'gold'
            when account_balance_usd >= 1000 then 'silver'
            else 'bronze'
        end as customer_category -- RENAMED in v3 (was customer_tier in v2)
        , convert_timezone('UTC', current_timestamp()) as staged_at_utc
    from stage
)

select *
from enriched
