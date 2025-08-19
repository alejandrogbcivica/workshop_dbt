{{ config(
    materialized='table'
    ) }}

with li as (
    select *
    from {{ ref('stg_tpch__lineitem') }}
)

, o as (
    select
        id_order
        , id_customer
        , order_date_utc
        , order_status
        , is_fulfilled
        , order_priority
        , order_clerk
        , ship_priority_rank
    from {{ ref('stg_tpch__orders') }}
)

, base as (
    select
        li.id_lineitem
        , li.id_order
        , li.id_part
        , li.id_supplier
        , {{ dbt_utils.generate_surrogate_key(['li.ship_mode']) }} as id_ship_mode
        , li.ship_date_utc::date as ship_date
        , li.commit_date_utc::date as commit_date
        , li.receipt_date_utc::date as receipt_date
        , o.order_date_utc::date as order_date
        , li.quantity_qty
        , li.extended_price_usd
        , li.discount_pct
        , li.tax_pct
        , li.return_flag
        , li.line_status
        , o.order_priority
        , o.order_clerk
        , o.ship_priority_rank

        , convert_timezone('UTC', current_timestamp()) as staged_at_utc
    from li
    left join o
        on li.id_order = o.id_order
)

select
    id_lineitem
    , id_order
    , id_part
    , id_supplier
    , id_ship_mode
    , order_date
    , ship_date
    , commit_date
    , receipt_date
    , quantity_qty
    , extended_price_usd
    , discount_pct
    , tax_pct
    , extended_price_usd as gross_revenue_usd
    , (extended_price_usd * discount_pct) as discount_amount_usd
    , ((extended_price_usd - (extended_price_usd * discount_pct)) * tax_pct) as tax_amount_usd
    , (extended_price_usd - (extended_price_usd * discount_pct) + ((extended_price_usd - (extended_price_usd * discount_pct)) * tax_pct)) as net_revenue_usd
    , return_flag
    , line_status
    , order_priority
    , order_clerk
    , ship_priority_rank
    , staged_at_utc
from base