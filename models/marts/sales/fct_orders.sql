{{ config(
    materialized='table',
    event_time='order_date'
    ) }}

-- Aggregate metrics by order_id from fct_order_items
with f as (
    select
        id_order
        , sum(1) as lines_count  -- number of items (lines) in the order
        , sum(gross_revenue_usd) as gross_revenue_usd  -- total gross revenue
        , sum(discount_amount_usd) as discount_amount_usd  -- total discounts
        , sum(tax_amount_usd) as tax_amount_usd  -- total taxes
        , sum(net_revenue_usd) as net_revenue_usd  -- total net revenue
    from {{ ref('fct_order_items') }}
    group by 1
)

-- Select one representative row per order to bring order-level attributes
, o as (
    select
        id_order
        , id_customer
        , order_date
        , order_priority
        , order_clerk
        , ship_priority_rank
        , order_status
        , is_fulfilled
        , row_number() over (partition by id_order order by id_lineitem) as rn
    from {{ ref('fct_order_items') }}
)

-- Final output: one row per order with both metrics and attributes
select
    f.id_order
    , o.id_customer
    , o.order_date
    , f.lines_count
    , f.gross_revenue_usd
    , f.discount_amount_usd
    , f.tax_amount_usd
    , f.net_revenue_usd
    , o.order_priority
    , o.order_clerk
    , o.ship_priority_rank
    , o.order_status
    , o.is_fulfilled
    , convert_timezone('UTC', current_timestamp()) as staged_at_utc
from f
inner join o
    on
        f.id_order = o.id_order
        and o.rn = 1
