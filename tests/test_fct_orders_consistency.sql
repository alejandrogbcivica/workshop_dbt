with line_sums as (
    select
        id_order
        , sum(gross_revenue_usd) as gross_sum
        , sum(discount_amount_usd) as discount_sum
        , sum(tax_amount_usd) as tax_sum
        , sum(net_revenue_usd) as net_sum
        , count(*) as lines_cnt
    from {{ ref('fct_order_items') }}
    group by 1
)

, orders as (
    select
        id_order
        , gross_revenue_usd
        , discount_amount_usd
        , tax_amount_usd
        , net_revenue_usd
        , lines_count
    from {{ ref('fct_orders') }}
)

select
    o.id_order
    , l.gross_sum
    , o.gross_revenue_usd
    , l.discount_sum
    , o.discount_amount_usd
    , l.tax_sum
    , o.tax_amount_usd
    , l.net_sum
    , o.net_revenue_usd
    , l.lines_cnt
    , o.lines_count
from orders as o
inner join line_sums as l on o.id_order = l.id_order
where
    abs(l.gross_sum - o.gross_revenue_usd) > 0.01
    or abs(l.discount_sum - o.discount_amount_usd) > 0.01
    or abs(l.tax_sum - o.tax_amount_usd) > 0.01
    or abs(l.net_sum - o.net_revenue_usd) > 0.01
    or l.lines_cnt <> o.lines_count
