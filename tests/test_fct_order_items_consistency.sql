select
    id_lineitem
    , id_order
    , id_part
    , gross_revenue_usd
    , discount_amount_usd
    , tax_amount_usd
    , net_revenue_usd
from {{ ref('fct_order_items') }}
where abs(
    net_revenue_usd - (gross_revenue_usd - discount_amount_usd + tax_amount_usd)
) > 0.01
