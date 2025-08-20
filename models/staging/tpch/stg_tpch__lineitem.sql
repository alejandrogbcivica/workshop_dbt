{{ config(
    materialized = 'incremental',
    unique_key = 'id_lineitem',
    on_schema_change = 'fail',
    cluster_by = ['ship_date_utc'],
    tags = ['silver','incremental']
) }}

with source as (
    select *
    from {{ source('tpch','lineitem') }}
    {% if is_incremental() %}
      where convert_timezone('UTC', loaded_at) > (
        select coalesce(max(loaded_at_utc), '1900-01-01'::timestamp)
        from {{ this }}
      )
    {% endif %}
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['l_orderkey','l_linenumber']) }} as id_lineitem
        , {{ dbt_utils.generate_surrogate_key(['l_orderkey']) }} as id_order
        , {{ dbt_utils.generate_surrogate_key(['l_partkey']) }} as id_part
        , {{ dbt_utils.generate_surrogate_key(['l_suppkey']) }} as id_supplier
        , cast(l_linenumber as number) as line_number
        , cast(l_quantity as decimal(12, 3)) as quantity_qty
        , cast(l_extendedprice as decimal(12, 2)) as extended_price_usd
        , cast(l_discount as decimal(6, 4)) as discount_pct
        , cast(l_tax as decimal(6, 4)) as tax_pct
        , trim(l_returnflag) as return_flag
        , trim(l_linestatus) as line_status
        , cast(l_shipdate as date) as ship_date_utc
        , cast(l_commitdate as date) as commit_date_utc
        , cast(l_receiptdate as date) as receipt_date_utc
        , trim(l_shipinstruct) as ship_instructions
        , trim(l_shipmode) as ship_mode
        , trim(l_comment) as line_comment
        , loaded_at as loaded_at_utc
        , convert_timezone('UTC', current_timestamp()) as staged_at_utc
        , '{{ invocation_id }}' as dbt_invocation_id
        , '{{ env_var("DBT_CLOUD_RUN_ID","local") }}' as dbt_cloud_run_id
        , '{{ env_var("DBT_CLOUD_JOB_ID","manual") }}' as dbt_cloud_job_id
    from source
    where
        l_orderkey is not null
        and l_partkey is not null
        and l_suppkey is not null -- Revisar
)

select *
from renamed