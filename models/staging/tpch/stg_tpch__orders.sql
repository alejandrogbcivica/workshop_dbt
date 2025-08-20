{{ config(
    materialized = 'incremental',
    unique_key = 'id_order',
    on_schema_change = 'fail',
    cluster_by = ['order_date_utc'],
    tags = ['silver','incremental']
) }}

with source as (
    select *
    from {{ source('tpch','orders') }}
    {% if is_incremental() %}
      where convert_timezone('UTC', loaded_at) > (
        select coalesce(max(loaded_at_utc), '1900-01-01'::timestamp)
        from {{ this }}
      )
    {% endif %}
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['o_orderkey']) }} as id_order
        , {{ dbt_utils.generate_surrogate_key(['o_custkey']) }} as id_customer
        , cast(o_orderkey as number) as order_key
        , o_orderstatus as order_status
        , coalesce(o_orderstatus = 'F', false) as is_fulfilled
        , cast(o_orderdate as date) as order_date_utc
        , cast(o_totalprice as decimal(12, 2)) as total_price_usd
        , trim(o_orderpriority) as order_priority
        , trim(o_clerk) as order_clerk
        , cast(o_shippriority as number) as ship_priority_rank
        , trim(o_comment) as order_comment
        , loaded_at as loaded_at_utc
        , convert_timezone('UTC', current_timestamp()) as staged_at_utc
        , '{{ invocation_id }}' as dbt_invocation_id
        , '{{ env_var("DBT_CLOUD_RUN_ID","local") }}' as dbt_cloud_run_id
        , '{{ env_var("DBT_CLOUD_JOB_ID","manual") }}' as dbt_cloud_job_id
    from source
    where
        o_orderkey is not null
        and o_custkey is not null
        and o_orderdate is not null
        and o_totalprice is not null
)

select *
from renamed
