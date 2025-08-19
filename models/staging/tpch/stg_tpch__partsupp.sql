with source as (
    select *
    from {{ source('tpch','partsupp') }}
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['ps_partkey','ps_suppkey']) }} as id_partsupp
        , {{ dbt_utils.generate_surrogate_key(['ps_partkey']) }} as id_part
        , {{ dbt_utils.generate_surrogate_key(['ps_suppkey']) }} as id_supplier
        , cast(ps_availqty as decimal(12, 3)) as available_qty
        , cast(ps_supplycost as decimal(12, 2)) as supply_cost_usd
        , trim(ps_comment) as partsupp_comment
        , loaded_at as loaded_at_utc
        , convert_timezone('UTC', current_timestamp()) as staged_at_utc
        , '{{ invocation_id }}' as dbt_invocation_id
        , '{{ env_var("DBT_CLOUD_RUN_ID","local") }}' as dbt_cloud_run_id
        , '{{ env_var("DBT_CLOUD_JOB_ID","manual") }}' as dbt_cloud_job_id
    from source
    where
        ps_partkey is not null
        and ps_suppkey is not null
)

select *
from renamed
