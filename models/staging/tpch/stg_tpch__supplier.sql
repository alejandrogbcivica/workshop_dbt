with source as (
    select *
    from {{ source('tpch','supplier') }}
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['s_suppkey']) }} as id_supplier
        , {{ dbt_utils.generate_surrogate_key(['s_nationkey']) }} as id_nation
        , cast(s_suppkey as number) as supplier_key
        , cast(s_nationkey as number) as nation_key
        , trim(s_name) as supplier_name
        , trim(s_address) as supplier_address
        , trim(s_phone) as supplier_phone
        , cast(s_acctbal as decimal(12, 2)) as account_balance_usd
        , trim(s_comment) as supplier_comment
        , loaded_at as loaded_at_utc
        , convert_timezone('UTC', current_timestamp()) as staged_at_utc
        , '{{ invocation_id }}' as dbt_invocation_id
        , '{{ env_var("DBT_CLOUD_RUN_ID","local") }}' as dbt_cloud_run_id
        , '{{ env_var("DBT_CLOUD_JOB_ID","manual") }}' as dbt_cloud_job_id
    from source
    where
        s_suppkey is not null
        and s_name is not null
        and s_nationkey is not null
)

select *
from renamed
