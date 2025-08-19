with source as (
    select *
    from {{ source('tpch','part') }}
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['p_partkey']) }} as id_part
        , cast(p_partkey as number) as part_key
        , trim(p_name) as part_name
        , trim(p_mfgr) as manufacturer
        , trim(p_brand) as brand
        , trim(p_type) as part_type
        , cast(p_size as number) as size_qty
        , trim(p_container) as container_type
        , cast(p_retailprice as decimal(12, 2)) as retail_price_usd
        , trim(p_comment) as part_comment
        , loaded_at as loaded_at_utc
        , convert_timezone('UTC', current_timestamp()) as staged_at_utc
        , '{{ invocation_id }}' as dbt_invocation_id
        , '{{ env_var("DBT_CLOUD_RUN_ID","local") }}' as dbt_cloud_run_id
        , '{{ env_var("DBT_CLOUD_JOB_ID","manual") }}' as dbt_cloud_job_id
    from source
    where
        p_partkey is not null
        and p_name is not null
)

select *
from renamed
