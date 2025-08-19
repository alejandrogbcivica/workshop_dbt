with source as (
    select *
    from {{ source('tpch','region') }}
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['r_regionkey']) }} as id_region
        , cast(r_regionkey as number) as region_key
        , trim(r_name) as region_name
        , trim(r_comment) as region_comment
        , loaded_at as loaded_at_utc
        , convert_timezone('UTC', current_timestamp()) as staged_at_utc
        , '{{ invocation_id }}' as dbt_invocation_id
        , '{{ env_var("DBT_CLOUD_RUN_ID","local") }}' as dbt_cloud_run_id
        , '{{ env_var("DBT_CLOUD_JOB_ID","manual") }}' as dbt_cloud_job_id
    from source
    where
        r_regionkey is not null
        and r_name is not null
)

select *
from renamed
