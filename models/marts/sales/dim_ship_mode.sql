{{ config(
    materialized='table'
    ) }}


with stage as (
    select distinct
        trim(ship_mode) as ship_mode
    from {{ ref('stg_tpch__lineitem') }}
    where ship_mode is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['ship_mode']) }} as id_ship_mode
    , ship_mode
    , convert_timezone('UTC', current_timestamp()) as staged_at_utc
from stage
