{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook="truncate table {{ this }};"
) }}


{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook="truncate table {{ this }};"
) }}

select
    locationid::int as zone_key,
    locationid::int as location_id,
    _zone as zone_name,
    borough
from {{ source('bronze_ingestion', 'taxi_zone_lookup') }}