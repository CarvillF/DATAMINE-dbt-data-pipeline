{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook="truncate table {{ this }};"
) }}


select 1 as service_id, 'yellow' as service_type
union all
select 2 as service_id, 'green' as service_type
