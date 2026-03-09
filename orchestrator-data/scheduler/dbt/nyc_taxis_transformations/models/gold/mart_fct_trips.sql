{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook="truncate table {{ this }};"
) }}

select
    -- llave subrogada
    md5(concat(service_type, pickup_ts, dropoff_ts, pickup_location_id, dropoff_location_id)) as trip_key,
    to_char(pickup_ts, 'yyyymmdd')::int as pickup_date_key,
    date(pickup_ts) as pickup_date,
    pickup_location_id as pu_zone_key,
    dropoff_location_id as do_zone_key,
    case when service_type = 'yellow' then 1 else 2 end as service_key,
    case
        when payment_type = 1 then 1 -- credit card
        when payment_type = 2 then 2 -- cash
        else 3 -- other / unknown / dispute
    end as payment_type_key,
    case 
        when vendor_id = 1 then 1
        when vendor_id = 2 then 2
        else 3 -- Other / Unknown
    end as vendor_key,
    pickup_ts,
    dropoff_ts,
    passengers_count,
    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount
from {{ ref('int_taxi_trips_zones_joined') }}