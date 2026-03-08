with taxi_trips_yellow as (
    select
        service_type,
        source_month,
        vendorid :: int as vendor_id,
        tpep_pickup_datetime :: timestamp as pickup_ts,  
        tpep_dropoff_datetime :: timestamp as dropoff_ts,
        passenger_count :: int as passengers_count,
        trip_distance :: numeric(10,2) as trip_distance,
        pulocationid :: int as pickup_location_id,
        dolocationid :: int as dropoff_location_id,
        ratecodeid :: int as rate_code_id,
        payment_type :: int as payment_type,
        fare_amount :: numeric(10,2) as fare_amount,
        tip_amount :: numeric(10,2) as tip_amount,
        tolls_amount :: numeric(10,2) as tolls_amount,
        total_amount :: numeric(10,2) as total_amount
    from {{ source('bronze_ingestion', 'yellow_tripdata') }}
),

taxi_trips_green as (
    select
        service_type,
        source_month,
        vendorid :: int as vendor_id,
        lpep_pickup_datetime :: timestamp as pickup_ts, 
        lpep_dropoff_datetime :: timestamp as dropoff_ts,
        passenger_count :: int as passengers_count,
        trip_distance :: numeric(10,2) as trip_distance,
        pulocationid :: int as pickup_location_id,
        dolocationid :: int as dropoff_location_id,
        ratecodeid :: int as rate_code_id,
        payment_type :: int as payment_type,
        fare_amount :: numeric(10,2) as fare_amount,
        tip_amount :: numeric(10,2) as tip_amount,
        tolls_amount :: numeric(10,2) as tolls_amount,
        total_amount :: numeric(10,2) as total_amount
    from {{ source('bronze_ingestion', 'green_tripdata') }}
), 

taxi_trips_combined as (
    select * from taxi_trips_yellow
    union all
    select * from taxi_trips_green
),

taxi_zones as (
    select
        locationid :: int as location_id,
        _zone as zone_name
    from {{ source('bronze_ingestion', 'taxi_zone_lookup') }}
)

SELECT
    trips.*,
    zones_pickup.zone_name AS pickup_location_name,
    zones_dropoff.zone_name AS dropoff_location_name
FROM taxi_trips_combined AS trips
INNER JOIN taxi_zones AS zones_pickup
    on  trips.pickup_location_id = zones_pickup.location_id
INNER JOIN taxi_zones AS zones_dropoff
    on  trips.dropoff_location_id = zones_dropoff.location_id



WHERE trips.pickup_ts IS NOT NULL
  AND trips.dropoff_ts IS NOT NULL
  AND trips.pickup_ts <= trips.dropoff_ts
  AND trips.trip_distance >= 0
  AND trips.total_amount >= 0