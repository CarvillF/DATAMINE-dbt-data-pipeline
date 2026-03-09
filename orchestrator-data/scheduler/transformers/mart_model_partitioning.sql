create schema if not exists analytics_gold;


-- Dimensión de zonas (Partido por hash / 4 particiones)

drop table if exists analytics_gold.dim_zone cascade;

create table analytics_gold.dim_zone (
    zone_key int,
    location_id int,
    zone_name varchar,
    borough varchar
) partition by hash (zone_key);

create table analytics_gold.dim_zone_p0 partition of analytics_gold.dim_zone 
    for values with (modulus 4, remainder 0);
create table analytics_gold.dim_zone_p1 partition of analytics_gold.dim_zone 
    for values with (modulus 4, remainder 1);
create table analytics_gold.dim_zone_p2 partition of analytics_gold.dim_zone 
    for values with (modulus 4, remainder 2);
create table analytics_gold.dim_zone_p3 partition of analytics_gold.dim_zone 
    for values with (modulus 4, remainder 3);


-- Dimensión de servicios (Partido por listas / 2 particiones)

drop table if exists analytics_gold.dim_service_type cascade;

create table analytics_gold.dim_service_type (
    service_id int,
    service_type varchar
) partition by list (service_type);

create table analytics_gold.dim_service_yellow partition of analytics_gold.dim_service_type 
    for values in ('yellow');
create table analytics_gold.dim_service_green partition of analytics_gold.dim_service_type 
    for values in ('green');


-- Dimensión de pagos (Partido por listas / 3 particiones)

drop table if exists analytics_gold.dim_payment_type cascade;

create table analytics_gold.dim_payment_type (
    payment_type_key int,
    payment_type varchar
) partition by list (payment_type);

create table analytics_gold.dim_payment_cash partition of analytics_gold.dim_payment_type 
    for values in ('cash');
create table analytics_gold.dim_payment_card partition of analytics_gold.dim_payment_type 
    for values in ('card');
create table analytics_gold.dim_payment_other partition of analytics_gold.dim_payment_type 
    for values in ('other/unknown');


-- Dimensión de pagos (Partido por listas / particiones en dependencia del numero de meses)

drop table if exists analytics_gold.fct_trips cascade;

create table analytics_gold.fct_trips (
    trip_key varchar,
    pickup_date_key int,
    pickup_date date,
    pu_zone_key int,
    do_zone_key int,
    service_key int,
    payment_type_key int,
    pickup_ts timestamp,
    dropoff_ts timestamp,
    passengers_count int,
    trip_distance numeric(10,2),
    fare_amount numeric(10,2),
    tip_amount numeric(10,2),
    tolls_amount numeric(10,2),
    total_amount numeric(10,2)
) partition by range (pickup_date);


do $$
declare
    rec record;
    start_date date;
    end_date date;
    table_name text;
begin
    for rec in (
        select distinct source_month 
        from analytics_silver.int_taxi_trips_zones_joined 
        where source_month is not null
    ) loop
        start_date := to_date(rec.source_month || '-01', 'yyyy-mm-dd');
        end_date := start_date + interval '1 month';
        table_name := 'analytics_gold.fct_trips_' || to_char(start_date, 'yyyy_mm');

        -- creamos la partición mensual dinámicamente
        execute format(
            'create table if not exists %i partition of analytics_gold.fct_trips for values from (%l) to (%l);',
            table_name,
            start_date,
            end_date
        );
    end loop;
end $$;

create table if not exists analytics_gold.fct_trips_default partition of analytics_gold.fct_trips default;