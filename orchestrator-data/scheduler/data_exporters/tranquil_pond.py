from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def execute_partition_script(*args, **kwargs):
    logger = kwargs.get('logger')
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Dividimos el código en fragmentos para poder ver el progreso en los logs
    steps =[
        ("Creando Esquema analytics_gold", """
            create schema if not exists analytics_gold;
        """),
        
        ("Recreando mart_dim_zone (Hash)", """
            drop table if exists analytics_gold.mart_dim_zone cascade;
            create table analytics_gold.mart_dim_zone (
                zone_key int,
                location_id int,
                zone_name varchar,
                borough varchar
            ) partition by hash (zone_key);

            create table analytics_gold.mart_dim_zone_p0 partition of analytics_gold.mart_dim_zone for values with (modulus 4, remainder 0);
            create table analytics_gold.mart_dim_zone_p1 partition of analytics_gold.mart_dim_zone for values with (modulus 4, remainder 1);
            create table analytics_gold.mart_dim_zone_p2 partition of analytics_gold.mart_dim_zone for values with (modulus 4, remainder 2);
            create table analytics_gold.mart_dim_zone_p3 partition of analytics_gold.mart_dim_zone for values with (modulus 4, remainder 3);
        """),
        
        ("Recreando mart_dim_service_type (List)", """
            drop table if exists analytics_gold.mart_dim_service_type cascade;
            create table analytics_gold.mart_dim_service_type (
                service_id int,
                service_type varchar
            ) partition by list (service_type);

            create table analytics_gold.mart_dim_service_yellow partition of analytics_gold.mart_dim_service_type for values in ('yellow');
            create table analytics_gold.mart_dim_service_green partition of analytics_gold.mart_dim_service_type for values in ('green');
        """),
        
        ("Recreando mart_dim_payment_type (List)", """
            drop table if exists analytics_gold.mart_dim_payment_type cascade;
            create table analytics_gold.mart_dim_payment_type (
                payment_type_key int,
                payment_type varchar
            ) partition by list (payment_type);

            create table analytics_gold.mart_dim_payment_cash partition of analytics_gold.mart_dim_payment_type for values in ('cash');
            create table analytics_gold.mart_dim_payment_card partition of analytics_gold.mart_dim_payment_type for values in ('card');
            create table analytics_gold.mart_dim_payment_other partition of analytics_gold.mart_dim_payment_type for values in ('other/unknown');
        """),
        
        ("Recreando mart_fct_trips (Base tabla de Hechos)", """
            drop table if exists analytics_gold.mart_fct_trips cascade;
            create table analytics_gold.mart_fct_trips (
                trip_key varchar,
                pickup_date_key int,
                pickup_date date,
                pu_zone_key int,
                do_zone_key int,
                service_key int,
                payment_type_key int,
                vendor_key int,          -- <-- NUEVA COLUMNA AGREGADA AQUÍ
                pickup_ts timestamp,
                dropoff_ts timestamp,
                passengers_count int,
                trip_distance numeric(10,2),
                fare_amount numeric(10,2),
                tip_amount numeric(10,2),
                tolls_amount numeric(10,2),
                total_amount numeric(10,2)
            ) partition by range (pickup_date);
            
            create table if not exists analytics_gold.mart_fct_trips_default partition of analytics_gold.mart_fct_trips default;
        """),
        
        ("Generando particiones mensuales", """
            DO $$
            DECLARE
                start_date DATE;
                end_date DATE;
                table_name TEXT;
            BEGIN
                FOR i IN 0..47 LOOP
                    start_date := '2022-01-01'::DATE + (i || ' months')::INTERVAL;
                    end_date := start_date + '1 month'::INTERVAL;
                    table_name := 'analytics_gold.mart_fct_trips_' || to_char(start_date, 'YYYY_MM');
                    EXECUTE format('CREATE TABLE IF NOT EXISTS %%I PARTITION OF analytics_gold.mart_fct_trips FOR VALUES FROM (%%L) TO (%%L);', table_name, start_date, end_date);
                END LOOP;
            END $$;
        """)
    ]

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for step_name, sql in steps:
            logger.info(f"Ejecutando: {step_name}...")
            loader.execute(sql)
            logger.info(f"OK: {step_name} completado.")
        loader.conn.commit()
    logger.info("¡Todas las tablas y particiones creadas y guardadas con éxito!")