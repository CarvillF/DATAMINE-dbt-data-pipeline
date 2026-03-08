from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
import time
import random

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_bronze_data(df: DataFrame, **kwargs):
    logger = kwargs.get('logger')
    
    if df is None or df.empty:
        logger.warning("Export: DataFrame vacío. Saltando.")
        return

    # Phase: Concurrency Jitter
    time.sleep(random.uniform(0.1, 3.0))

    # Phase: Data Type Fixing
    int_columns = [
        'vendorid', 'ratecodeid', 'payment_type', 'passenger_count', 
        'pulocationid', 'dolocationid', 'trip_type'
    ]
    
    for col in int_columns:
        if col in df.columns:
            df[col] = df[col].astype('Int64')

    source_month = df['source_month'].iloc[0]
    service_type = df['service_type'].iloc[0]

    # Phase: Configuration
    schema_name = 'bronze'
    table_name = f"{service_type}_tripdata"
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Phase: Isolated Schema Setup
    try:
        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as setup_loader:
            setup_loader.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    except Exception:
        pass

    # Phase: DB Transaction
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        
        # Phase: Check Existence & Idempotency
        query_check_table = f"""
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = '{schema_name}' 
        AND table_name = '{table_name}';
        """
        
        table_exists_df = loader.load(query_check_table)
        table_exists = not table_exists_df.empty

        if table_exists:
            query_delete = f"""
            DELETE FROM {schema_name}.{table_name} 
            WHERE source_month = '{source_month}' 
            AND service_type = '{service_type}';
            """
            loader.execute(query_delete)
            logger.info(f"Export: Limpieza ejecutada para {service_type} - {source_month}")

            query_columns = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema_name}' 
            AND table_name = '{table_name}';
            """
            db_cols_df = loader.load(query_columns)
            db_columns = db_cols_df['column_name'].tolist()
            
            common_cols = [c for c in df.columns if c in db_columns]
            df_final = df[common_cols]
            
        else:
            logger.info(f"Export: Tabla nueva {table_name}. Creando con esquema completo.")
            df_final = df

        # Phase: Load
        loader.export(
            df_final,
            schema_name,
            table_name,
            index=False,
            if_exists='append',
            allow_reserved_words_aliases=True
        )
        
    logger.info(f"Export: Carga finalizada en {schema_name}.{table_name}")