from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_taxi_zone_lookup(df: DataFrame, **kwargs):
    logger = kwargs.get('logger')
    
    if df is None or df.empty:
        logger.warning("Export Lookup: DataFrame vacío. Saltando.")
        return

    # Phase: Configuration
    schema_name = 'bronze'
    table_name = 'taxi_zone_lookup'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Phase: Load (Replace)
    logger.info(f"Export Lookup: Sobrescribiendo tabla {schema_name}.{table_name}...")
    
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='replace', # Estrategia ideal y limpia para dimensiones pequeñas
            allow_reserved_words_aliases=True
        )
        
    logger.info("Export Lookup: Carga del catálogo de zonas finalizada con éxito.")