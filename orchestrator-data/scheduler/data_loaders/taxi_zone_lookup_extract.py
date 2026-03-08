import pandas as pd
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_taxi_zone_lookup(*args, **kwargs):
    logger = kwargs.get('logger')
    
    # URL oficial del catálogo de zonas de NYC TLC
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
    logger.info(f"Ingest Lookup: Descargando catálogo de zonas desde {url}")
    
    # Phase: Extraction
    df = pd.read_csv(url)
    
    # Phase: Metadata (Adaptado para catálogo estático)
    df['ingest_ts'] = datetime.utcnow()
    df['source_month'] = 'static'
    df['service_type'] = 'lookup'
    
    # Normalizar nombres de columnas
    df.columns = [c.lower() for c in df.columns]
    
    logger.info(f"Ingest Lookup: Descarga exitosa. Filas: {len(df)}")
    
    return df