import pandas as pd
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_nyc_data(chunk_data, *args, **kwargs):
    logger = kwargs.get('logger')
    
    service = chunk_data['service_type']
    year = chunk_data['year']
    month = chunk_data['month']
    source_month = chunk_data['source_month']
    
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    file_name = f"{service}_tripdata_{year}-{month:02d}.parquet"
    url = f"{base_url}/{file_name}"
    
    logger.info(f"Ingest: Iniciando descarga directa para {service} - {source_month} desde {url}")
    
    try:
        # Extracción
        df = pd.read_parquet(url)
        
        # Metadata
        df['ingest_ts'] = datetime.utcnow()
        df['source_month'] = source_month
        df['service_type'] = service
        
        df.columns = [c.lower() for c in df.columns]
        
        logger.info(f"Ingest: Descarga exitosa. Filas: {len(df)}")
        return df

    except Exception as e:
        logger.info(f"Ingest: No se pudo descargar {url}. Error: {str(e)}")
        return pd.DataFrame()