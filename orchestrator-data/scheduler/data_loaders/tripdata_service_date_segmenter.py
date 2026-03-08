import pandas as pd
from typing import Dict, List

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


# Data chunker por meses
@data_loader
def generate_load_plan(*args, **kwargs):
    # Configuración
    start_date = kwargs.get('start_date', '2022-01-01')
    end_date = kwargs.get('end_date', '2023-12-31')
    services = ['yellow', 'green']
    
    # Rango de fechas
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')
    chunks = []
    metadata = []
    
    for date in dates:
        year = date.year
        month = date.month
        month_str = f"{year}-{month:02d}"
        
        for service in services:
            chunks.append({
                'year': year,
                'month': month,
                'source_month': month_str,
                'service_type': service
            })
            
            metadata.append({
                'block_uuid': f"ingest_{service}_{year}_{month:02d}"
            })
            
    return [chunks, metadata]