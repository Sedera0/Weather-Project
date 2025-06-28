import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
import logging
import numpy as np

logger = logging.getLogger(__name__)

def load_historical_data(**kwargs):
    """
    Main loading function that handles both direct calls and Airflow context
    Args:
        kwargs: Airflow context and function parameters
    """
    try:
        # Get parameters from Airflow context or direct call
        ti = kwargs.get('ti')
        data = kwargs.get('data') or (ti.xcom_pull(task_ids='transform_historical_data') if ti else None)
        db_uri = kwargs.get('db_uri') or "dbname=weather_db user=postgres_weather password=0000 host=localhost port=5432"
        
        if data is None:
            raise ValueError("No data provided and no XCom data found")
        
        # Convert to DataFrame if needed
        if not isinstance(data, pd.DataFrame):
            df = pd.DataFrame(data)
            logger.info("Converted input data to DataFrame")
        else:
            df = data
        
        # Prepare data
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['city'] = df['city'].astype(str).str.slice(0, 100)
        df['Country'] = df['Country'].astype(str).str.slice(0, 100)
        numeric_cols = ['Latitude', 'Longitude', 'temperature', 'humidity', 'wind_speed']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.replace({np.nan: None})
        
        # Database operations
        with psycopg2.connect(db_uri) as conn:
            with conn.cursor() as cur:
                columns = ['date', 'city', 'Country', 'Latitude', 'Longitude',
                         'temperature', 'humidity', 'wind_speed', 'weather_desc']
                records = [tuple(row) for row in df[columns].itertuples(index=False)]
                
                execute_values(
                    cur,
                    """
                    INSERT INTO historical_weather (
                        date, city, country, latitude, longitude,
                        temperature, humidity, wind_speed, weather_desc
                    ) VALUES %s
                    ON CONFLICT (city, date) DO NOTHING
                    """,
                    records,
                    page_size=1000
                )
        logger.info(f"Successfully loaded {len(records)} rows")
        return True
        
    except Exception as e:
        logger.error(f"Loading failed: {str(e)}", exc_info=True)
        raise