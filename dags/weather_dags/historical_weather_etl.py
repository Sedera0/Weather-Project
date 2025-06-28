from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

# Configuration des chemins
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Solution temporaire - définir DB_URI directement
DB_URI = "dbname=weather_db user=postgres_weather password=0000 host=localhost port=5432"

HISTORICAL_DATA_DIR = PROJECT_ROOT / 'weather_pipeline' / 'data' / 'historical'

def _import_historical_functions():
    """Import avec gestion d'erreur"""
    try:
        from weather_pipeline.scripts.etl.historical_transformer import transform_historical_data
        from weather_pipeline.scripts.etl.historical_loader import load_historical_data
        return transform_historical_data, load_historical_data
    except ImportError as e:
        print(f"Erreur d'import: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'historical_weather_loader',
    default_args=default_args,
    schedule=None,
    catchup=False,
    doc_md="""## Chargement des données historiques""",
    tags=['weather', 'historical'],
) as dag:

    try:
        transform, load = _import_historical_functions()
    except Exception as e:
        raise Exception(f"Échec du chargement des fonctions: {e}")

    transform_task = PythonOperator(
        task_id='transform_historical_data',
        python_callable=transform,
        op_kwargs={'data_dir': str(HISTORICAL_DATA_DIR)}
    )

    load_task = PythonOperator(
        task_id='load_historical_data',
        python_callable=load,
        op_kwargs={
        'db_uri': "dbname=weather_db user=postgres_weather password=0000 host=localhost port=5432"
    }
)

    transform_task >> load_task