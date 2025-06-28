from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys
import pandas as pd

PROJECT_ROOT = Path(__file__).absolute().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'weather_pipeline' / 'data'
sys.path.insert(0, str(PROJECT_ROOT))

def _import_functions():
    """Import séparé pour éviter les circulaires"""
    from weather_pipeline.scripts.collectors.openweather import fetch_weather_data
    from weather_pipeline.scripts.etl.transformer import transform_weather_data
    from weather_pipeline.scripts.etl.tourism_metrics import generate_tourism_reports
    return fetch_weather_data, transform_weather_data, generate_tourism_reports

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_tourism_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['weather', 'tourism'],
) as dag:

    fetch_weather_data, transform_weather_data, generate_tourism_reports = _import_functions()

    # Import différé du loader pour éviter les circulaires
    from weather_pipeline.scripts.etl.loader import load_to_postgres

    extract = PythonOperator(
        task_id='extract_weather_data',
        python_callable=fetch_weather_data,
        op_kwargs={
            'cities': ['Paris', 'London', 'Tokyo'],
            'output_dir': str(DATA_DIR)
        }
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_weather_data,
        op_kwargs={
            'input_dir': str(DATA_DIR / 'raw'),
            'output_dir': str(DATA_DIR / 'processed')
        }
    )

    analyze = PythonOperator(
    task_id='generate_tourism_reports',
    python_callable=lambda **kwargs: generate_tourism_reports(
        df=pd.read_csv(kwargs['input_path']),
        output_dir=kwargs['output_dir']
    ),
    op_kwargs={
        'input_path': str(DATA_DIR / 'processed' / 'weather_processed.csv'),
        'output_dir': str(DATA_DIR / 'reports')
    },
    execution_timeout=timedelta(minutes=10)
)

    load_weather = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_to_postgres,
        op_kwargs={
            'csv_path': str(DATA_DIR / 'processed' / 'weather_processed.csv'),
            'table_name': 'weather_data'
        }
    )

    load_tourism = PythonOperator(
        task_id='load_tourism_data',
        python_callable=load_to_postgres,
        op_kwargs={
            'csv_path': str(DATA_DIR / 'reports' / 'tourism_recommendations.csv'),
            'table_name': 'tourism_recommendations'
        }
    )

    extract >> transform >> [analyze, load_weather]
    analyze >> load_tourism