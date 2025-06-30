from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import psycopg2
import sys

# Configuration des chemins
PROJECT_ROOT = Path(__file__).absolute().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'weather_pipeline' / 'data'
sys.path.insert(0, str(PROJECT_ROOT))

# Configuration de la base de données
DB_CONFIG = {
    'host': 'localhost',
    'database': 'weather_db',
    'user': 'postgres_weather',
    'password': '0000',
    'port': '5432'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def _import_functions():
    """Import explicite des fonctions nécessaires"""
    from weather_pipeline.scripts.collectors.openweather import fetch_weather_data
    from weather_pipeline.scripts.etl.transformer import transform_weather_data
    from weather_pipeline.scripts.etl.tourism_metrics import generate_tourism_reports
    from weather_pipeline.scripts.etl.loader import load_to_postgres
    return fetch_weather_data, transform_weather_data, generate_tourism_reports, load_to_postgres

def get_historical_data(cities):
    """Récupère les données historiques depuis PostgreSQL"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            query = """
                SELECT date, city, temperature as temp, 
                       wind_speed, weather_desc as weather
                FROM historical_weather
                WHERE city = ANY(%s)
            """
            return pd.read_sql(query, conn, params=(cities,))
    except Exception as e:
        print(f"Warning: Impossible de charger les historiques - {str(e)}")
        return pd.DataFrame()

def generate_reports_wrapper(input_path, output_dir):
    """Wrapper pour intégrer les données historiques"""
    # Import explicite dans la fonction
    from weather_pipeline.scripts.etl.tourism_metrics import generate_tourism_reports
    
    # Chargement des données actuelles
    current_df = pd.read_csv(input_path)
    
    # Récupération des données historiques
    cities = current_df['city'].unique().tolist()
    historical_df = get_historical_data(cities)
    
    # Génération du rapport complet
    return generate_tourism_reports(
        df=pd.concat([current_df, historical_df], ignore_index=True),
        output_dir=output_dir,
        db_uri=None  # Optionnel pour usage futur
    )

with DAG(
    'weather_tourism_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['weather', 'tourism'],
) as dag:

    # Import explicite des fonctions
    fetch_weather_data, transform_weather_data, _, load_to_postgres = _import_functions()

    # Définition des tâches
    extract = PythonOperator(
        task_id='extract_weather_data',
        python_callable=fetch_weather_data,
        op_kwargs={
            'cities': [  'Vancouver',  'Portland',  'San Francisco',  'Seattle',  'Los Angeles',  'San Diego',  'Las Vegas',  'Phoenix',  'Albuquerque',  'Denver',  'San Antonio',  'Dallas',  'Houston',  'Kansas City', 'Minneapolis',  'Saint Louis',  'Chicago',  'Nashville',  'Indianapolis',  'Atlanta',  'Detroit',  'Jacksonville',  'Charlotte',  'Miami',  'Pittsburgh',  'Toronto',  'Philadelphia',
            'New York',  'Montreal',  'Boston',  'Beersheba',  'Tel Aviv District',  'Eilat',  'Haifa',  'Nahariyya',  'Jerusalem'],
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
        python_callable=generate_reports_wrapper,
        op_kwargs={
            'input_path': str(DATA_DIR / 'processed' / 'weather_processed.csv'),
            'output_dir': str(DATA_DIR / 'reports')
        }
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_to_postgres,
        op_kwargs={
            'csv_path': str(DATA_DIR / 'reports' / 'tourism_recommendations.csv'),
            'table_name': 'tourism_recommendations'
        }
    )

    # Orchestration
    extract >> transform >> analyze >> load