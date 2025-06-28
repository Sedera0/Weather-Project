from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'postgres_operations_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['postgres', 'test']
) as dag:

    # Test de connexion simple
    test_connection = SQLExecuteQueryOperator(
        task_id='test_connection',
        conn_id='postgres_weather',
        sql='SELECT 1',
        autocommit=True
    )

    # Création de table
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_weather',
        sql="""
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            test_value VARCHAR(100)
        )
        """,
        autocommit=True
    )

    # Insertion de données
    insert_data = SQLExecuteQueryOperator(
        task_id='insert_data',
        conn_id='postgres_weather',
        sql="""
        INSERT INTO test_table (test_value)
        VALUES ('Connection test successful')
        """,
        autocommit=True
    )

    # Définir l'ordre d'exécution
    test_connection >> create_table >> insert_data