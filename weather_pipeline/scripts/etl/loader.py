import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from pathlib import Path
import logging
from weather_pipeline.config.settings import DB_URI

logger = logging.getLogger(__name__)

def load_to_postgres(csv_path, table_name='weather_data'):
    """Charge les données avec UPSERT pour tourism_recommendations"""
    try:
        # Vérification du fichier
        csv_file = Path(csv_path)
        if not csv_file.exists():
            raise FileNotFoundError(f"Fichier introuvable: {csv_path}")

        # Lecture du CSV
        df = pd.read_csv(csv_file)
        if df.empty:
            logger.warning("Avertissement: Le fichier CSV est vide")
            return False

        # Connexion à PostgreSQL
        conn = psycopg2.connect(DB_URI)
        cur = conn.cursor()

        # Vérification que la table existe (optionnel)
        cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s)", (table_name,))
        if not cur.fetchone()[0]:
            raise ValueError(f"La table {table_name} n'existe pas")

        # Conversion des données
        columns = df.columns.tolist()
        tuples = [tuple(x) for x in df.to_numpy()]

        # Requête SQL différenciée
        if table_name == 'tourism_recommendations':
            insert_sql = sql.SQL("""
                INSERT INTO {} ({}) VALUES %s
                ON CONFLICT (city, month) DO UPDATE SET
                    avg_temp = EXCLUDED.avg_temp,
                    max_temp = EXCLUDED.max_temp,
                    min_temp = EXCLUDED.min_temp,
                    avg_score = EXCLUDED.avg_score,
                    recommendation = EXCLUDED.recommendation
            """).format(
                sql.Identifier(table_name),
                sql.SQL(',').join(map(sql.Identifier, columns))
            )
        else:
            insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(table_name),
                sql.SQL(',').join(map(sql.Identifier, columns))
            )

        # Exécution
        execute_values(cur, insert_sql, tuples)
        conn.commit()
        logger.info(f"Succès: {len(df)} lignes chargées dans {table_name}")
        return True

    except Exception as e:
        logger.error(f"Erreur: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'conn' in locals():
            conn.close()