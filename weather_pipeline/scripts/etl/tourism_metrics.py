import pandas as pd
import numpy as np
from pathlib import Path
import psycopg2
from psycopg2 import sql
import logging
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import create_engine


logger = logging.getLogger(__name__)

def calculate_seasonality(df):
    """Ajoute une classification saisonnière plus précise (version corrigée)"""
    if not pd.api.types.is_datetime64_any_dtype(df['month']):
        df['month'] = pd.to_datetime(df['month'])
    
    conditions = [
        df['month'].dt.month.isin([12, 1, 2]),
        df['month'].dt.month.isin([3, 4, 5]),
        df['month'].dt.month.isin([6, 7, 8]),
        df['month'].dt.month.isin([9, 10, 11])
    ]
    choices = np.array(['Hiver', 'Printemps', 'Été', 'Automne'], dtype='object')
    
    df['season'] = np.select(conditions, choices, default='Inconnu')
    return df

def fetch_historical_data(db_uri, cities=None):
    """Récupère les données historiques depuis PostgreSQL - Version corrigée"""
    try:
        
        # Création du moteur SQLAlchemy
        engine = create_engine(db_uri)
        
        # Construction de la requête
        base_query = """
            SELECT 
                date, city, temperature as temp, 
                wind_speed, weather_desc as weather
            FROM historical_weather
        """
        
        if cities:
            # Version sécurisée avec paramètres
            placeholders = ','.join(['%s'] * len(cities))
            query = f"{base_query} WHERE city IN ({placeholders})"
            params = tuple(cities)
        else:
            query = base_query
            params = None
        
        # Exécution avec pandas
        with engine.connect() as connection:
            historical_df = pd.read_sql(
                sql=query,
                con=connection,
                params=params
            )
        
        # Post-traitement
        if not historical_df.empty:
            historical_df['weather'] = historical_df['weather'].str.lower()
            historical_df['date'] = pd.to_datetime(historical_df['date'])
        
        return historical_df
    
    except Exception as e:
        logger.error(f"Erreur historique: {str(e)}", exc_info=True)
        return pd.DataFrame()  # Retourne un DataFrame vide en cas d'erreur

def generate_tourism_reports(df, output_dir, db_uri=None):
    """
    Génère des rapports en combinant données actuelles et historiques
    Args:
        df: DataFrame des données actuelles (from OpenWeather)
        output_dir: Répertoire de sortie
        db_uri: URI de la base de données pour les historiques (optionnel)
    """
    # Vérification des colonnes requises
    required_cols = ['city', 'date', 'temp', 'wind_speed', 'weather']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Colonnes manquantes: {missing_cols}")

    # Conversion des dates
    df['date'] = pd.to_datetime(df['date'])

    # Récupération des données historiques si db_uri est fourni
    if db_uri:
        try:
            cities = df['city'].unique().tolist()
            historical_df = fetch_historical_data(db_uri, cities)
            
            if not historical_df.empty:
                # Fusion des données
                df = pd.concat([df, historical_df], ignore_index=True)
                logger.info(f"Données historiques ajoutées: {len(historical_df)} enregistrements")
        except Exception as e:
            logger.warning(f"Impossible d'ajouter les historiques: {str(e)}")

    # Calcul des indicateurs touristiques
    df['ideal_temp'] = df['temp'].between(22, 28).astype(int)
    df['low_wind'] = (df['wind_speed'] < 15).astype(int)
    df['no_rain'] = (~df['weather'].str.contains('rain|drizzle', case=False, na=False)).astype(int)
    df['tourist_score'] = df['ideal_temp'] + df['low_wind'] + df['no_rain']

    # Agrégation mensuelle
    monthly_stats = df.groupby(['city', pd.Grouper(key='date', freq='ME')]).agg({
        'temp': ['mean', 'max', 'min'],
        'tourist_score': 'mean',
        'weather': lambda x: x.str.contains('rain').mean()
    }).reset_index()

    # Nettoyage des noms de colonnes
    monthly_stats.columns = ['city', 'month', 'avg_temp', 'max_temp', 'min_temp', 'avg_score', 'rain_probability']

    # Recommandations
    monthly_stats['recommendation'] = np.where(
        (monthly_stats['avg_score'] >= 2.5) & (monthly_stats['rain_probability'] < 0.2),
        '⭐️⭐️⭐️',
        np.where(
            monthly_stats['avg_score'] >= 1.8,
            '⭐️⭐️',
            '⭐️'
        )
    )

    # Ajout des saisons et indicateurs
    monthly_stats = calculate_seasonality(monthly_stats)
    monthly_stats['temp_range'] = monthly_stats['max_temp'] - monthly_stats['min_temp']
    monthly_stats['comfort_index'] = (0.6 * monthly_stats['avg_temp']) - (0.3 * monthly_stats['rain_probability'] * 100)

    # Sauvegarde
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_path = output_path / 'tourism_recommendations.csv'
    monthly_stats.to_csv(report_path, index=False)
    
    return str(report_path)