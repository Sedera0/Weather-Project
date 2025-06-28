import pandas as pd
import numpy as np
from pathlib import Path

def calculate_seasonality(df):
    """Ajoute une classification saisonnière plus précise (version corrigée)"""
    # Conversion explicite en datetime si nécessaire
    if not pd.api.types.is_datetime64_any_dtype(df['month']):
        df['month'] = pd.to_datetime(df['month'])
    
    # Version robuste avec conversion de type explicite
    conditions = [
        df['month'].dt.month.isin([12, 1, 2]),
        df['month'].dt.month.isin([3, 4, 5]),
        df['month'].dt.month.isin([6, 7, 8]),
        df['month'].dt.month.isin([9, 10, 11])
    ]
    choices = np.array(['Hiver', 'Printemps', 'Été', 'Automne'], dtype='object')  # Correction clé ici
    
    df['season'] = np.select(conditions, choices, default='Inconnu')
    return df

def generate_tourism_reports(df, output_dir):
    """
    Génère des rapports mensuels pour les recommandations touristiques (version corrigée)
    """
    # Vérification des colonnes requises
    required_cols = ['city', 'date', 'temp', 'wind_speed', 'weather']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Colonnes manquantes: {missing_cols}")

    # Conversion des types avec gestion des erreurs
    try:
        df['date'] = pd.to_datetime(df['date'])
    except Exception as e:
        raise ValueError(f"Erreur de conversion de date: {str(e)}")

    # Calcul des indicateurs touristiques
    df['ideal_temp'] = df['temp'].between(22, 28).astype(int)
    df['low_wind'] = (df['wind_speed'] < 15).astype(int)
    df['no_rain'] = (~df['weather'].str.contains('rain|drizzle', case=False, na=False)).astype(int)
    df['tourist_score'] = df['ideal_temp'] + df['low_wind'] + df['no_rain']

    # Agrégation mensuelle (avec ME au lieu de M pour éviter le warning)
    monthly_stats = df.groupby(['city', pd.Grouper(key='date', freq='ME')]).agg({
        'temp': ['mean', 'max', 'min'],
        'tourist_score': 'mean',
        'weather': lambda x: x.str.contains('rain').mean()
    }).reset_index()

    # Nettoyage des noms de colonnes
    monthly_stats.columns = ['city', 'month', 'avg_temp', 'max_temp', 'min_temp', 'avg_score', 'rain_probability']

    # Calcul des recommandations (version robuste)
    monthly_stats['recommendation'] = np.where(
        (monthly_stats['avg_score'] >= 2.5) & (monthly_stats['rain_probability'] < 0.2),
        '⭐️⭐️⭐️',
        np.where(
            monthly_stats['avg_score'] >= 1.8,
            '⭐️⭐️',
            '⭐️'
        )
    )

    # Ajout des saisons (après vérification)
    monthly_stats = calculate_seasonality(monthly_stats)

    # Calcul des indicateurs complémentaires
    monthly_stats['temp_range'] = monthly_stats['max_temp'] - monthly_stats['min_temp']
    monthly_stats['comfort_index'] = (0.6 * monthly_stats['avg_temp']) - (0.3 * monthly_stats['rain_probability'] * 100)

    # Sauvegarde
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_path = output_path / 'tourism_recommendations.csv'
    monthly_stats.to_csv(report_path, index=False)
    
    return str(report_path)