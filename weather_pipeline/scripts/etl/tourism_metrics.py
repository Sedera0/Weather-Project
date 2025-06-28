import pandas as pd
from pathlib import Path

def generate_tourism_reports(df, output_dir):
    """
    Génère des rapports mensuels pour les recommandations touristiques
    
    Args:
        df: DataFrame contenant les données météo transformées
        output_dir: Dossier de sortie pour le rapport CSV
    """
    # Vérification des colonnes requises
    required_cols = ['city', 'date', 'temp', 'wind_speed', 'weather']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Colonnes manquantes: {missing_cols}")

    # Conversion des types si nécessaire
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])

    # Calcul des indicateurs touristiques
    df['ideal_temp'] = df['temp'].between(22, 28)
    df['low_wind'] = df['wind_speed'] < 15
    df['no_rain'] = ~df['weather'].str.contains('rain|drizzle', case=False, na=False)
    df['tourist_score'] = df['ideal_temp'].astype(int) + df['low_wind'].astype(int) + df['no_rain'].astype(int)

    # Agrégation mensuelle
    monthly_stats = df.groupby(['city', pd.Grouper(key='date', freq='M')]).agg({
        'temp': ['mean', 'max', 'min'],
        'tourist_score': 'mean',
        'weather': lambda x: x.str.contains('rain').mean()
    }).reset_index()

    # Nettoyage des noms de colonnes
    monthly_stats.columns = ['city', 'month', 'avg_temp', 'max_temp', 'min_temp', 'avg_score', 'rain_probability']

    # Calcul des recommandations
    monthly_stats['recommendation'] = monthly_stats['avg_score'].apply(
        lambda score: '⭐️⭐️⭐️' if score >= 2.5 else '⭐️⭐️' if score >= 1.5 else '⭐️'
    )

    # Sauvegarde
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_path = output_path / 'tourism_recommendations.csv'
    monthly_stats.to_csv(report_path, index=False)
    
    return str(report_path)