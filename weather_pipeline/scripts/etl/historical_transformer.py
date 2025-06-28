import pandas as pd
from pathlib import Path

def transform_historical_data(data_dir):
    """Combine tous les fichiers Kaggle en un DataFrame normalisé"""
    data_path = Path(data_dir)
    
    # 1. Chargement des villes
    cities = pd.read_csv(data_path / 'city_attributes.csv')
    cities = cities.rename(columns={'City': 'city'})
    
    # 2. Fusion des données météo
    metrics = {
        'temperature': pd.read_csv(data_path / 'temperature.csv'),
        'humidity': pd.read_csv(data_path / 'humidity.csv'),
        'wind_speed': pd.read_csv(data_path / 'wind_speed.csv'),
        'weather_desc': pd.read_csv(data_path / 'weather_description.csv')
    }
    
    # 3. Normalisation
    dfs = []
    for metric_name, df in metrics.items():
        melted = df.melt(id_vars=['datetime'], 
                        var_name='city', 
                        value_name=metric_name)
        dfs.append(melted.set_index(['datetime', 'city']))
    
    # 4. Fusion finale
    combined = pd.concat(dfs, axis=1).reset_index()
    final_df = combined.merge(cities, on='city')
    
    # 5. Nettoyage
    final_df['datetime'] = pd.to_datetime(final_df['datetime'])
    final_df = final_df.rename(columns={'datetime': 'date'})
    
    return final_df[['date', 'city', 'Country', 'Latitude', 'Longitude',
                    'temperature', 'humidity', 'wind_speed', 'weather_desc']]