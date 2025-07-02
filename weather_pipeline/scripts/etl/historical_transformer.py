import pandas as pd
from pathlib import Path
import numpy as np

def transform_historical_data(data_dir):
    """Combine tous les fichiers Kaggle en un DataFrame normalisé avec validation des données"""
    data_path = Path(data_dir)
    
    # 1. Chargement des villes avec validation
    try:
        cities = pd.read_csv(data_path / 'city_attributes.csv')
        required_city_cols = ['City', 'Country', 'Latitude', 'Longitude']
        if not all(col in cities.columns for col in required_city_cols):
            raise ValueError("Fichier city_attributes.csv ne contient pas toutes les colonnes requises")
        cities = cities.rename(columns={'City': 'city'})
    except Exception as e:
        raise ValueError(f"Erreur lors du chargement des villes: {str(e)}")
    
    # 2. Chargement et validation des données météo
    metric_files = {
        'temperature': 'temperature.csv',
        'humidity': 'humidity.csv',
        'wind_speed': 'wind_speed.csv',
        'weather_desc': 'weather_description.csv'
    }
    
    dfs = []
    for metric_name, filename in metric_files.items():
        try:
            df = pd.read_csv(data_path / filename)
            
            # Validation des colonnes
            if 'datetime' not in df.columns:
                raise ValueError(f"Colonne 'datetime' manquante dans {filename}")
                
            melted = df.melt(
                id_vars=['datetime'],
                var_name='city',
                value_name=metric_name
            )
            
            # Conversion des valeurs numériques
            if metric_name in ['temperature', 'humidity', 'wind_speed']:
                melted[metric_name] = pd.to_numeric(melted[metric_name], errors='coerce')
                
            dfs.append(melted.set_index(['datetime', 'city']))
            
        except Exception as e:
            raise ValueError(f"Erreur lors du traitement de {filename}: {str(e)}")
    
    # 3. Fusion finale
    try:
        combined = pd.concat(dfs, axis=1).reset_index()
        final_df = combined.merge(cities, on='city')
    except Exception as e:
        raise ValueError(f"Erreur lors de la fusion des données: {str(e)}")
    
    # 4. Nettoyage et validation approfondie
    try:
        # Conversion de la date
        final_df['date'] = pd.to_datetime(final_df['datetime'], errors='coerce')
        
        # Suppression des lignes avec dates invalides
        final_df = final_df[final_df['date'].notna()]
        
        # Correction des températures (conversion Kelvin -> Celsius si > 100)
        final_df['temperature'] = np.where(
            final_df['temperature'] > 100,
            final_df['temperature'] - 273.15,
            final_df['temperature']
        )
        
        # Validation des plages de valeurs
        final_df = final_df[
            (final_df['temperature'].between(-50, 60)) &
            (final_df['humidity'].between(0, 100)) &
            (final_df['wind_speed'] >= 0)
        ]
        
        # Formatage final
        final_df = final_df[[
            'date', 'city', 'Country', 'Latitude', 'Longitude',
            'temperature', 'humidity', 'wind_speed', 'weather_desc'
        ]]
        
        # Vérification du résultat final
        if final_df.empty:
            raise ValueError("Aucune donnée valide après nettoyage")
            
        return final_df
    
    except Exception as e:
        raise ValueError(f"Erreur lors du nettoyage final: {str(e)}")