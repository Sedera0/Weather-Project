import pandas as pd
import json
from pathlib import Path
import numpy as np

def transform_weather_data(input_dir, output_dir):
    """Transforme les fichiers JSON bruts en CSV nettoyés avec indicateurs touristiques"""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    raw_files = list(input_path.glob('*.json'))
    
    processed_data = []
    for file in raw_files:
        try:
            with open(file) as f:
                data = json.load(f)
                processed_data.append({
                    'city': data['name'],
                    'timestamp': data['dt'],
                    'temp': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'wind_speed': data['wind']['speed'],
                    'weather': data['weather'][0]['main'].lower()
                })
        except Exception as e:
            print(f"Erreur traitement {file.name}: {str(e)}")
            continue
    
    if processed_data:
        df = pd.DataFrame(processed_data)
        df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')        
        # Indicateurs touristiques (version corrigée)
        df['ideal_temp'] = df['temp'].between(22, 28)
        df['low_wind'] = df['wind_speed'] < 15
        df['no_rain'] = ~df['weather'].str.contains('rain|drizzle', regex=True, na=False)
        
        # Score composite - conversion explicite en int
        df['tourist_score'] = (
            df['ideal_temp'].astype(int) + 
            df['low_wind'].astype(int) + 
            df['no_rain'].astype(int)
        )
        
        # Version corrigée de np.select()
        conditions = [
            (df['tourist_score'] == 3),
            (df['tourist_score'] == 2),
            (df['tourist_score'] <= 1)
        ]
        choices = ['Excellent', 'Correct', 'Défavorable']
        
        # Conversion explicite des types
        df['travel_advice'] = pd.Series(
            np.select(conditions, choices, default='Inconnu'),
            dtype='string'
        )
        
        # Sauvegarde
        csv_path = output_path / 'weather_processed.csv'
        df.to_csv(csv_path, index=False)
        print(f"Fichier transformé sauvegardé : {csv_path}")
        return df
    else:
        print("Aucune donnée valide à transformer")
        return None