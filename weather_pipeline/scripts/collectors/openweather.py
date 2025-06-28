import requests
import json
from datetime import datetime
import os
from pathlib import Path
from weather_pipeline.config.settings import OPENWEATHER_API_KEY

def fetch_weather_data(cities, output_dir):
    """Collecte les données météo pour une liste de villes"""
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    
    # Convertir en Path et créer les dossiers si inexistants
    output_path = Path(output_dir) / 'raw'
    output_path.mkdir(parents=True, exist_ok=True)
    
    for city in cities:
        params = {
            'q': city,
            'appid': OPENWEATHER_API_KEY,
            'units': 'metric'
        }
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            # Sauvegarde avec gestion robuste du chemin
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{city.lower()}_{timestamp}.json"
            filepath = output_path / filename
            
            with open(filepath, 'w') as f:
                json.dump(response.json(), f)
                
            print(f"Données sauvegardées : {filepath}")
                
        except requests.exceptions.RequestException as e:
            print(f"Erreur API pour {city}: {e}")
        except Exception as e:
            print(f"Erreur inattendue pour {city}: {e}")