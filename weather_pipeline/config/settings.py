import os

# API Configuration
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY', '3add5c1cb1905b308451684e842cecf1')

# Database Configuration
DB_URI = "dbname=weather_db user=postgres_weather password=0000 host=localhost port=5432"

# Paths
DATA_DIR = os.path.join(os.path.dirname(__file__), '../data')