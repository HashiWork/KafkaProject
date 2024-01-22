import requests
import json
from datetime import datetime
import os

# Remplacez 'YOUR_ACCESS_KEY' par votre clé d'API weatherstack
api_key = 'dd77ce1d99c64732c3fe821e18f2ed64'

# Endpoint de l'API weatherstack pour obtenir les conditions météorologiques actuelles
api_url = f'http://api.weatherstack.com/current?access_key={api_key}&query=Bordeaux'

# Définir le chemin du répertoire "/temp/datakafka/"
output_directory = '/temp/datakafka/'

try:
    # Vérifier si le répertoire existe, sinon le créer
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print(f"Le répertoire {output_directory} a été créé.")

    # Effectuer la requête GET à l'API
    response = requests.get(api_url)
    
    # Vérifier si la requête a réussi (code 200)
    if response.status_code == 200:
        # Charger les données JSON à partir de la réponse
        data = response.json()
        
        # Obtenir la date et l'heure actuelles au format JJ-MM-AAA_HH
        current_time = datetime.now().strftime('%d-%m-%Y_%H')
        
        # Définir le chemin du fichier JSON avec le format de date/heure modifié
        json_file_path = f'{output_directory}weather_data_{current_time}.json'
        
        # Enregistrer les données dans le fichier JSON
        with open(json_file_path, 'w') as json_file:
            json.dump(data, json_file, indent=2)
        
        print(f"Les données ont été enregistrées avec succès dans {json_file_path}")
    else:
        print(f"Erreur lors de la requête à l'API. Code de statut : {response.status_code}")
except Exception as e:
    print(f"Une erreur s'est produite : {e}")
