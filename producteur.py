import json
from kafka import KafkaProducer
import time
import requests
from datetime import datetime
from pathlib import Path

# Configuration du producteur Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         key_serializer=str.encode,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_data_from_server(filename):
    url = f"http://172.16.12.92/{filename}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return None

def filename_to_timestamp(filename):
    # Extraire l'heure du nom du fichier et la transformer
    file_name = Path(filename).stem
    file_hour = file_name[13:21]
    return file_hour

def calculate_averages(data):
    # Assurez-vous que ces clés existent dans vos données
    if 'current' in data:
        current_data = data['current']
        temp = current_data.get('temperature', 0)
        humidity = current_data.get('humidity', 0)
        return {'avg_temp': temp, 'avg_humidity': humidity}
    return None

def run_producer():
    while True:
        filename = f"weather_data_08-52-02.json"
        data = fetch_data_from_server(filename)

        if data:
            averages = calculate_averages(data)
            if averages:
                datas = str(averages) + " " + str(data)
                timestamp = filename_to_timestamp(filename)
                producer.send('meteo', key=timestamp, value=datas)
                print(f"Message envoyé avec timestamp {timestamp} et moyennes {averages} et données : {data}")

        time.sleep(1)  # Attendre une minute avant de chercher les nouvelles données

if __name__ == "__main__":
    run_producer()
