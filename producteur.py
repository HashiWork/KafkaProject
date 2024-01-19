import json
from kafka import KafkaProducer
import time
import requests
from datetime import datetime, timedelta

# Configuration du producteur Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         key_serializer=str.encode,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_data_from_server(timestamp):
    url = f"http://172.16.12.92/weather_data_08-44-14.json"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return None

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
        current_time = datetime.now()
        formatted_time = current_time.strftime("%H-%M-%S")
        filename = f"weather_data_{formatted_time}.json"
        data = fetch_data_from_server(filename)

        if data:
            averages = calculate_averages(data)
            if averages:
                producer.send('meteo', key=formatted_time, value=averages)
                print(f"Message envoyé avec timestamp {formatted_time} et moyennes {averages} et {data}")

        time.sleep(60)  # Attendre une minute avant de chercher les nouvelles données

if __name__ == "__main__":
    run_producer()
