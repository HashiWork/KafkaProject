import os
import json
from kafka import KafkaProducer
import requests
from datetime import datetime
from bs4 import BeautifulSoup

# Configuration du producteur Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         key_serializer=str.encode,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

METEO_URL = "http://192.168.1.43/meteo/"

def fetch_data_from_server(filename):
    url = f"{METEO_URL}{filename}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return None

def get_filenames_from_webpage(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a')
        filenames = [link.get('href') for link in links if link.get('href').endswith('.json')]
        return filenames
    else:
        return []

def filename_to_datetime(filename):
    parts = filename.split('_')
    date_part = parts[2]  # '22-01-2024'
    hour_part = parts[3].split('.')[0]  # '14'
    datetime_str = f"{date_part}_{hour_part}"
    return datetime.strptime(datetime_str, '%d-%m-%Y_%H')

def calculate_averages(data):
    if 'current' in data:
        current_data = data['current']
        return {'avg_temp': current_data.get('temperature', 0), 'avg_humidity': current_data.get('humidity', 0)}
    return None

def run_producer():
    filenames = get_filenames_from_webpage(METEO_URL)
    sorted_filenames = sorted(filenames, key=filename_to_datetime)
    for filename in sorted_filenames:
        data = fetch_data_from_server(filename)
        if data:
            averages = calculate_averages(data)
            if averages:
                datas = str(averages) + " " + str(data)
                timestamp = filename_to_datetime(filename).strftime('%Y-%m-%d %H:%M:%S')
                producer.send('meteo', key=timestamp, value=datas)
                print(f"Message envoyé avec timestamp {timestamp} et moyennes {averages} et données : {data}")

if __name__ == "__main__":
    run_producer()
