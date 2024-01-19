from kafka import KafkaConsumer
import json

# Configuration du consommateur
consumer = KafkaConsumer(
    'meteo',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Commence à lire depuis le début si aucun offset n'est enregistré
    group_id='groupe_consommateur_meteo',  # Identifie le groupe de consommateurs
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Désérialise les données JSON reçues
)

def process_message(message):
    # Vous pouvez ajouter ici une logique de traitement supplémentaire si nécessaire
    print(f"Clé: {message.key}, Données Météo: {message.value}")

def main():
    try:
        for message in consumer:
            process_message(message)
            consumer.commit()  # Commit explicitement l'offset après l'affichage
    except KeyboardInterrupt:
        print("Arrêt du consommateur")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
