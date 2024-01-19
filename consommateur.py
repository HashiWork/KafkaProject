from kafka import KafkaConsumer
import json

# Configuration du consommateur Kafka
consumer = KafkaConsumer(
    'meteo',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Commence à lire depuis le début si aucun offset n'est enregistré
    enable_auto_commit=False,      # Désactive le commit automatique des offsets
    group_id='groupe_consommateur_meteo',  # Identifie le groupe de consommateurs
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Désérialise les données JSON reçues
)

def main():
    try:
        for message in consumer:
            # Affichage des données et de la clé
            print(f"Clé: {message.key}, Données: {message.value}")

            # Commit explicite de l'offset
            consumer.commit()
    except KeyboardInterrupt:
        print("Arrêt du consommateur")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
