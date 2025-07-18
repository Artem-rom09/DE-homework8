import csv
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_TOPIC = 'tweets'
KAFKA_SERVER = 'kafka:9092'
DATA_SOURCE_FILE = 'amazon_reviews.csv'
MESSAGES_PER_SECOND = 12

print("Connecting to Kafka...")
producer = None
for i in range(5):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Successfully connected to Kafka!")
        break
    except KafkaError as e:
        print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
        time.sleep(5)

if not producer:
    print("Could not connect to Kafka after several retries. Exiting.")
    exit()

# --- Читання файлу та відправка повідомлень ---
print(f"Starting to stream data from {DATA_SOURCE_FILE} to topic '{KAFKA_TOPIC}'...")
# ...
try:
    with open(DATA_SOURCE_FILE, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=',')
        for row in reader:
            print(f"DEBUG: {list(row.keys())}") 
            break                          
            message = {
                'review_id': row.get('review_id'),
                'product_id': row.get('product_id'),
                'customer_id': row.get('customer_id'),
                'star_rating': row.get('star_rating'),
                'review_body': row.get('review_body', '')[:140],
                'stream_timestamp': datetime.now().isoformat()
            }

            # Відправляємо повідомлення
            producer.send(KAFKA_TOPIC, value=message)
            print(f"Sent: {message['review_id']}")

            # Контролюємо швидкість
            time.sleep(1 / MESSAGES_PER_SECOND)

except FileNotFoundError:
    print(f"Error: Data file not found at {DATA_SOURCE_FILE}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

finally:
    print("Flushing and closing producer...")
    producer.flush()
    producer.close()
    print("Producer closed.")