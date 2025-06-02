import json
import os
from kafka import KafkaConsumer
import sqlite3
from datetime import datetime

# Configurations

STREAM_TOPICS = ['transaction_log']
TABLE_TOPICS = ['TOTAL_PAR_TRANSACTION_TYPE']

ALL_TOPICS = STREAM_TOPICS + TABLE_TOPICS
DATA_LAKE_PATH = os.path.expanduser('~/data_lake_project/data_lake')
DB_PATH = os.path.expanduser('~/data_lake_project/data_warehouse.db')

# Connexion SQLite
conn = sqlite3.connect(DB_PATH)
conn.execute('PRAGMA foreign_keys = ON')
cursor = conn.cursor()

# Consumer Kafka
consumer = KafkaConsumer(
    *ALL_TOPICS,
    bootstrap_servers=['localhost:9092'],
    group_id='data_lake_group',
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Traitement
for message in consumer:
    try:
        data = json.loads(message.value.decode('utf-8'))
        topic = message.topic

        # Gestion du timestamp
        timestamp = data.get('timestamp')

        # Utiliser la date actuelle pour les dossiers
        date = datetime.today().date()

        # Data Lake
        if topic in STREAM_TOPICS:
            folder_path = os.path.expanduser(f'{DATA_LAKE_PATH}/{date}/stream_{topic}')
            os.makedirs(folder_path, exist_ok=True)
            file_path = os.path.join(folder_path, f'{topic}.json')
            with open(file_path, 'a') as f:
                json.dump(data, f)
                f.write('\n')
            # Ajouter status par défaut
            data['status'] = data.get('status', 'completed')
        elif topic in TABLE_TOPICS:
            key = message.key.decode('utf-8') if message.key else 'unknown'
            data['TRANSACTION_TYPE'] = key
            folder_path = os.path.expanduser(f'{DATA_LAKE_PATH}/{date}/table_{topic}')
            os.makedirs(folder_path, exist_ok=True)
            file_path = os.path.join(folder_path, f'{topic}.json')
            with open(file_path, 'w') as f:
                json.dump(data, f)

        # Data Warehouse
        if topic == 'transaction_log':
            cursor.execute('''
            INSERT OR REPLACE INTO transactions (transaction_id, amount, status, timestamp, transaction_type)
            VALUES (?, ?, ?, ?, ?)
            ''', (data['transaction_id'], data['amount'], data['status'], timestamp, data['transaction_type']))
        elif topic == 'TOTAL_PAR_TRANSACTION_TYPE':
            transaction_type = data.get('TRANSACTION_TYPE', 'unknown')
            total_amount = data.get('TOTAL_AMOUNT', 0.0)
            cursor.execute('''
            INSERT OR REPLACE INTO total_par_transaction_type (transaction_type, total_amount)
            VALUES (?, ?)
            ''', (transaction_type, total_amount))

        conn.commit()
    except Exception as e:
        print(f"Error: {e}, Topic: {topic}, Data: {data}")

conn.close()