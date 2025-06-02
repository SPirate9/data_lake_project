# pipeline.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import time
import json
import os
import schedule
import logging
import sqlite3

# Configurations
DATA_LAKE_PATH = os.path.expanduser('~/data_lake_project/data_lake')
DB_PATH = os.path.expanduser('~/data_lake_project/data_warehouse.db')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MyOptions(PipelineOptions):
    pass

def read_data_lake():
    today = time.strftime('%Y-%m-%d')
    data = []
    for folder in ['stream_transaction_log', 'table_TOTAL_PAR_TRANSACTION_TYPE']:
        file_path = os.path.join(DATA_LAKE_PATH, today, folder, f'{folder.split("_")[1]}.json')
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                if 'stream_' in folder:
                    data.extend([json.loads(line) for line in f])
                else:
                    data.append(json.load(f))
            print(f"Read from {file_path}: {data[-1] if data else 'No data'}")
    return data

class WriteToSQLite(beam.DoFn):
    def setup(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.execute('PRAGMA foreign_keys = ON')
        self.cursor = self.conn.cursor()

    def process(self, element):
        data = element
        if 'TRANSACTION_ID' in data:
            self.cursor.execute('''
            INSERT OR REPLACE INTO transactions (transaction_id, amount, status, timestamp, transaction_type)
            VALUES (?, ?, ?, ?, ?)
            ''', (data['TRANSACTION_ID'], data['AMOUNT'], data['status'], data['timestamp'], data['TRANSACTION_TYPE']))
        elif 'TOTAL_AMOUNT' in data:
            transaction_type = data.get('TRANSACTION_TYPE', 'unknown')
            total_amount = data.get('TOTAL_AMOUNT', 0.0)
            print(f"Pipeline inserting: {transaction_type}, {total_amount}")
            self.cursor.execute('''
            INSERT OR REPLACE INTO total_par_transaction_type (transaction_type, total_amount)
            VALUES (?, ?)
            ''', (transaction_type, total_amount))
        self.conn.commit()
        yield data

    def teardown(self):
        self.conn.close()

def run_pipeline():
    logger.info("Lancement du pipeline Dataflow...")
    options = MyOptions(
        project='mon-projet-dataflow',
        job_name=f'datalake-job-{int(time.time())}',
        runner='DataflowRunner',
        region='us-central1',
        temp_location='gs://mon-bucket-dataflow/temp',
        staging_location='gs://mon-bucket-dataflow/staging'
    )
    with beam.Pipeline(options=options) as p:
        (
            p 
            | "Lire Data Lake" >> beam.Create(read_data_lake())
            | "Écrire dans SQLite" >> beam.ParDo(WriteToSQLite())
            | "Afficher" >> beam.Map(lambda x: print(f"Donnée traitée: {x}"))
        )
    logger.info("Pipeline soumis à Dataflow.")

def schedule_pipeline():
    logger.info("Planification du pipeline toutes les 10 minutes...")
    schedule.every(10).minutes.do(run_pipeline)
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    logger.info("Démarrage du script...")
    run_pipeline()
    schedule_pipeline()