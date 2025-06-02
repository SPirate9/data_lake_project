import sqlite3
import os

DB_PATH = os.path.expanduser('~/data_lake_project/data_warehouse.db')
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    amount REAL,
    status TEXT,
    timestamp TEXT,
    transaction_type TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS total_par_transaction_type (
    transaction_type TEXT PRIMARY KEY,
    total_amount REAL
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    email TEXT,
    last_updated TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS permissions (
    user_id INTEGER,
    folder_path TEXT,
    access_level TEXT,
    PRIMARY KEY (user_id, folder_path),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
INSERT OR IGNORE INTO users (user_id, username, email, last_updated)
VALUES (1, 'Saad', 'saad.s@example.com', '2025-04-27T16:35:33.000Z'),
       (2, 'noam', 'noam@example.com', '2025-04-27T16:35:33.000Z')
''')

folder_path = os.path.expanduser('~/data_lake_project/data_lake/2025-04-27/stream_transaction_log')
cursor.execute('''
INSERT OR IGNORE INTO permissions (user_id, folder_path, access_level)
VALUES (1, ?, 'read')
''', (folder_path,))

conn.commit()
conn.close()
print("Base de données initialisée avec succès.")