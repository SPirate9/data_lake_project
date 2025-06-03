import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data_warehouse.db')
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
INSERT OR IGNORE INTO permissions (user_id, folder_path, access_level)
VALUES (1, '2025-04-27', 'read'),
       (2, '2025-04-27', 'read')
''')

conn.commit()
conn.close()
print("Base de données initialisée avec succès.")