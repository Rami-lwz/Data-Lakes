import sqlite3
# Connexion à une base de données (ou création si elle n'existe pas)

conn = sqlite3.connect('/home/rami/Desktop/efrei/Datalakes2/Data-Lakes/db/test.db')

cursor = conn.cursor()
# Création d'une table de test
cursor.execute('CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, value TEXT)')
cursor.execute('INSERT INTO test_table (value) VALUES ("Hello, SQLite!")')
conn.commit()
# Interrogation de la base
cursor.execute('SELECT * FROM test_table')
print(cursor.fetchall())
conn.close()