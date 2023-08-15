import sqlite3

conn = sqlite3.connect('30K.sqlite')
cursor = conn.cursor()
cursor.execute('SELECT token_C FROM tokens_T')
tokens = [row[0] for row in cursor.fetchall()]
cursor.close()
conn.close()
print(tokens)
