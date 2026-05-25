import sqlite3
import pandas as pd
conn = sqlite3.connect('DB.db')
cursor = conn.cursor()
#print(cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())
data = cursor.execute("SELECT * FROM stream").fetchall()
df = pd.DataFrame(data)
print(df)
conn.close()