import os
import pandas as pd
import sqlite3

PATH = './data'
path_temp = PATH + '/temp'

def carga_stream():
    try:
        data = pd.read_csv(f"{path_temp}/stream_transformado.csv", dtype={'price': str})
        conn = sqlite3.connect('./DB.db')
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS stream (symbol TEXT, timestamp TEXT, price VARCHAR(30))")
        data.to_sql('stream', conn, if_exists='append', index=False)
        conn.commit()
        conn.close()
        print(f"Datos cargados exitosamente: stream")
    except Exception as e:
        print(f"Error al cargar: stream {e}")

if not os.path.exists(path_temp):
    os.makedirs(path_temp)

try:
    data = pd.read_csv(f"{PATH}/stream.csv", dtype={'p': str, 'price': str})
    data = pd.DataFrame(data)
    
    # Finnhub devuelve los datos con llaves cortas: p (price), s (symbol), t (timestamp), v (volume)
    # Renombramos a los esperados si vienen en ese formato
    rename_map = {
        "p": "price",
        "s": "symbol",
        "t": "timestamp",
        "v": "volume"
    }
    # Solo renombramos si encontramos estas columnas, por si ya venían bien formateadas
    data = data.rename(columns=lambda x: rename_map.get(x, x))

    # Verificar si existe timestamp, si no abortar
    if "timestamp" not in data.columns:
        raise ValueError(f"Falta columna 'timestamp'. Columnas encontradas: {data.columns.tolist()}")

    # Finnhub manda el timestamp en milisegundos (Unix timestamp)
    if pd.api.types.is_numeric_dtype(data['timestamp']):
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms', utc=True)
    else:
        data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True, errors='coerce')

    data['symbol'] = data['symbol'].str.replace("OANDA:","")

    data = data[["symbol","timestamp","price"]]

    data.to_csv(f"{path_temp}/stream_transformado.csv", index=False)
    
    print(f" Datos transformados correctamente.")
    carga_stream()
    

except Exception as e:
    print(f" Error al transformar los datos: {e}")


