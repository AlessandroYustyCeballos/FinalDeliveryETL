import os
import pandas as pd
import finnhub
import yfinance as yf
import sqlite3

PATH = './data'

def Lista_simbolos():
    if os.path.exists("./Symbol.csv"):
        print("Symbol.csv ya existe")
        extraccion_batch()
    else:
        API_KEY = "d669i81r01qots73p5fgd669i81r01qots73p5g0"
        finnhub_client = finnhub.Client(API_KEY)
        data=finnhub_client.forex_symbols('OANDA')
        data=pd.DataFrame(data)

        data_new = data[['displaySymbol', 'symbol']]
        #Crear csv con listados de simbolos
        data_new.to_csv("./Symbol.csv", index=False)
        print("Symbol.csv creado exitosamente")
        extraccion_batch()

def extraccion_batch():
    if os.path.exists("./Symbol.csv"):
        print("Iniciando extraccion batch...")
        data = pd.read_csv("./Symbol.csv")
        SYMBOLS = data['symbol'][range(0,10)] #Borrar el rango para iniciar con todos los simbolos

        for symbol in SYMBOLS:
            symbol2=symbol.replace("OANDA:", "")
            symbol=symbol.replace("OANDA:","")
            symbol=symbol.replace("_","")
            symbol = symbol + "=X"
            data = yf.download(symbol, period="2d", interval="1m")
            if data.empty:
                print(f"Datos no recibidos: symbol: {symbol}")
            else:
                print(f" Datos recibidos: symbol: {symbol}")
                data= pd.DataFrame(data)
                if isinstance(data.columns, pd.MultiIndex):
                    # Check if 'Ticker' is one of the level names and drop it
                    if 'Ticker' in data.columns.names:
                        data.columns = data.columns.droplevel('Ticker')
                    else:
                        data.columns = data.columns.droplevel(1)
                data = data.reset_index()
                # Guardar el archivo CSV
                data.to_csv(f'{PATH}/{symbol2}.csv', index=False)
                print(f"Datos guardados: symbol: {symbol2}")
                transformacion_batch(PATH, symbol2)
                
        print("Extraccion batch completada")
    else:
        print("Symbol.csv no existe")


def transformacion_batch(path, symbol2):
    path_temp = path + "/temp"
    if not os.path.exists(path_temp):
        os.makedirs(path_temp)
    
    print(f"Procesando: {symbol2}")
    if os.path.exists(f'{path}/{symbol2}.csv'):
        try:
            data = pd.read_csv(f'{path}/{symbol2}.csv')

            symbol = None
            for col in data.columns:
                if "Close_" in col:
                   symbol = col.split("Close_")[1]
                   break

            if symbol:
                data = data.rename(columns={
                    f'Close_{symbol}': 'close',
                    f'Open_{symbol}': 'open',
                    f'High_{symbol}': 'high',
                    f'Low_{symbol}': 'low',
                    f'Volume_{symbol}': 'volume'
                })
            else:
                # Si las columnas ya vienen simples y no multi-index
                symbol = symbol2
                data.columns = [str(c).lower() for c in data.columns]

            # Renombrar columna de fecha generada por el reset_index
            data = data.rename(columns={'Datetime': 'timestamp', 'Date': 'timestamp', 'datetime': 'timestamp', 'date': 'timestamp'})
            # Respaldo en caso de que sean datos antiguos mal guardados sin la fecha
            if 'timestamp' not in data.columns:
                data = data.reset_index().rename(columns={'index':'timestamp'})
            # Rellenamos symbol si no lo tenemos en column headers
            if 'symbol' not in data.columns:
                data['symbol'] = symbol
            data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True, errors='coerce')    
            data["timestamp"] = data["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

            # Seleccionamos las requeridas
            data = data[['timestamp','open','high','low','close']]
            data.to_csv(f'{path_temp}/{symbol2}.csv', index=False)
            print(f"Datos guardados: symbol: {symbol2}")
            carga_batch(path_temp, symbol2)



        except Exception as e:
            print(f"Error al transformar: {symbol2} {e}")
    
def carga_batch(path_temp, symbol2):
    try:
        print(f"Cargando: {symbol2}")
        db_path = './DB.db'

        # Abrir o crear la base de datos
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {symbol2} (timestamp DATETIME, open REAL, high REAL, low REAL, close REAL)")
        data = pd.read_csv(f'{path_temp}/{symbol2}.csv')
        
        data.to_sql(symbol2, conn, if_exists='append', index=False)
        conn.commit()
        conn.close()
        print(f"Datos cargados exitosamente: symbol: {symbol2}")
        #Crear log con los simbolos migrados en formato csv
        pd.DataFrame({'symbol': [symbol2]}).to_csv(f'./data/log.csv', mode='a', index=False, header=False)
        #Borrar datos del csv
        os.remove(f'{path_temp}/{symbol2}.csv')
    except Exception as e:
        print(f"Error al cargar: {symbol2} {e}")
    
Lista_simbolos()
    