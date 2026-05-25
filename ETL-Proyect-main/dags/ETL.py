from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import os
import requests

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS (Basado en Codespaces)
# ---------------------------------------------------------
# Usamos la variable de entorno de Airflow, o el directorio por defecto del contenedo

BASE_PATH = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
yahoo_data = f"{BASE_PATH}/data/yahoo.csv"
finhub_data = f"{BASE_PATH}/data/finhub.csv"
Temp_path = f"{BASE_PATH}/data/temp"


#1. Definir argumentos por defecto
default_args = {
     'owner': 'data_enginner',
     'depends_on_past': False,
     'retries': 1,
     'retry_delay': timedelta(seconds=15),
     }

# ---------------------------------------------------------
# Declaramos las funciones de extraccion
# ---------------------------------------------------------

def extraccion_yahoo():
    if not os.path.exists(Temp_path):
        os.makedirs(Temp_path)    
# Iniciamos extrayendo los datos por lotes, los cuales serán nuestra base o perfil de datos
    print(f" Extrayendo datos...")
    if not os.path.exists(yahoo_data):
        print("No se encontro el archivo yahoo.csv") 
        return False 
    else:
        try:
            data_yahoo = pd.read_csv(yahoo_data)
            data_yahoo.to_csv(f"{Temp_path}/yahoo.csv", index=False)
            print(f" Datos extraidos correctamente")
            return 'transformacion_yahoo'
        except Exception as e:
            print(f" Error al extraer los datos: {e}")
            return False

def extraccion_finhub():
# Ahora extraemos los datos de finhub
    try:
        if not os.path.exists(finhub_data):
            raise FileNotFoundError("No se encontro el archivo finhub.csv")
            return False
        data_finhub = pd.read_csv(finhub_data)
        data_finhub.to_csv(f"{Temp_path}/finhub.csv", index=False)
        print(f" Datos extraidos correctamente")
        return 'transformacion_finhub'
    except Exception as e:
        print(f" Error al extraer los datos: {e}")
        return False

def Alpha():
    API_KEY = "3E1NL1R2CK7L2AIW"
    url = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=EUR&to_currency=USD&apikey={API_KEY}"
    data = requests.get(url).json()
    
    # Manejo del Rate Limit o Error de API
    if 'Realtime Currency Exchange Rate' not in data:
        raise ValueError(f"Respuesta inesperada de AlphaVantage (¿Límite de API alcanzado?): {data}")
        
    return data['Realtime Currency Exchange Rate']

def extraccion_alpha():
# Ahora extraemos los datos de alpha local usando la función definida
    try:
        if os.path.exists(f"{Temp_path}/alpha.csv"):
            os.remove(f"{Temp_path}/alpha.csv")
        data_alpha = Alpha()
        if not isinstance(data_alpha, dict):
            raise ValueError(f"Respuesta inválida de Alpha: {data_alpha}")
        data_alpha = pd.DataFrame([data_alpha])
        data_alpha.to_csv(f"{Temp_path}/alpha.csv", index=False)
        print(f" Datos extraidos correctamente")
        return 'transformacion_alpha'
    except Exception as e:
        if os.path.exists(f"{Temp_path}/alpha.csv"):
            os.remove(f"{Temp_path}/alpha.csv")
        print(f" Error al extraer los datos: {e}")
        return False

# ---------------------------------------------------------
# Declaramos las funciones de transformacion
# ---------------------------------------------------------

def transformacion_yahoo():
    print(f" Transformando datos de yahoo...")
    if os.path.exists(f"{Temp_path}/yahoo.csv"):
        try:
            data = pd.read_csv(f"{Temp_path}/yahoo.csv")
            
            # Buscar el simbolo si está en el nombre de las columnas (ej: Close_EURUSD=X)
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
                symbol = "Unknown"
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
            data = data[['timestamp','symbol','open','high','low','close']]

            data.to_csv(f"{Temp_path}/yahoo_transformado.csv", index=False)
            os.remove(f"{Temp_path}/yahoo.csv")
            print(f" Datos transformados correctamente")
            return f"{Temp_path}/yahoo_transformado.csv"
        except Exception as e:
            print(f" Error al transformar los datos: {e}")
            return False

def transformacion_finhub():
    if os.path.exists(f"{Temp_path}/finhub.csv"):
        print(f" Transformando datos de finhub...")
        try:
            data = pd.read_csv(f"{Temp_path}/finhub.csv")
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
                
            # Agrupar por minuto y calcular OHLC
            ohlc = (
                data.groupby(["symbol", data["timestamp"].dt.floor("min")])["price"]
                .agg(open="first", high="max", low="min", close="last")
                .reset_index()
            )
            # Renombrar columnas para claridad
            ohlc = ohlc.rename(columns={"timestamp": "minute"})

            ohlc["timestamp"] = ohlc["minute"]
            # Convertimos la fecha al mismo formato que yahoo string
            ohlc["timestamp"] = ohlc["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

            data = ohlc[["timestamp", "symbol", "open", "high", "low", "close"]]

            data.to_csv(f"{Temp_path}/finhub_transformado.csv", index=False)
            os.remove(f"{Temp_path}/finhub.csv")
            print(f" Datos transformados correctamente{data}")
            return f"{Temp_path}/finhub_transformado.csv"

        except Exception as e:
            print(f" Error al transformar los datos: {e}")
            return False

def transformacion_alpha():
    if os.path.exists(f"{Temp_path}/alpha.csv"):
        print(f" Transformando datos de alpha...")
        try:
            data = pd.read_csv(f"{Temp_path}/alpha.csv")
            
            expected_cols = [
                '1. From_Currency Code',
                '3. To_Currency Code',
                '5. Exchange Rate',
                '6. Last Refreshed'
            ]
            missing_cols = [c for c in expected_cols if c not in data.columns]
            if missing_cols:
                raise ValueError(f"Faltan columnas en alpha.csv: {missing_cols}. Columnas disponibles: {data.columns.tolist()}")

            # Formatear columnas usando operaciones de Pandas (vectorizado)
            symbol_col = data['1. From_Currency Code'].astype(str) + "/" + data['3. To_Currency Code'].astype(str)
            price_col = data["5. Exchange Rate"].astype(float)
            
            # Dar mismo formato para la fecha que Yahoo y Finhub
            timestamp_col = pd.to_datetime(data["6. Last Refreshed"], utc=True, errors='coerce')
            timestamp_col = timestamp_col.dt.strftime("%Y-%m-%d %H:%M:%S")

            # Crear el dataframe final 
            df_transformado = pd.DataFrame({
                "timestamp": timestamp_col,
                "symbol": symbol_col,
                "price": price_col
            })

            data = df_transformado[['timestamp','symbol','price']]
            data.to_csv(f"{Temp_path}/alpha_transformado.csv", index=False)
            os.remove(f"{Temp_path}/alpha.csv")
            print(f" Datos transformados correctamente\n{data}")
            return f"{Temp_path}/alpha_transformado.csv"
        except Exception as e:
            if os.path.exists(f"{Temp_path}/alpha.csv"):
                os.remove(f"{Temp_path}/alpha.csv")
            print(f" Error al transformar los datos: {e}")
            return False
    print(f" No existe alpha.csv o no se extrajo correctamente")
    return False

def Validar_gx(**kwargs):
    import great_expectations as gx
    
    # XCom Pull dinámico
    ti = kwargs['ti']
    target_task_id = kwargs.get('target_task_id')
    ruta_csv = ti.xcom_pull(task_ids=target_task_id)

    try:
        if not ruta_csv or not os.path.exists(ruta_csv):
            print(f" No se pudo leer {ruta_csv} generada por {target_task_id}.")
            return 'cuarentena'
            
        print(f" Iniciando validación con Great Expectations para: {ruta_csv}")
        
        # Leer a dataset pandas y envolver en dataset de GX
        df = pd.read_csv(ruta_csv)
        gx_df = gx.from_pandas(df)
        
        # 1. Ejecutar grupo de expectativas básicas
        
        # Comprobar obligatorias 
        gx_df.expect_column_values_to_not_be_null('symbol')
        gx_df.expect_column_values_to_not_be_null('timestamp')
        gx_df.expect_column_values_to_not_be_null('close')
        gx_df.expect_column_values_to_not_be_null('open')
        gx_df.expect_column_values_to_not_be_null('high')
        gx_df.expect_column_values_to_not_be_null('low')

        # Comprobar QUE NO HAYA VALORES NEGATIVOS EN LOS PRECIOS
        gx_df.expect_column_values_to_be_between('close', min_value=0, strict_min=True)
        gx_df.expect_column_values_to_be_between('open', min_value=0, strict_min=True)

        # Comrpobar que no haya duplicados en timestamp
        gx_df.expect_column_values_to_be_unique('timestamp')

        # 2. Ejecutar validación
        results = gx_df.validate()
        
        # 3. Evaluar e Informar resultados
        if results["success"]:
            print(f" Validacion Exitosa para {ruta_csv}")
            return 'Merge'
        else:
            print(f" Fallaron las validaciones de negocio en {ruta_csv}")
            return 'Cuarentena'

    except Exception as e:
        print(f" Error al validar los datos en {ruta_csv}: {e}")
        return 'cuarentena'

def verificar_db():
    try:
        import sqlite3
        if sqlite3.connect("db.db"):
            return 'Merge'
        else:
            return 'crear_db'
       
    except Exception as e:
        print(f" Error al verificar la base de datos: {e}")
        return False
    
def crear_db():
    try:
        import sqlite3
        conn = sqlite3.connect("db.db")
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS DATOS (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                symbol TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL
            )
        ''')
        conn.commit()
        conn.close()
        print("Base de datos creada correctamente")
        return 'Merge'
    except Exception as e:
        print(f" Error al crear la base de datos: {e}")
        return False

def Merge():
    return 'cargar_db'


with DAG(
    'ETL_Delivery2',
    default_args=default_args,
    description='ETL_Delivery2',
    schedule_interval=timedelta(minutes=2), # Changed schedule_interval for easier testing
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=['Delivery2'],
    ) as dag:     

        #Tarea de extraccion
        extraccion_task_yahoo = PythonOperator(
            task_id='extraccion_yahoo',
            python_callable=extraccion_yahoo,
        )

        extraccion_task_finhub = PythonOperator(
            task_id='extraccion_finhub',
            python_callable=extraccion_finhub,
        )

        extraccion_task_alpha = PythonOperator(
            task_id='extraccion_alpha',
            python_callable=extraccion_alpha,
        )
        #Tarea de transformacion
        
        transformacion_task_yahoo = PythonOperator(
            task_id='transformacion_yahoo',
            python_callable=transformacion_yahoo,
        )

        transformacion_task_finhub = PythonOperator(
            task_id='transformacion_finhub',
            python_callable=transformacion_finhub,
        )
        transformacion_task_alpha = PythonOperator(
            task_id='transformacion_alpha',
            python_callable=transformacion_alpha,
        )

        validar_task_yahoo = BranchPythonOperator(
            task_id='validar_yahoo',
            python_callable=Validar_gx,
            op_kwargs={'target_task_id': 'transformacion_yahoo'},
        )

        validar_task_finhub = BranchPythonOperator(
            task_id='validar_finhub',
            python_callable=Validar_gx,
            op_kwargs={'target_task_id': 'transformacion_finhub'},
        )

        validar_task_alpha = BranchPythonOperator(
            task_id='validar_alpha',
            python_callable=Validar_gx,
            op_kwargs={'target_task_id': 'transformacion_alpha'},
        )

        

        mover_cuarentena = BashOperator(
            task_id='cuarentena',
            bash_command='echo "[ ❌ ] Moviendo lote a zona de cuarentena..."',
        )

        verificar_db_task = BranchPythonOperator(
            task_id='verificar_db',
            python_callable=verificar_db,
        )
        crear_db_task = PythonOperator(
            task_id='crear_db',
            python_callable=crear_db,
        )
        
        Merge_task = BashOperator(
            task_id='Merge',
            bash_command='echo "[ ✅ ] merge con base de datos"',
        )
        validar_merge = BranchPythonOperator(
            task_id='validar_merge',
            python_callable=Validar_gx,
            op_kwargs={'target_task_id': 'Merge'},
        )

        cargar_db_task = BashOperator(
            task_id='cargar_db',
            bash_command='echo "[ 🚀 ] Cargando datos a la base de datos..."',
        )

verificar_db_task >> [crear_db_task, Merge_task]

extraccion_task_yahoo >> transformacion_task_yahoo >> validar_task_yahoo >> [Merge_task, mover_cuarentena]
extraccion_task_finhub >> transformacion_task_finhub >> validar_task_finhub >> [Merge_task, mover_cuarentena]
extraccion_task_alpha >> transformacion_task_alpha >> validar_task_alpha >> [Merge_task, mover_cuarentena]

Merge_task >> validar_merge >> [cargar_db_task, mover_cuarentena]


