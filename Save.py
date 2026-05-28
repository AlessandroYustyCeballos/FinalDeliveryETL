

import pandas as pd
import os



# 2. Configuración de rutas


def Guarda(Fuente, data):
    FOLDER_PATH = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    CSV_FILE_YAHOO = f"{FOLDER_PATH}/data/yahoo.csv"
    CSV_FILE_FINHUB = f"{FOLDER_PATH}/data/finhub.csv"
    CSV_FILE_ALPHA = f"{FOLDER_PATH}/data/alpha.csv"

    if Fuente == "Yahoo":

        try:
            if not os.path.exists(os.path.dirname(CSV_FILE_YAHOO)):
                os.makedirs(os.path.dirname(CSV_FILE_YAHOO))
            
            df_new = pd.DataFrame(data)
            
            # Formatear el Dataframe de Yahoo para conservar la fecha y aplanar el MultiIndex
            df_new.reset_index(inplace=True)
            if isinstance(df_new.columns, pd.MultiIndex):
                new_cols = []
                for col in df_new.columns:
                    if col[1]:
                        new_cols.append(f"{col[0]}_{col[1]}")
                    else:
                        new_cols.append(col[0])
                df_new.columns = new_cols

            print("Ejecutando Guardado yahoo...")

            # Guardado en modo Append
            archivo_existe = os.path.isfile(CSV_FILE_YAHOO)
            df_new.to_csv(CSV_FILE_YAHOO, mode='a', index=False, header=not archivo_existe)

            print("Datos de Yahoo Guardados...")

        except Exception as e:
            print(f"Error al guardar los datos Yahoo: {e}")

    elif Fuente == "Finhub":
        try:
            if not os.path.exists(os.path.dirname(CSV_FILE_FINHUB)):
                os.makedirs(os.path.dirname(CSV_FILE_FINHUB))
            df_new = pd.DataFrame(data['data'])

            # Guardado en modo Append
            archivo_existe = os.path.isfile(CSV_FILE_FINHUB)
            df_new.to_csv(CSV_FILE_FINHUB, mode='a', index=False, header=not archivo_existe)

            print(f"✅ Guardado finhub.")

        except Exception as e:
            print(f"Error al guardar los datos finhub: {e}")

    elif Fuente == "Alpha":
        try:
            if not os.path.exists(os.path.dirname(CSV_FILE_ALPHA)):
                os.makedirs(os.path.dirname(CSV_FILE_ALPHA))
            info = data['Realtime Currency Exchange Rate']
            data_new = pd.DataFrame(info)

            # Guardado en modo Append
            archivo_existe = os.path.isfile(CSV_FILE_ALPHA)
            data_new.to_csv(CSV_FILE_ALPHA, mode='a', index=False, header=not archivo_existe)

            print(f"✅ Guardado alpha.")

        except Exception as e:
            print(f"Error al guardar los datos alpha: {e}")

