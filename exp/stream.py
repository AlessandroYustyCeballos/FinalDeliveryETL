import websocket
import json
import pandas as pd
import os

PATH = './data'
symbols = pd.read_csv(f'{PATH}/log.csv', header=None) 
symbols = "OANDA:" + symbols[0]
SYMBOLS = symbols.tolist()
API_KEY = "d669i81r01qots73p5fgd669i81r01qots73p5g0"

def Finhub():
    def on_message(ws, message):
        try:
            data = json.loads(message)
            # VALIDACIÓN CLAVE: Solo procesar si el tipo es 'trade' y contiene la llave 'data'
            if data.get('type') == 'trade' and 'data' in data:
                try:
                    if not os.path.exists(os.path.dirname(f'{PATH}/stream.csv')):
                        os.makedirs(os.path.dirname(f'{PATH}/stream.csv'))
                    df_new = pd.DataFrame(data['data'])

                    # Guardado en modo Append
                    archivo_existe = os.path.isfile(f'{PATH}/stream.csv')
                    df_new.to_csv(f'{PATH}/stream.csv', mode='a', index=False, header=not archivo_existe)

                    print(f"[OK] Guardado finhub.")

                except Exception as e:
                    print(f"Error al guardar los datos finhub: {e}")
            elif data.get('type') == 'ping':
                # Ignoramos los pings silenciosamente para no ensuciar la consola
                pass
            else:
                # Otros mensajes del sistema (como confirmación de suscripción)
                print(f"[INFO] Mensaje del sistema: {data}")

        except Exception as e:
            print(f"[WARN] Error procesando mensaje: {e}")

    def on_open(ws):
        for symbol in SYMBOLS:
            msg = {"type": "subscribe", "symbol": symbol}
            ws.send(json.dumps(msg))
            print(f"[OPEN] Suscrito a: {symbol}")

        print(f"[OPEN] Conexion iniciada. Guardando..")

    def on_error(ws, error):
        print(f"[ERROR] Error de WebSocket: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("### Conexion cerrada ###")

    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={API_KEY}",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

Finhub()
