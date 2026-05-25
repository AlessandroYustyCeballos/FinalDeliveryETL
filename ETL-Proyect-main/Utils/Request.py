import websocket
import json
import Save
import yfinance as yf
import requests



def Leer_json():
     with open("Utils/Pares.json", "r", encoding="utf-8") as f:
        symbols = json.load(f)

        return symbols


def Yahoo(symbol, period, interval):
    print(f" Datos recibidos: symbol: {symbol}, period: {period}, interval: {interval}")
    data = yf.download(symbol, period=period, interval=interval)
    Save.Guarda("Yahoo", data)




def Finhub(simbol):
    API_KEY = "d669i81r01qots73p5fgd669i81r01qots73p5g0"
    s_parsed = simbol.replace("=X", "")
    if len(s_parsed) == 6:
        s_parsed = f"{s_parsed[:3]}_{s_parsed[3:]}"

    SYMBOLS = [
        f"OANDA:{s_parsed}"
    ]
    print(f"Symbolo recibido en finhub: {SYMBOLS}")

    def on_message(ws, message):
        try:
            data = json.loads(message)
            # VALIDACIÓN CLAVE: Solo procesar si el tipo es 'trade' y contiene la llave 'data'
            if data.get('type') == 'trade' and 'data' in data:
                Save.Guarda("Finhub", data)
            elif data.get('type') == 'ping':
                # Ignoramos los pings silenciosamente para no ensuciar la consola
                pass
            else:
                # Otros mensajes del sistema (como confirmación de suscripción)
                print(f"📢 Mensaje del sistema: {data}")

        except Exception as e:
            print(f"⚠️ Error procesando mensaje: {e}")

    def on_open(ws):
        for symbol in SYMBOLS:
            msg = {"type": "subscribe", "symbol": symbol}
            ws.send(json.dumps(msg))
            print(f"📡 Suscrito a: {symbol}")

        print(f"📡 Conexión iniciada. Guardando..")

    def on_error(ws, error):
        print(f"❌ Error de WebSocket: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("### Conexión cerrada ###")

    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={API_KEY}",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()