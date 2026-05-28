from Utils.Request import Leer_json, Yahoo, Finhub
import pandas as pd




opcion= input("Inicializar? \n 1 = Si \n 0 = No\n:")

if opcion   == 0:
    exit
else:
    symbol = input(f"Escoge una divisa:\n{pd.DataFrame(Leer_json()).head()}\n: ")
    symbol=Leer_json()[int(symbol)]

    #Inciando Tareas 

    #Extraccion

    #Yahoo
    print("Inciacizando Yahoo...")
    Yahoo(symbol, '2d', '1m')
    
    #Finhub
    print("Inciacizando Finhub...")
    Finhub(symbol)
