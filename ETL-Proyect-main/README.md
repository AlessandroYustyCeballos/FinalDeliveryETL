<div align="center">
  <h1>Proyecto ETL de Fuentes de Trading Financiero</h1>
  <p>Pipeline orquestado por Apache Airflow para extracción, validación y consolidación de datos financieros de fuentes múltiples.</p>
</div>

---

# Ejecucion del proyecto
Abra el main en codespaces, espera hasta que se carguen todos los recursos... automaticamente el contenedor realizará la instalacion de los requerimientos, e inicializacion del entorno Airflow con credenciales admin por defecto. Una vez cargado todo, dirijase a la terminal y ejecute el siguiente comando:

```bash
python -m airflow standalone
```
Una ves inicializado el UI de Airflow, dirijase a la pestaña puertos dentro de la misma terminal y de click derecho sobre el puerto 8080 o UI Airflow y modifique la visibilidad del puerto a public, de esta manera podrá acceder al UI de Airflow desde el logo del mundo que muestra el puerto.

Una ves dentro de Airflow, dirijase a la pestaña DAGs y busque el DAG llamado `ETL_Delivery2`, deberia poder observarse las tareas reconocidas en el DAG. Antes de inicial el DAG lo primordial es inicializar la data por lotes y streaming, para ello dirijase a la pestaña terminal y ejecute el siguiente comando:

```bash
python preparar.py
```
ingrese por consola la opcion 1 para inicializar seguido de la divisa a conultar. Una ves finalizado el proceso, dirijase a la pestaña DAGs y active el DAG `ETL_Delivery2` desde el interruptor izquierdo.

## Descripción General
Este proyecto implementa una canalización completa de **ETL (Extracción, Transformación, Carga/Unificación)** construida bajo el motor de Airflow. Se encarga de conectarse recurrentemente a 3 diferentes proveedores de APIs financieras, recolectar lotes de datos, transformarlos a una estructura temporal idéntica, pasarlos por un estricto validador de Calidad de Datos (Data Quality) con **Great Expectations**, y finalmente consolidar en una base local todos aquellos lotes que cumplan la normativa.

## Estructura del Orquestador (DAG)
El DAG central se llama `ETL_Delivery2` (localizado en `dags/ETL.py`) y se ejecuta rítmicamente. Está compuesto de las siguientes tres macro-fases de negocio:

### 1. Extraccion
Tres tareas paralelas (`extraccion_task_yahoo`, `extraccion_task_finhub`, y `extraccion_task_alpha`) ejecutan consultas a sus respectivas APIs usando llamadas a la web (HTTP Requests) o simulacros de recarga. Empaquetan el valor extraído en caché temporal.
* Yahoo Finance: Requiere conexión de historial.
* Finnhub: Optimizado para WebSockets (extrae valores cortos como `p`, `v`, `t`).
* Alpha Vantage: Extracción JSON (Plan gratuito = max 25/día).

### 2. Transformación
Las tareas leen los CSV residuales de la fase extractiva usando métodos vectorizados de `Pandas`:
- Estandarización de nombres de columnas a minúsculas (`open`, `high`, `low`, `close`, `price`).
- Formateo inteligente Universal de Timestamps a texto ISO (`%Y-%m-%d %H:%M:%S`).
- Aplanamiento de columnas múltiples (solución al problema de MultiIndex de yfinance).

### 3. Aseguramiento Validador y Bifurcacion (Branching)
Se emplea un motor dinámico usando un `BranchPythonOperator` para las tareas `validar_task_*`. Por medio del framework Great Expectations, analiza las políticas de los negocios:
* Estructura Cero-Nulos: No permite casillas vacías.
* Precios Reales: Comprueba rigurosamente que los precios (OHLC o simples) jamás sean valores negativos (`min_value=0`).
> Condicion Excluyente: Si los datos no pasan este rubro por cualquier motivo (ej. Alpha careciendo de estructuras de High/Low por ser de flujo variable interbancario), Airflow cortará instantáneamente esa línea de flujo.

### 4. Rutas Finales 
Si el validador dio *"Éxito"*, las métricas pasan por XCom su ruta hacia la tarea final: **`cargar_db_task`**. 
Esta consolida todos sus precursores en un Dataframe Maestro único (`data/BD_temp.csv`) organizado crónologicamente inverso, ignorando sin romperse las ramas previas que cayeron en desgracia.

Si las validaciones fallaron, la métrica es arrojada hacia **`cuarentena`** (Un colector terminal secundario para rechazar lotes dañados o inestables sin perjudicar las otras bases).

## Requisitos de Ejecucion
Asegúrate de contar con tus dependencias, puedes instalarlas usando:
```bash
pip install -r requirements.txt
```
* **Bibliotecas Base**: `pandas`, `requests`
* **Módulos Financieros**: `yfinance`, `finnhub-python`, `websocket-client`
* **Orquestador y QA**: `apache-airflow`, `great_expectations`

## Notas de API
Si integras un contenedor o corres de manera local, considera proveer credenciales vivas dentro del archivo secreto `.env`. La API de AlphaVantage cuenta con restricciones agresivas de Rate Limit, si observas demasiados lotes Alpha dirigiéndose masivamente hacia la cuarentena, verifica la tolerancia diaria de tu cuota.
