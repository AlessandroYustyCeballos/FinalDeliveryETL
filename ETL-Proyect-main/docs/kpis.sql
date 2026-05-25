-- =====================================================================
-- KPIs para el dashboard analítico de Metabase
-- =====================================================================
-- Proyecto: ETL Forex - Entrega Final
-- Base de datos: trading_db (PostgreSQL)
--
-- IMPORTANTE: este archivo está en docs/, no en sql/. Postgres solo
-- auto-ejecuta los .sql que viven en sql/ (montados en
-- /docker-entrypoint-initdb.d/), así que este archivo es únicamente una
-- biblioteca de queries de referencia para usar en Metabase.
--
-- CONTEXTO DEL PROYECTO:
--   El usuario elige UNA SOLA divisa al inicializar (preparar.py).
--   Por lo tanto, fact_quotes contiene datos de UN ÚNICO par de
--   divisas pero alimentado por TRES fuentes (Yahoo / Finnhub / Alpha).
--   Los KPIs están diseñados para comparar:
--     - Entre fuentes (calidad y discrepancia)
--     - A lo largo del tiempo (día, hora, minuto)
--
-- Uso en Metabase:
--   1. Abrir Metabase (http://localhost:3000)
--   2. + New → SQL query → escoger "Trading DB"
--   3. Copiar el query deseado y pegarlo
--   4. Elegir el tipo de visualización sugerida y guardar
--   5. Agregar al dashboard
-- =====================================================================


-- ============================================================
-- SECCIÓN 1 - KPIs DE SALUD DEL PIPELINE (cards arriba)
-- ============================================================

-- ---------------------------------------------------------------------
-- KPI 1 - Total de registros consolidados
-- Visualización: Number
-- Insight: volumen total cargado por el pipeline al warehouse
-- ---------------------------------------------------------------------
SELECT COUNT(*) AS total_quotes
FROM fact_quotes;


-- ---------------------------------------------------------------------
-- KPI 2 - Fuentes activas alimentando el warehouse
-- Visualización: Number
-- Insight: confirma que las 3 fuentes (Yahoo / Finnhub / Alpha) están
--          contribuyendo al modelo dimensional
-- ---------------------------------------------------------------------
SELECT COUNT(DISTINCT source_id) AS fuentes_activas
FROM fact_quotes;


-- ---------------------------------------------------------------------
-- KPI 3 - Tasa de éxito de validación de calidad
-- Visualización: Number con sufijo "%"
-- Insight: si es < 90% hay problemas en alguna fuente
-- ---------------------------------------------------------------------
SELECT
    ROUND(
        100.0 * (SELECT COUNT(*) FROM fact_quotes)::numeric /
        NULLIF(
            (SELECT COUNT(*) FROM fact_quotes) +
            (SELECT COUNT(*) FROM quarantine_quotes),
            0
        ),
        2
    ) AS pct_exito_validacion;


-- ---------------------------------------------------------------------
-- KPI 4 - Registros enviados a cuarentena
-- Visualización: Number
-- Insight: cuántos lotes falló validación. Idealmente bajo.
-- ---------------------------------------------------------------------
SELECT COUNT(*) AS rechazados
FROM quarantine_quotes;


-- ---------------------------------------------------------------------
-- KPI 5 - Tiempo de la última carga
-- Visualización: Number con formato datetime
-- Insight: prueba que el DAG sigue activo y cargando datos
-- ---------------------------------------------------------------------
SELECT MAX(loaded_at) AS ultima_carga
FROM fact_quotes;



-- ============================================================
-- SECCIÓN 2 - KPIs DEL PAR DE DIVISAS (gráficos centrales)
-- Todos centrados en el par elegido por el usuario
-- ============================================================

-- ---------------------------------------------------------------------
-- KPI 6 - Evolución del precio de cierre por fuente
-- Visualización: Line chart
--   X: timestamp
--   Y: close
--   Group: source_name (una línea por fuente)
-- Insight: permite ver visualmente la consistencia entre Yahoo y Finnhub
--          para el mismo par. Si las líneas divergen es señal de
--          problemas en alguna fuente.
-- ---------------------------------------------------------------------
SELECT
    timestamp,
    source_name,
    close
FROM vw_quotes_flat
WHERE close IS NOT NULL
ORDER BY timestamp;


-- ---------------------------------------------------------------------
-- KPI 7 - Estadísticas descriptivas por fuente
-- Visualización: Table
-- Insight: comparativa rápida de las 3 fuentes para el mismo par.
--          ¿Cuál tiene más datos? ¿Cuál tiene precios más estables?
-- ---------------------------------------------------------------------
SELECT
    source_name,
    COUNT(*)                                                    AS registros,
    ROUND(AVG(COALESCE(close, price))::numeric, 5)              AS precio_promedio,
    ROUND(MIN(COALESCE(close, price))::numeric, 5)              AS precio_minimo,
    ROUND(MAX(COALESCE(close, price))::numeric, 5)              AS precio_maximo,
    ROUND(STDDEV(COALESCE(close, price))::numeric, 6)           AS desviacion_estandar
FROM vw_quotes_flat
WHERE COALESCE(close, price) IS NOT NULL
GROUP BY source_name
ORDER BY registros DESC;


-- ---------------------------------------------------------------------
-- KPI 8 - Discrepancia de precio entre Yahoo y Finnhub
-- Visualización: Line chart
--   X: timestamp (alineado al minuto)
--   Y: diferencia (Yahoo - Finnhub)
-- Insight: ¿qué tan parecidos son los precios reportados por las dos
--          fuentes? Una diferencia constante cerca de cero indica
--          buena consistencia. Picos indican desincronización.
-- ---------------------------------------------------------------------
SELECT
    date_trunc('minute', y.timestamp) AS minuto,
    AVG(y.close - f.close)            AS diferencia,
    ROUND((AVG(y.close - f.close) / AVG(y.close) * 100)::numeric, 4) AS diferencia_pct
FROM vw_quotes_flat y
JOIN vw_quotes_flat f
  ON date_trunc('minute', y.timestamp) = date_trunc('minute', f.timestamp)
WHERE y.source_name = 'Yahoo'
  AND f.source_name = 'Finnhub'
  AND y.close IS NOT NULL
  AND f.close IS NOT NULL
GROUP BY date_trunc('minute', y.timestamp)
ORDER BY minuto;



-- ============================================================
-- SECCIÓN 3 - KPIs TEMPORALES
-- Comportamiento del par a lo largo del tiempo
-- ============================================================

-- ---------------------------------------------------------------------
-- KPI 9 - Cobertura del pipeline por día y fuente
-- Visualización: Stacked bar chart
--   X: date_only
--   Y: registros
--   Group: source_name
-- Insight: demuestra que las 3 fuentes alimentan el warehouse
--          consistentemente. Detecta días con caídas.
-- ---------------------------------------------------------------------
SELECT
    date_only,
    source_name,
    COUNT(*) AS registros
FROM vw_quotes_flat
GROUP BY date_only, source_name
ORDER BY date_only DESC, source_name;


-- ---------------------------------------------------------------------
-- KPI 10 - Volatilidad intradiaria (rango high-low por hora)
-- Visualización: Bar chart
--   X: hour
--   Y: volatilidad_promedio
-- Insight: identifica las horas del día donde el par es más volátil
--          (típicamente apertura de Londres / Nueva York). Útil para
--          decisiones de trading.
-- ---------------------------------------------------------------------
SELECT
    hour,
    ROUND(AVG(high - low)::numeric, 6)              AS volatilidad_promedio,
    ROUND((AVG(high - low) / AVG(close) * 100)::numeric, 4) AS volatilidad_pct,
    COUNT(*) AS muestras
FROM vw_quotes_flat
WHERE high IS NOT NULL
  AND low  IS NOT NULL
  AND close > 0
GROUP BY hour
ORDER BY hour;


-- ---------------------------------------------------------------------
-- KPI 11 - Tendencia del precio durante la jornada
-- Visualización: Line chart con bandas
--   X: timestamp
--   Y: close, high, low (3 series)
-- Insight: vela aplanada que muestra la evolución completa del par
--          incluyendo el rango intra-minuto
-- ---------------------------------------------------------------------
SELECT
    timestamp,
    open,
    high,
    low,
    close
FROM vw_quotes_flat
WHERE source_name = 'Yahoo'
  AND close IS NOT NULL
ORDER BY timestamp;



-- ============================================================
-- SECCIÓN 4 - KPIs DE NEGOCIO / INSIGHTS
-- ============================================================

-- ---------------------------------------------------------------------
-- KPI 12 - Variación porcentual diaria del cierre
-- Visualización: Bar chart con colores condicionales (verde/rojo)
-- Insight: en qué días el par ganó vs perdió valor
-- ---------------------------------------------------------------------
WITH cierres_diarios AS (
    SELECT
        date_only,
        FIRST_VALUE(close) OVER (PARTITION BY date_only ORDER BY timestamp ASC ) AS apertura,
        FIRST_VALUE(close) OVER (PARTITION BY date_only ORDER BY timestamp DESC) AS cierre,
        ROW_NUMBER()       OVER (PARTITION BY date_only ORDER BY timestamp DESC) AS rn
    FROM vw_quotes_flat
    WHERE source_name = 'Yahoo' AND close IS NOT NULL
)
SELECT
    date_only,
    apertura,
    cierre,
    ROUND(((cierre - apertura) / apertura * 100)::numeric, 4) AS variacion_pct
FROM cierres_diarios
WHERE rn = 1
ORDER BY date_only DESC;


-- ---------------------------------------------------------------------
-- KPI 13 - Distribución de precios de cierre (histograma)
-- Visualización: Histogram
--   X: close (bin automático)
--   Y: frecuencia
-- Insight: en qué rango de precios pasó la mayor parte del tiempo
-- ---------------------------------------------------------------------
SELECT close
FROM vw_quotes_flat
WHERE source_name = 'Yahoo'
  AND close IS NOT NULL;



-- ============================================================
-- ANEXO - QUERIES AUXILIARES (no son KPIs pero útiles)
-- ============================================================

-- ---------------------------------------------------------------------
-- AUX 1 - Últimos 100 ticks de todas las fuentes
-- Útil para tabla "live feed" en el dashboard
-- ---------------------------------------------------------------------
SELECT
    timestamp,
    source_name,
    COALESCE(close, price) AS metric,
    open, high, low
FROM vw_quotes_flat
ORDER BY timestamp DESC
LIMIT 100;


-- ---------------------------------------------------------------------
-- AUX 2 - Resumen ejecutivo (una sola fila con todo lo importante)
-- Útil para card "summary" arriba del dashboard
-- ---------------------------------------------------------------------
SELECT
    (SELECT symbol FROM dim_symbol LIMIT 1)                  AS par_analizado,
    (SELECT COUNT(*) FROM fact_quotes)                       AS total_registros,
    (SELECT COUNT(DISTINCT source_id) FROM fact_quotes)      AS fuentes_activas,
    (SELECT MIN(ts) FROM dim_time
       WHERE time_id IN (SELECT time_id FROM fact_quotes))   AS desde,
    (SELECT MAX(ts) FROM dim_time
       WHERE time_id IN (SELECT time_id FROM fact_quotes))   AS hasta,
    (SELECT MAX(loaded_at) FROM fact_quotes)                 AS ultima_carga,
    (SELECT COUNT(*) FROM quarantine_quotes)                 AS lotes_cuarentena;


-- ---------------------------------------------------------------------
-- AUX 3 - Cantidad de ticks recibidos en las últimas 24h
-- Útil como métrica de actividad reciente
-- ---------------------------------------------------------------------
SELECT
    source_name,
    COUNT(*) AS ticks_ultimas_24h
FROM vw_quotes_flat
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY source_name
ORDER BY ticks_ultimas_24h DESC;
