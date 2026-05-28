
-- Metabase
-- (http://localhost:3000)


-- ---------------------------------------------------------------------
-- Total de registros consolidados en el data warehouse

SELECT COUNT(*) AS total_quotes
FROM fact_quotes;

-- ---------------------------------------------------------------------
-- Fuentes activas alimentando el warehouse

SELECT COUNT(DISTINCT source_id) AS fuentes_activas
FROM fact_quotes;

-- ---------------------------------------------------------------------
-- Tasa de éxito de validación de calidad

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

SELECT COUNT(*) AS rechazados
FROM quarantine_quotes;


-- ---------------------------------------------------------------------
-- Estadísticas descriptivas por fuente

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
-- Línea de precio  3 fuentes

SELECT
    timestamp,
    source_name,
    COALESCE(close, price) AS precio
FROM vw_quotes_flat
WHERE COALESCE(close, price) IS NOT NULL
ORDER BY timestamp;

-- ---------------------------------------------------------------------
-- Diferencia Diaria

SELECT
    date_only,
    source_name,
    ROUND(AVG(COALESCE(close, price))::numeric, 5) AS precio_promedio
FROM vw_quotes_flat
WHERE source_name IN ('Alpha', 'Yahoo')
  AND COALESCE(close, price) IS NOT NULL
GROUP BY date_only, source_name
ORDER BY date_only, source_name;

-- ---------------------------------------------------------------------
-- Cobertura del pipeline por día y fuente

SSELECT
    date_only,
    source_name,
    COUNT(*) AS registros
FROM vw_quotes_flat
GROUP BY date_only, source_name
ORDER BY date_only DESC, source_name;



-- ---------------------------------------------------------------------
-- Crecimiento del warehouse

SELECT
    date_trunc('hour', loaded_at) AS hora,
    COUNT(*)                       AS registros_cargados,
    SUM(COUNT(*)) OVER (ORDER BY date_trunc('hour', loaded_at)) AS acumulado
FROM fact_quotes
GROUP BY date_trunc('hour', loaded_at)
ORDER BY hora;

-- ---------------------------------------------------------------------
-- Tendencia del precio durante la jornada

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


-- ---------------------------------------------------------------------
-- Discrepancia de precio entre Yahoo y Finnhub

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


-- ---------------------------------------------------------------------
-- Finnhub y Alpha


SELECT
    timestamp,
    source_name,
    COALESCE(close, price) AS precio
FROM vw_quotes_flat
WHERE COALESCE(close, price) IS NOT NULL
  AND DATE(timestamp) = '2026-05-28'
ORDER BY timestamp;
