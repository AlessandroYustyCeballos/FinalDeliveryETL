-- =====================================================================
-- ESQUEMA DIMENSIONAL - TRADING DB
-- Pipeline ETL Forex: Yahoo / Finnhub / Alpha Vantage -> Postgres
-- =====================================================================
-- Se ejecuta automáticamente la PRIMERA vez que el contenedor Postgres
-- arranca (porque está montado en /docker-entrypoint-initdb.d/).
-- Para reejecutar: docker compose down -v && docker compose up -d
-- =====================================================================

-- ---------------------------------------------------------------------
-- DIMENSIÓN: SYMBOL
-- Catálogo de pares de divisas (EUR/USD, GBP/USD, etc.)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_id       SERIAL PRIMARY KEY,
    symbol          VARCHAR(20) UNIQUE NOT NULL,   -- e.g. 'EURUSD'
    base_currency   VARCHAR(10) NOT NULL,          -- e.g. 'EUR'
    quote_currency  VARCHAR(10) NOT NULL,          -- e.g. 'USD'
    description     TEXT
);

-- ---------------------------------------------------------------------
-- DIMENSIÓN: SOURCE
-- Origen del dato (importante para trazabilidad)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_source (
    source_id       SERIAL PRIMARY KEY,
    source_name     VARCHAR(50) UNIQUE NOT NULL,   -- 'Yahoo' | 'Finnhub' | 'Alpha'
    source_type     VARCHAR(20) NOT NULL,          -- 'batch' | 'stream' | 'api'
    description     TEXT
);

-- ---------------------------------------------------------------------
-- DIMENSIÓN: TIME
-- Tabla de tiempo desnormalizada para facilitar agregaciones en BI
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_time (
    time_id         BIGSERIAL PRIMARY KEY,
    ts              TIMESTAMP UNIQUE NOT NULL,     -- timestamp completo
    year            INT NOT NULL,
    month           INT NOT NULL,
    day             INT NOT NULL,
    hour            INT NOT NULL,
    minute          INT NOT NULL,
    day_of_week     INT NOT NULL,                  -- 0=Lunes, 6=Domingo
    date_only       DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_time_date ON dim_time(date_only);

-- ---------------------------------------------------------------------
-- HECHOS: FACT_QUOTES
-- Métricas OHLC + price unificadas. Una fila por (timestamp, symbol, source).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_quotes (
    quote_id        BIGSERIAL PRIMARY KEY,
    time_id         BIGINT NOT NULL REFERENCES dim_time(time_id),
    symbol_id       INT    NOT NULL REFERENCES dim_symbol(symbol_id),
    source_id       INT    NOT NULL REFERENCES dim_source(source_id),
    open            NUMERIC(18, 8),
    high            NUMERIC(18, 8),
    low             NUMERIC(18, 8),
    close           NUMERIC(18, 8),
    price           NUMERIC(18, 8),                -- usado por Alpha (tick simple)
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (time_id, symbol_id, source_id)         -- idempotencia
);

CREATE INDEX IF NOT EXISTS idx_fact_time   ON fact_quotes(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_symbol ON fact_quotes(symbol_id);
CREATE INDEX IF NOT EXISTS idx_fact_source ON fact_quotes(source_id);

-- ---------------------------------------------------------------------
-- CUARENTENA: registros que NO pasaron la validación de calidad
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quarantine_quotes (
    quarantine_id   BIGSERIAL PRIMARY KEY,
    source_name     VARCHAR(50),
    symbol          VARCHAR(20),
    timestamp_raw   TEXT,
    payload         JSONB,                         -- fila original completa
    reason          TEXT,                          -- por qué falló
    quarantined_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------
-- SEED de dim_source (las 3 fuentes ya las conocemos)
-- ---------------------------------------------------------------------
INSERT INTO dim_source (source_name, source_type, description) VALUES
    ('Yahoo',   'batch',  'Yahoo Finance vía yfinance, datos OHLC históricos'),
    ('Finnhub', 'stream', 'Finnhub WebSocket, trades en tiempo real agregados a OHLC por minuto'),
    ('Alpha',   'api',    'Alpha Vantage CURRENCY_EXCHANGE_RATE, tick puntual')
ON CONFLICT (source_name) DO NOTHING;

-- ---------------------------------------------------------------------
-- VISTA convenience para Power BI / Streamlit:
-- fact + dims aplanado, listo para arrastrar a un dashboard.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_quotes_flat AS
SELECT
    f.quote_id,
    t.ts                AS timestamp,
    t.date_only,
    t.hour,
    t.minute,
    s.symbol,
    s.base_currency,
    s.quote_currency,
    src.source_name,
    src.source_type,
    f.open, f.high, f.low, f.close, f.price,
    f.loaded_at
FROM fact_quotes f
JOIN dim_time   t   ON f.time_id   = t.time_id
JOIN dim_symbol s   ON f.symbol_id = s.symbol_id
JOIN dim_source src ON f.source_id = src.source_id;
