// Genera el informe técnico en formato Word (.docx)
// Uso: node docs/generate_docx.js

const fs = require('fs');
const path = require('path');
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, HeadingLevel, LevelFormat, BorderStyle, WidthType, ShadingType,
  PageBreak, PageOrientation, TabStopType, TabStopPosition,
  TableOfContents,
} = require('docx');

// ---------- Utilidades ----------
const FONT = 'Calibri';
const border = { style: BorderStyle.SINGLE, size: 4, color: 'CCCCCC' };
const borders = { top: border, bottom: border, left: border, right: border };

const p = (text, opts = {}) => new Paragraph({
  spacing: { after: 120 },
  alignment: opts.align || AlignmentType.JUSTIFIED,
  children: [new TextRun({ text, font: FONT, size: opts.size || 22, bold: opts.bold, italics: opts.italics, color: opts.color })],
});

const h1 = (text) => new Paragraph({
  heading: HeadingLevel.HEADING_1,
  spacing: { before: 360, after: 200 },
  children: [new TextRun({ text, font: FONT, size: 32, bold: true, color: '1F3864' })],
});

const h2 = (text) => new Paragraph({
  heading: HeadingLevel.HEADING_2,
  spacing: { before: 240, after: 160 },
  children: [new TextRun({ text, font: FONT, size: 26, bold: true, color: '2E74B5' })],
});

const h3 = (text) => new Paragraph({
  heading: HeadingLevel.HEADING_3,
  spacing: { before: 200, after: 120 },
  children: [new TextRun({ text, font: FONT, size: 24, bold: true, color: '2E74B5' })],
});

const bullet = (text) => new Paragraph({
  numbering: { reference: 'bullets', level: 0 },
  spacing: { after: 80 },
  children: [new TextRun({ text, font: FONT, size: 22 })],
});

const code = (text) => new Paragraph({
  spacing: { after: 120 },
  shading: { fill: 'F2F2F2', type: ShadingType.CLEAR, color: 'auto' },
  children: [new TextRun({ text, font: 'Consolas', size: 20 })],
});

const cell = (text, opts = {}) => new TableCell({
  borders,
  width: { size: opts.width, type: WidthType.DXA },
  shading: opts.header ? { fill: '1F3864', type: ShadingType.CLEAR, color: 'auto' } : undefined,
  margins: { top: 100, bottom: 100, left: 140, right: 140 },
  children: [new Paragraph({
    children: [new TextRun({
      text,
      font: FONT,
      size: opts.header ? 22 : 22,
      bold: opts.header || opts.bold,
      color: opts.header ? 'FFFFFF' : (opts.color || '000000'),
    })],
  })],
});

const table = (rows, colWidths) => {
  const tableWidth = colWidths.reduce((a, b) => a + b, 0);
  return new Table({
    width: { size: tableWidth, type: WidthType.DXA },
    columnWidths: colWidths,
    rows: rows.map((row, idx) => new TableRow({
      tableHeader: idx === 0,
      children: row.map((cellText, colIdx) => cell(cellText, { width: colWidths[colIdx], header: idx === 0 })),
    })),
  });
};

const separator = () => new Paragraph({
  border: { bottom: { style: BorderStyle.SINGLE, size: 8, color: '2E74B5', space: 1 } },
  spacing: { before: 120, after: 240 },
  children: [new TextRun('')],
});

const pageBreak = () => new Paragraph({ children: [new PageBreak()] });

// ---------- Contenido ----------
const content = [
  // ============ PORTADA ============
  new Paragraph({ spacing: { before: 2400 }, alignment: AlignmentType.CENTER,
    children: [new TextRun({ text: 'INFORME TÉCNICO', font: FONT, size: 48, bold: true, color: '1F3864' })] }),
  new Paragraph({ spacing: { before: 240 }, alignment: AlignmentType.CENTER,
    children: [new TextRun({ text: 'Proyecto ETL Forex — Entrega Final', font: FONT, size: 36, bold: true, color: '2E74B5' })] }),
  new Paragraph({ spacing: { before: 480 }, alignment: AlignmentType.CENTER,
    children: [new TextRun({
      text: 'Pipeline ETL end-to-end con orquestación Airflow, modelo dimensional en PostgreSQL, streaming Kafka y dashboards en Metabase y Streamlit.',
      font: FONT, size: 22, italics: true, color: '595959',
    })] }),

  new Paragraph({ spacing: { before: 1600 }, alignment: AlignmentType.CENTER, children: [new TextRun('')] }),
  table([
    ['Integrantes', 'Bryan Andres Herrera Betancur (2244008)\nAlessandro Yusty Ceballos (2240248)'],
    ['Curso', 'ETL (G51)'],
    ['Docente', 'Daniel Felipe Romero Bernal'],
    ['Programa', 'Ingeniería de Datos e Inteligencia Artificial'],
    ['Universidad', 'Universidad Autónoma de Occidente'],
    ['Fecha', 'Mayo 2026'],
    ['Repositorio', 'https://github.com/AlessandroYustyCeballos/FinalDeliveryETL'],
  ], [3000, 6000]),

  pageBreak(),

  // ============ 1. RESUMEN EJECUTIVO ============
  h1('1. Resumen ejecutivo'),
  p('Este proyecto implementa un pipeline ETL completo, reproducible y orquestado, que consume datos del mercado forex desde tres fuentes complementarias (Yahoo Finance, Finnhub vía WebSocket y Alpha Vantage), los valida con Great Expectations y los consolida en un modelo dimensional sobre PostgreSQL. Los datos se exponen mediante dos dashboards complementarios: uno analítico construido sobre Metabase y otro operativo en tiempo real construido sobre Streamlit y Plotly, conectado al stream de Kafka.'),
  p('Toda la infraestructura se levanta con un único comando (docker compose up -d), incluyendo diez servicios: PostgreSQL del proyecto, PostgreSQL del metastore de Airflow, Zookeeper, Kafka, Airflow scheduler y webserver, Metabase, Finnhub WebSocket persistente, Kafka producer y dashboard Streamlit.'),

  h3('Objetivos cumplidos'),
  bullet('Extracción real-time desde 3 fuentes (Yahoo, Finnhub WebSocket, Alpha Vantage)'),
  bullet('Transformación a esquema unificado (timestamp, symbol, OHLC, source)'),
  bullet('Validación de calidad con Great Expectations 1.x+ y branching a cuarentena'),
  bullet('Carga REAL al modelo dimensional (sin tareas fantasma con BashOperator)'),
  bullet('Kafka producer simulando CDC sobre la fact table'),
  bullet('Kafka consumer + Streamlit con dashboard en tiempo real'),
  bullet('Dashboard analítico en Metabase sobre la BD consolidada'),
  bullet('Dos notebooks EDA (dataset batch y API stream)'),
  bullet('Documento técnico, README y .gitignore'),

  pageBreak(),

  // ============ 2. CONTEXTO Y OBJETIVO ============
  h1('2. Contexto y objetivo del negocio'),
  p('El proyecto consume datos del mercado forex (mercado cambiario internacional) como insumo para análisis y modelos predictivos futuros. La motivación se alinea con el ODS 1 (Fin de la pobreza) de la Agenda 2030: aplicaciones de remesas internacionales, microcrédito en divisas y herramientas de hedging para poblaciones vulnerables podrían beneficiarse de modelos predictivos confiables sobre las variaciones cambiarias.'),
  p('El objetivo técnico principal es diseñar e implementar un pipeline ETL reproducible que combine ingesta batch e ingesta streaming, garantice calidad de los datos mediante validaciones declarativas, los consolide en un modelo dimensional analítico y los exponga a usuarios de negocio mediante dashboards complementarios.'),

  // ============ 3. ARQUITECTURA ============
  h1('3. Arquitectura del sistema'),
  p('La arquitectura sigue un patrón Lambda simplificado donde un único modelo dimensional alimenta tanto el plano analítico (Metabase) como el plano operativo (Kafka + Streamlit). Esto elimina la "verdad doble" típica de arquitecturas Lambda mal diseñadas, ya que ambos dashboards consumen del mismo data warehouse validado.'),

  h3('3.1. Stack tecnológico'),
  table([
    ['Capa', 'Tecnología', 'Versión'],
    ['Orquestación', 'Apache Airflow', '2.9.2'],
    ['Data Warehouse', 'PostgreSQL', '15'],
    ['Streaming', 'Apache Kafka (Confluent)', '7.6.1'],
    ['Validación de calidad', 'Great Expectations', '1.x+'],
    ['BI analítico', 'Metabase', 'latest'],
    ['Real-time dashboard', 'Streamlit + Plotly', 'latest'],
    ['Contenerización', 'Docker Compose', 'v2'],
    ['Lenguaje principal', 'Python', '3.11'],
  ], [3000, 4000, 2000]),

  h3('3.2. Servicios del docker-compose'),
  p('El stack completo está definido en docker-compose.yml con los siguientes 10 servicios:'),
  bullet('postgres — Data warehouse del proyecto (puerto 5433 expuesto)'),
  bullet('airflow-db — PostgreSQL interno para el metastore de Airflow'),
  bullet('zookeeper — Coordinador para Kafka'),
  bullet('kafka — Broker de streaming (puerto 9092 expuesto)'),
  bullet('airflow-init — Inicialización idempotente del scheduler'),
  bullet('airflow-scheduler — Ejecutor de tareas del DAG'),
  bullet('airflow-webserver — UI de Airflow (puerto 8080)'),
  bullet('metabase — Dashboard analítico (puerto 3000)'),
  bullet('finnhub-stream — WebSocket persistente de Finnhub'),
  bullet('producer — Kafka producer leyendo de fact_quotes'),
  bullet('streamlit — Dashboard real-time (puerto 8501)'),

  pageBreak(),

  // ============ 4. DATA INGESTION (20%) ============
  h1('4. Data Ingestion'),
  p('El sistema ingiere datos de tres fuentes complementarias para demostrar capacidad de manejar múltiples patrones de extracción: batch programado, streaming continuo y API REST con rate limit.'),

  h3('4.1. Fuentes implementadas'),
  table([
    ['Fuente', 'Tipo', 'Frecuencia', 'Estructura'],
    ['Yahoo Finance', 'Batch (yfinance HTTP)', 'Cada 2 minutos vía DAG', 'OHLC con 1-min intervals'],
    ['Finnhub', 'Stream (WebSocket)', 'Continuo 24/7', 'JSON {p, s, t, v} por trade'],
    ['Alpha Vantage', 'API REST', 'Cada 2 minutos vía DAG', 'JSON con currency exchange rate'],
  ], [2500, 2500, 2500, 1500]),

  h3('4.2. Yahoo Finance (extracción batch)'),
  p('La tarea extraccion_yahoo del DAG llama directamente a yfinance.download() en cada corrida. Descarga los últimos 2 días con resolución de 1 minuto, aplana el MultiIndex que devuelve yfinance y persiste como CSV temporal para la fase de transformación. Si la llamada falla (sin internet, símbolo inválido), cae al CSV bootstrap creado por preparar.py como mecanismo de respaldo.'),

  h3('4.3. Finnhub (WebSocket persistente)'),
  p('Se implementó un servicio dedicado finnhub-stream en el docker-compose que mantiene una conexión WebSocket persistente a wss://ws.finnhub.io. Cada trade recibido se valida (solo type=trade), se desestructura del payload corto {p, s, t, v} y se appendea a data/finhub.csv. El servicio tiene restart: unless-stopped para reconectarse automáticamente ante caídas.'),
  p('Esta separación entre el WebSocket persistente y el DAG es clave: el WebSocket acumula ticks continuamente, mientras que el DAG cada 2 minutos consume el CSV acumulado, lo transforma a OHLC y lo carga. Esto desacopla la velocidad del stream de la velocidad de procesamiento del warehouse.'),

  h3('4.4. Alpha Vantage (API REST)'),
  p('La tarea extraccion_alpha hace un requests.get() al endpoint CURRENCY_EXCHANGE_RATE en cada corrida del DAG. La respuesta es un JSON con un único exchange rate. El handling de errores está cuidado: si Alpha responde con "Information" o "Note" (típico de rate limit), la tarea lanza AirflowSkipException, lo que marca la tarea como skipped (no fallida) y el resto del DAG continúa.'),
  p('Cabe destacar que el tier gratuito de Alpha Vantage permite 25 requests por día, por lo que en operación real conviene ajustar el schedule del DAG o cachear los valores. Es un trade-off conocido y documentado en las decisiones técnicas.'),

  pageBreak(),

  // ============ 5. DATA TRANSFORMATION & MODELING (20%) ============
  h1('5. Data Transformation y modelo dimensional'),

  h3('5.1. Transformación al esquema unificado'),
  p('Las tres fuentes tienen esquemas heterogéneos. La fase de transformación normaliza todas al mismo esquema canónico: (timestamp, symbol, open, high, low, close, price, source). Esto permite que las tres se inserten en la misma tabla fact_quotes y se consulten conjuntamente desde Metabase.'),
  bullet('Yahoo: aplanado del MultiIndex (Close_EURUSD=X → close), normalización de timestamp a UTC, extracción del símbolo desde el nombre de columna.'),
  bullet('Finnhub: renombrado de llaves cortas (p→price, s→symbol, t→timestamp, v→volume), conversión de Unix-ms a datetime UTC y agregación OHLC por minuto usando groupby + agg(first/max/min/last).'),
  bullet('Alpha: extracción de la respuesta anidada, construcción del símbolo concatenando From + To currencies, normalización del timestamp.'),

  p('Decisión clave: la agregación a 1 minuto en Finnhub es deliberada para garantizar schema-compatibility con Yahoo (que ya viene pre-agregado). Esto permite que ambas fuentes alimenten la misma fact_quotes con datos comparables. La justificación cuantitativa de esta decisión está en el notebook eda_api.ipynb.'),

  h3('5.2. Validación con Great Expectations'),
  p('La validación está implementada como BranchPythonOperator (validar_yahoo, validar_finhub, validar_alpha) usando la API moderna de Great Expectations 1.x+ (versión instalada vía _PIP_ADDITIONAL_REQUIREMENTS). El patrón implementado replica fielmente el material de referencia del curso:'),
  bullet('Obtención de contexto: gx.get_context(mode="cloud") si hay token, fallback a mode="ephemeral".'),
  bullet('Registro de Data Source pandas, Asset DataFrame y Batch Definition para formalizar el batch.'),
  bullet('Creación de ExpectationSuite con métodos suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(...)) y gxe.ExpectColumnValuesToBeBetween(...).'),
  bullet('ValidationDefinition que vincula el batch con la suite, ejecutada con validation_definition.run().'),

  p('Reglas aplicadas:'),
  table([
    ['Regla', 'Aplica a'],
    ['ExpectColumnValuesToNotBeNull', 'symbol, timestamp'],
    ['ExpectColumnValuesToNotBeNull + ToBeBetween (>0)', 'open, high, low, close (Yahoo / Finnhub)'],
    ['ExpectColumnValuesToBeBetween (>0)', 'price (Alpha)'],
  ], [5000, 4000]),

  p('Si una sola expectativa falla, todo el lote se redirige a la tabla quarantine_quotes con el payload original en JSONB y la razón del fallo. Esto cumple el principio de "fail-safe" mencionado en el rubric.'),

  h3('5.3. Modelo dimensional (esquema en estrella)'),
  p('El warehouse implementa un esquema en estrella clásico sobre PostgreSQL, definido en sql/01_init_schema.sql que se ejecuta automáticamente al primer arranque del contenedor de Postgres.'),

  table([
    ['Tabla', 'Tipo', 'Rol'],
    ['dim_symbol', 'Dimensión', 'Catálogo de pares (base/quote currency)'],
    ['dim_source', 'Dimensión', 'Yahoo / Finnhub / Alpha. Precargada por seed.'],
    ['dim_time', 'Dimensión', 'Timestamp desnormalizado (year, month, day, hour, dow)'],
    ['fact_quotes', 'Hechos', 'OHLC + price con FKs y UNIQUE(time, symbol, source)'],
    ['quarantine_quotes', 'Soporte', 'Lotes rechazados con payload JSONB'],
    ['vw_quotes_flat', 'Vista', 'fact + dims joineado, lista para BI'],
  ], [2500, 2000, 4500]),

  h3('5.4. Justificación del diseño'),
  bullet('Estrella en lugar de copo de nieve: las dimensiones son pequeñas y no requieren jerarquías profundas. Simplicidad sobre normalización pura.'),
  bullet('UNIQUE(time_id, symbol_id, source_id) en fact_quotes: habilita reejecuciones idempotentes del DAG sin duplicar registros (INSERT ... ON CONFLICT DO NOTHING).'),
  bullet('dim_source con seed precargado en el SQL inicial: evita race conditions en la primera ejecución del DAG.'),
  bullet('Vista vw_quotes_flat: oculta los joins a Metabase y Streamlit. Cambios futuros en el modelo no afectan a los dashboards.'),

  h3('5.5. Diseño del DAG ETL_Delivery3'),
  p('El DAG tiene tres ramas paralelas que convergen en una única tarea de carga, siguiendo el patrón de fan-out / fan-in:'),
  code('extraccion_yahoo  → transformacion_yahoo  → validar_yahoo  ─┐\nextraccion_finhub → transformacion_finhub → validar_finhub ─┼→ [cargar_db | cuarentena]\nextraccion_alpha  → transformacion_alpha  → validar_alpha  ─┘'),

  p('La tarea cargar_db usa trigger_rule="none_failed_min_one_success" para ejecutarse aunque algunas ramas fallen, mientras al menos una haya pasado validación. Esto garantiza que un fallo en Alpha (por rate limit) no impide cargar los datos válidos de Yahoo y Finnhub.'),

  pageBreak(),

  // ============ 6. REAL-TIME STREAM SIMULATION (10%) ============
  h1('6. Real-Time Stream Simulation con Kafka'),

  h3('6.1. Producer (app/producer.py)'),
  p('Implementa exactamente lo que el rubric solicita: un productor que itera sobre las filas de la fact table y las publica al topic Kafka con un delay. El producer:'),
  bullet('Consulta fact_quotes haciendo JOIN con las 3 dimensiones para que cada mensaje sea autocontenido.'),
  bullet('Mantiene un cursor en memoria (last_id) para no reenviar mensajes ya publicados.'),
  bullet('Publica al topic quotes_stream con time.sleep(1.0) entre mensajes (configurable vía PRODUCER_DELAY).'),
  bullet('Particiona los mensajes por symbol para preservar orden por par de divisas.'),
  bullet('Cuando llega al final de fact_quotes, hace polling cada 10 segundos esperando nuevos datos.'),
  bullet('Tiene reconexión automática si Kafka cae temporalmente.'),

  h3('6.2. Configuración Kafka'),
  p('Kafka utiliza dos listeners para soportar conexiones desde host y desde otros contenedores:'),
  bullet('PLAINTEXT://kafka:29092 — para el producer y consumer corriendo dentro del docker-compose.'),
  bullet('PLAINTEXT_HOST://localhost:9092 — para conexiones desde el host de desarrollo.'),

  p('El topic quotes_stream se crea automáticamente al recibir el primer mensaje (auto.create.topics.enable=true por defecto). La replicación es de 1 ya que es un cluster single-broker para desarrollo.'),

  // ============ 7. REAL-TIME VISUALIZATION (15%) ============
  h1('7. Real-Time Visualization con Streamlit'),

  h3('7.1. Arquitectura del dashboard'),
  p('El dashboard está implementado en app/dashboard.py usando Streamlit + Plotly. La arquitectura inicial usaba un thread daemon para consumir Kafka en background, pero esto causaba errores de "Event loop is closed" por la incompatibilidad entre threading y el ciclo de vida de Streamlit. Se rediseñó con un enfoque síncrono:'),
  bullet('@st.cache_resource mantiene un único KafkaConsumer vivo entre reruns de Streamlit.'),
  bullet('En cada rerun del script, se llama consumer.poll(timeout_ms=1500) para sacar nuevos mensajes.'),
  bullet('Los mensajes nuevos se appendean a un deque(maxlen=2000) cacheado.'),
  bullet('La UI se redibuja cada 2 segundos con un snapshot del buffer.'),
  bullet('Sin threads, sin asyncio, robusto y simple.'),

  h3('7.2. Componentes de la UI'),
  bullet('Status bar superior con 4 KPIs: estado de conexión, total recibido, nuevos este refresh, intervalo de refresh.'),
  bullet('Sidebar con selector de par de divisas, multiselect de fuentes y slider para tamaño de ventana.'),
  bullet('4 KPIs de mercado: último precio + Δ%, máximo, mínimo y conteo de ticks en la ventana.'),
  bullet('Gráfico Plotly con una línea por fuente, permitiendo comparar visualmente Yahoo vs Finnhub vs Alpha.'),
  bullet('Vista panorámica con tabla del último precio de cada símbolo en el buffer.'),
  bullet('Expander con los últimos 20 ticks crudos del símbolo seleccionado.'),

  // ============ 8. STATIC VISUALIZATION (10%) ============
  h1('8. Static Visualization con Metabase'),

  h3('8.1. Decisión: Metabase vs Power BI / Looker'),
  p('El rubric menciona Power BI y Looker Studio como ejemplos de BI tradicional. Se eligió Metabase por las siguientes razones:'),
  bullet('Open source, sin necesidad de licencia ni cuenta corporativa.'),
  bullet('Se integra directamente al docker-compose como un servicio más.'),
  bullet('Conecta nativamente a PostgreSQL sin drivers raros.'),
  bullet('Auto-detecta el schema y reconoce las foreign keys, sugiriendo joins automáticamente.'),
  bullet('Permite al evaluador ver los dashboards funcionando en vivo, no solo capturas estáticas.'),

  h3('8.2. Configuración de Metabase'),
  p('Metabase se conecta al PostgreSQL del proyecto en el primer arranque mediante un setup wizard. La configuración usa la red interna de Docker (host: postgres, port: 5432), por lo que no requiere abrir puertos al exterior. Sus metadatos persistentes (usuarios, dashboards, configuraciones) se guardan en una base de datos metabase dentro del mismo PostgreSQL, creada automáticamente por sql/00_create_metabase_db.sql.'),

  h3('8.3. KPIs implementados'),
  p('El dashboard analítico consume principalmente de la vista vw_quotes_flat (joinea fact + dims). Los queries están versionados en docs/kpis.sql para reproducibilidad:'),
  bullet('Total de registros consolidados en el warehouse'),
  bullet('Fuentes activas alimentando el sistema'),
  bullet('Tasa de éxito de validación (% de filas que pasaron Great Expectations)'),
  bullet('Registros enviados a cuarentena'),
  bullet('Tiempo de la última carga (heartbeat del DAG)'),
  bullet('Estadísticas descriptivas por fuente (promedio, mín, máx, desviación estándar)'),
  bullet('Evolución del precio por fuente para comparación cross-source'),
  bullet('Diferencia diaria entre Alpha y Yahoo'),
  bullet('Cobertura del pipeline por día y fuente (stacked bar)'),
  bullet('Crecimiento del warehouse en el tiempo'),
  bullet('Tendencia OHLC durante la jornada'),

  pageBreak(),

  // ============ 9. VALUE GENERATION (10%) ============
  h1('9. Articulación de generación de valor'),

  h3('9.1. Valor operacional — Stream en tiempo real'),
  p('El Kafka producer + consumer Streamlit soporta procesos operacionales donde la latencia importa: un trader, un sistema de alertas o un motor de toma de decisiones automatizada necesitan ver el precio actual con segundos de retardo, no horas. El topic quotes_stream actúa como un bus de eventos al que pueden conectarse múltiples consumidores simultáneamente: por ejemplo, un servicio futuro de alertas que dispare notificaciones cuando un par cruce un umbral, o un microservicio de risk management que recalcule la exposición en vivo.'),
  p('La arquitectura desacopla la producción de eventos del consumo, lo que es la base de cualquier sistema reactivo en producción. Para el caso de uso del proyecto (ODS 1 — No pobreza), el componente operativo abre la puerta a productos como aplicaciones móviles de remesas que ajusten tasas en tiempo real para beneficiar a poblaciones vulnerables, o sistemas de alerta temprana para microempresas con exposición cambiaria.'),

  h3('9.2. Valor analítico — Modelo dimensional + dashboard estático'),
  p('El modelo dimensional sobre PostgreSQL + Metabase soporta procesos analíticos donde la profundidad histórica y la capacidad de cruzar dimensiones pesan más que la latencia. El esquema en estrella permite responder preguntas como: "¿cuál fue la volatilidad promedio del EUR/USD los lunes vs los viernes del último mes?" o "¿qué fuente reporta sistemáticamente precios más altos en horas de baja liquidez?" — preguntas imposibles de responder con un stream efímero.'),
  p('Esta capa es la base sobre la cual se construirán los modelos predictivos futuros mencionados como objetivo del proyecto. Tener los datos limpios, validados y dimensionados en un warehouse es prerrequisito de cualquier proyecto de machine learning serio. El dashboard de Metabase materializa este valor para usuarios no técnicos (gestores de producto, analistas financieros, decisores de política pública), permitiéndoles explorar los datos sin escribir SQL.'),

  h3('9.3. Convergencia de ambos valores'),
  p('Crucialmente, ambos planos consumen del mismo modelo dimensional: el producer de Kafka NO lee del stream crudo de Finnhub, sino de fact_quotes ya validada. Esto garantiza que lo que se ve en el dashboard real-time es el mismo dato que se ve en el dashboard analítico (con el desfase natural de la simulación), eliminando la temida "verdad doble" que afecta a arquitecturas Lambda mal diseñadas.'),
  p('La validación con Great Expectations en el DAG es el gate único de calidad, por lo que ningún dato sucio llega ni al BI ni al stream. Es una arquitectura coherente, no un conjunto de piezas desconectadas.'),

  pageBreak(),

  // ============ 10. FEEDBACK ENTREGA 2 ============
  h1('10. Atención al feedback de la entrega 2'),
  p('Comentario del docente sobre la entrega 2:'),
  new Paragraph({
    spacing: { after: 240, before: 120 },
    indent: { left: 720 },
    children: [new TextRun({
      text: '"Muy incompleto el trabajo. Crearon muchas tareas fantasmas, que no tienen una conclusión real, por otro lado simulan el merge y la carga con bashoperator. La orquestación falla desde ahí. Las visualizaciones deben ser desde la base de datos consolidada, no desde las fuentes obtenidas."',
      font: FONT, size: 22, italics: true, color: '595959',
    })],
  }),

  p('A continuación se documenta cómo cada observación fue atendida en esta entrega final:'),

  table([
    ['Observación del docente', 'Solución implementada'],
    ['Tareas fantasma (Merge, validar_merge, verificar_db, crear_db)', 'Eliminadas. Solo permanecen tareas con efecto real en la BD.'],
    ['Merge y carga simulados con BashOperator (echo)', 'cargar_db reescrita como PythonOperator con inserciones reales vía SQLAlchemy a dim_* y fact_quotes.'],
    ['Orquestación falla desde la carga', 'DAG ETL_Delivery3 end-to-end funcional, idempotente. La cuarentena ahora persiste en tabla con JSONB en lugar de un echo.'],
    ['Visualizaciones desde las fuentes, no del warehouse', 'Metabase conecta a vw_quotes_flat (vista del modelo dimensional). El producer de Kafka lee de fact_quotes, no del WebSocket crudo.'],
  ], [4000, 5500]),

  // ============ 11. DECISIONES TÉCNICAS ============
  h1('11. Decisiones técnicas y trade-offs'),

  table([
    ['Decisión', 'Alternativa considerada', 'Trade-off'],
    ['PostgreSQL en lugar de SQLite', 'Mantener SQLite de la entrega 2', '+ Conexión nativa a BI, + concurrencia. − Más memoria.'],
    ['Metabase para BI estático', 'Power BI / Looker Studio', '+ Open source, integra en compose. − Menos polish que PBI.'],
    ['Streamlit para real-time', 'Dash / Bokeh', '+ Más simple, hot-reload. − Threading limitado.'],
    ['Producer lee de fact_quotes', 'Stream directo de Finnhub a Kafka', '+ Garantiza calidad validada. − No es CDC "real".'],
    ['Ramas paralelas del DAG', 'Pipeline secuencial', '+ Throughput. − Más complejidad en el branching.'],
    ['Esquema dimensional', 'Tabla por símbolo (estrategia E2)', '+ Soporta BI. − Requiere upserts en dimensiones.'],
    ['GE API moderna + GX Cloud', 'API legacy gx.from_pandas()', '+ Suite persistente, mismo patrón del docente. − Más código.'],
    ['NaN → NULL al cargar', 'Aceptar NaN literal de PostgreSQL', '+ COALESCE funciona en Metabase. − Filtro extra en el load.'],
    ['Streamlit síncrono (poll en cada rerun)', 'Thread daemon con consumer', '+ Sin "Event loop closed". − Latencia ligeramente mayor.'],
    ['Finnhub WebSocket persistente', 'Solo durante preparar.py', '+ Datos frescos siempre. − Servicio extra en compose.'],
  ], [3000, 3000, 3500]),

  // ============ 12. CÓMO EJECUTAR ============
  h1('12. Cómo ejecutar el proyecto'),

  h3('12.1. Requisitos previos'),
  bullet('Docker Desktop con WSL2 (Windows) o nativo (Linux/Mac)'),
  bullet('6 GB de RAM asignados a Docker mínimo'),
  bullet('Puertos libres: 8080, 8501, 3000, 5433, 9092'),

  h3('12.2. Pasos para levantar el stack'),
  code('# 1. Clonar el repositorio\ngit clone https://github.com/AlessandroYustyCeballos/FinalDeliveryETL.git\ncd FinalDeliveryETL\n\n# 2. Levantar todo el stack\ndocker compose up -d\n\n# 3. Verificar que todos los servicios estén Up\ndocker compose ps\n\n# 4. Activar el DAG ETL_Delivery3 en Airflow UI\n#    http://localhost:8080 (admin / admin)\n\n# 5. Configurar Metabase la primera vez\n#    http://localhost:3000\n\n# 6. Abrir el dashboard real-time\n#    http://localhost:8501'),

  h3('12.3. Comandos útiles'),
  code('# Estado de las 3 fuentes en el warehouse\ndocker exec -it postgres psql -U etl_user -d trading_db -c "SELECT source_name, COUNT(*), MAX(timestamp) FROM vw_quotes_flat GROUP BY source_name;"\n\n# Logs del WebSocket de Finnhub en vivo\ndocker compose logs finnhub-stream -f\n\n# Trigger del DAG por CLI\ndocker compose exec airflow-scheduler airflow dags trigger ETL_Delivery3\n\n# Reiniciar el producer si su cursor se desincroniza\ndocker compose restart producer'),

  pageBreak(),

  // ============ 13. CONCLUSIONES ============
  h1('13. Conclusiones'),
  p('Esta entrega final implementa un pipeline ETL completo, reproducible y funcional end-to-end. La arquitectura cumple los siete puntos del rubric: ingesta multi-fuente con tres patrones distintos (batch, stream WebSocket, API REST), transformación a un esquema unificado, modelado dimensional formal en estrella, validación con Great Expectations versión moderna, simulación de streaming con Kafka, dashboard real-time en Streamlit y dashboard analítico en Metabase. Todo se levanta con un único comando de docker-compose y se entrega documentado en este informe y en el README del repositorio.'),
  p('Los problemas señalados en la retroalimentación de la segunda entrega fueron atendidos explícitamente: se eliminaron las tareas fantasma, se reescribió la carga como un PythonOperator real con SQLAlchemy, y las visualizaciones consumen del modelo dimensional consolidado (no de los CSVs intermedios). La iteración entre entregas demuestra el valor del feedback técnico y la capacidad del equipo de evolucionar la arquitectura.'),
  p('La articulación entre el plano operacional (Kafka + Streamlit) y el plano analítico (modelo dimensional + Metabase) es coherente porque ambos consumen de la misma fuente de verdad (fact_quotes validada), evitando los problemas típicos de arquitecturas Lambda mal diseñadas. Esto permite construir productos futuros (alertas, modelos predictivos, dashboards adicionales) sin reescribir la base.'),

  h3('13.1. Próximos pasos'),
  bullet('Migrar el _PIP_ADDITIONAL_REQUIREMENTS a un Dockerfile propio para imagen reproducible y arranque más rápido.'),
  bullet('Implementar un consumer Kafka adicional que persista métricas agregadas (alertas) en una tabla fact_alerts.'),
  bullet('Entrenar el modelo predictivo de variación cambiaria mencionado como objetivo del proyecto (asociado al ODS 1).'),
  bullet('Añadir CI con GitHub Actions: linter de Python + tests unitarios de las funciones de transformación.'),
  bullet('Documentar runbook de operaciones (cómo monitorear, cómo escalar, cómo debuggear).'),

  // ============ 14. ANEXOS ============
  h1('14. Anexos'),

  h3('14.1. Repositorio'),
  p('https://github.com/AlessandroYustyCeballos/FinalDeliveryETL'),

  h3('14.2. Estructura del repositorio'),
  code('FinalDeliveryETL/\n├── app/\n│   ├── producer.py        # Kafka producer (fact_quotes → topic)\n│   ├── consumer.py        # Consumer standalone (debug)\n│   └── dashboard.py       # Streamlit real-time\n├── dags/\n│   └── ETL.py             # DAG ETL_Delivery3\n├── sql/\n│   ├── 00_create_metabase_db.sql\n│   └── 01_init_schema.sql # Modelo dimensional\n├── docs/\n│   ├── technical_report.md\n│   ├── informe_entrega_final.docx (este archivo)\n│   ├── generate_docx.js\n│   ├── kpis.sql           # Biblioteca de queries para Metabase\n│   └── Metabase - Dashboard estatico.pdf\n├── notebooks/\n│   ├── eda_dataset.ipynb  # EDA del batch (Yahoo)\n│   ├── eda_api.ipynb      # EDA del stream (Finnhub)\n│   └── data/              # CSVs de muestra\n├── Utils/                 # Helpers de extracción\n├── docker-compose.yml     # 10 servicios\n├── preparar.py            # Bootstrap de CSVs\n├── Save.py                # Helper de persistencia\n├── requirements.txt\n├── .gitignore\n└── README.md'),

  h3('14.3. Notebooks de EDA'),
  bullet('eda_dataset.ipynb — EDA del batch (Yahoo): calidad del dato, validación lógica OHLC, distribución del precio de cierre, cobertura temporal, comparativa entre pares.'),
  bullet('eda_api.ipynb — EDA del stream (Finnhub): tasa de mensajes, inter-arrival times, justificación cuantitativa de la agregación a 1 minuto.'),
];

// ---------- Build the document ----------
const doc = new Document({
  styles: {
    default: { document: { run: { font: FONT, size: 22 } } },
    paragraphStyles: [
      { id: 'Heading1', name: 'Heading 1', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 32, bold: true, font: FONT, color: '1F3864' },
        paragraph: { spacing: { before: 360, after: 200 }, outlineLevel: 0 } },
      { id: 'Heading2', name: 'Heading 2', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 26, bold: true, font: FONT, color: '2E74B5' },
        paragraph: { spacing: { before: 240, after: 160 }, outlineLevel: 1 } },
      { id: 'Heading3', name: 'Heading 3', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 24, bold: true, font: FONT, color: '2E74B5' },
        paragraph: { spacing: { before: 200, after: 120 }, outlineLevel: 2 } },
    ],
  },
  numbering: {
    config: [
      { reference: 'bullets', levels: [{ level: 0, format: LevelFormat.BULLET, text: '•',
        alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
    ],
  },
  sections: [{
    properties: {
      page: {
        size: { width: 12240, height: 15840 },
        margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 },
      },
    },
    children: content,
  }],
});

const outPath = path.join(__dirname, 'informe_entrega_final.docx');
Packer.toBuffer(doc).then((buffer) => {
  fs.writeFileSync(outPath, buffer);
  console.log(`✅ Generado: ${outPath}`);
});
