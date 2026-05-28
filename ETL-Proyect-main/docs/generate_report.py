"""
Genera el informe PDF de la entrega final.
Uso: python docs/generate_report.py
Salida: docs/informe_entrega_final.pdf
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether
)
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER, TA_LEFT

OUT = "docs/informe_entrega_final.pdf"

doc = SimpleDocTemplate(
    OUT, pagesize=A4,
    leftMargin=2*cm, rightMargin=2*cm,
    topMargin=2*cm, bottomMargin=2*cm,
    title="ETL Forex - Informe Entrega Final",
    author="Bryan Herrera, Alessandro Yusty"
)

styles = getSampleStyleSheet()
h1 = ParagraphStyle('h1', parent=styles['Heading1'], fontSize=18,
                     textColor=colors.HexColor('#1a4480'), spaceAfter=12)
h2 = ParagraphStyle('h2', parent=styles['Heading2'], fontSize=14,
                     textColor=colors.HexColor('#1a4480'), spaceAfter=8, spaceBefore=14)
h3 = ParagraphStyle('h3', parent=styles['Heading3'], fontSize=12,
                     textColor=colors.HexColor('#2a5f9e'), spaceAfter=6, spaceBefore=10)
body = ParagraphStyle('body', parent=styles['BodyText'], fontSize=10,
                       alignment=TA_JUSTIFY, leading=14, spaceAfter=6)
small = ParagraphStyle('small', parent=styles['BodyText'], fontSize=9,
                        alignment=TA_LEFT, textColor=colors.grey)
bullet = ParagraphStyle('bullet', parent=body, leftIndent=14, bulletIndent=0)
code = ParagraphStyle('code', parent=styles['Code'], fontSize=8,
                       textColor=colors.HexColor('#333'),
                       backColor=colors.HexColor('#f4f4f4'),
                       borderPadding=6, leading=11)

def tbl(data, col_widths=None, header_bg='#1a4480'):
    t = Table(data, colWidths=col_widths)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor(header_bg)),
        ('TEXTCOLOR',  (0,0), (-1,0), colors.white),
        ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE',   (0,0), (-1,-1), 9),
        ('ALIGN',      (0,0), (-1,-1), 'LEFT'),
        ('VALIGN',     (0,0), (-1,-1), 'TOP'),
        ('GRID',       (0,0), (-1,-1), 0.4, colors.lightgrey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f7f9fc')]),
        ('LEFTPADDING',(0,0), (-1,-1), 6),
        ('RIGHTPADDING',(0,0),(-1,-1), 6),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING',(0,0),(-1,-1), 4),
    ]))
    return t

story = []

# ============================ PORTADA ============================
story.append(Spacer(1, 4*cm))
story.append(Paragraph("Proyecto ETL Forex", ParagraphStyle(
    'cover_title', parent=h1, fontSize=28, alignment=TA_CENTER,
    textColor=colors.HexColor('#1a4480'))))
story.append(Spacer(1, 0.3*cm))
story.append(Paragraph("Informe de Entrega Final", ParagraphStyle(
    'cover_sub', parent=h1, fontSize=18, alignment=TA_CENTER,
    textColor=colors.HexColor('#2a5f9e'))))
story.append(Spacer(1, 2*cm))
story.append(Paragraph(
    "Pipeline ETL end-to-end para datos del mercado forex, "
    "orquestado con Airflow, con modelo dimensional en PostgreSQL, "
    "streaming en Kafka y dashboards en Metabase y Streamlit.",
    ParagraphStyle('cover_desc', parent=body, alignment=TA_CENTER,
                    fontSize=11, textColor=colors.grey)))
story.append(Spacer(1, 3*cm))
story.append(tbl([
    ['Integrantes', 'Bryan Andres Herrera Betancur (2244008)'],
    ['', 'Alessandro Yusty Ceballos (2240248)'],
    ['Curso', 'ETL (G51)'],
    ['Docente', 'Daniel Felipe Romero Bernal'],
    ['Programa', 'Ingeniería de Datos e Inteligencia Artificial'],
    ['Universidad', 'Universidad Autónoma de Occidente'],
    ['Fecha', 'Mayo 2026'],
], col_widths=[4*cm, 11*cm], header_bg='#1a4480'))

story.append(PageBreak())

# ====================== 1. RESUMEN EJECUTIVO ======================
story.append(Paragraph("1. Resumen ejecutivo", h1))
story.append(Paragraph(
    "Este proyecto implementa un pipeline ETL completo, reproducible y "
    "orquestado, que consume datos del mercado forex desde tres fuentes "
    "complementarias (Yahoo Finance, Finnhub vía WebSocket y Alpha Vantage), "
    "los valida con reglas declarativas de calidad y los consolida en un "
    "modelo dimensional sobre PostgreSQL. Los datos se exponen a través "
    "de dos dashboards complementarios: uno analítico construido sobre "
    "Metabase y otro operativo en tiempo real construido sobre Streamlit "
    "y Plotly, conectado al stream de Kafka.", body))
story.append(Paragraph(
    "Toda la infraestructura se levanta con un único comando "
    "(<font face='Courier'>docker compose up -d</font>), incluyendo nueve "
    "servicios: Postgres del proyecto, Postgres del metastore de Airflow, "
    "Zookeeper, Kafka, Airflow scheduler, Airflow webserver, Metabase, "
    "el producer de Kafka y el dashboard Streamlit.", body))

story.append(Paragraph("Objetivos cumplidos", h3))
for it in [
    "Extracción desde 3 fuentes (Yahoo, Finnhub WebSocket, Alpha Vantage).",
    "Transformación a esquema unificado (timestamp, symbol, OHLC, source).",
    "Validación de calidad declarativa con bifurcación a cuarentena.",
    "Carga REAL al modelo dimensional (sin tareas fantasma).",
    "Kafka producer simulando CDC sobre la fact table.",
    "Kafka consumer + Streamlit con dashboard en tiempo real.",
    "Dashboard analítico en Metabase sobre la BD consolidada.",
    "Dos notebooks EDA (dataset y API).",
    "Documento técnico, README y .gitignore.",
]:
    story.append(Paragraph("• " + it, bullet))

# =========== 2. RETROALIMENTACIÓN ENTREGA 2 ATENDIDA ==============
story.append(Paragraph("2. Atención a la retroalimentación de la entrega 2", h1))
story.append(Paragraph(
    "<i>Comentario del docente sobre la entrega 2:</i>", body))
story.append(Paragraph(
    "<font color='#666666'>«Muy incompleto el trabajo. Crearon muchas "
    "tareas fantasmas, que no tienen una conclusión real, por otro lado "
    "simulan el merge y la carga con bashoperator. La orquestación falla "
    "desde ahí. Las visualizaciones deben ser desde la base de datos "
    "consolidada, no desde las fuentes obtenidas.»</font>",
    ParagraphStyle('quote', parent=body, leftIndent=20,
                    fontName='Helvetica-Oblique', textColor=colors.HexColor('#555'))))

story.append(Spacer(1, 6))
story.append(tbl([
    ['Observación del docente', 'Solución implementada'],
    ['Tareas fantasma (Merge, validar_merge, verificar_db, crear_db)',
     'Eliminadas. Solo permanecen tareas con efecto real en la BD.'],
    ['Merge y carga simulados con BashOperator (echo)',
     'cargar_db reescrita como PythonOperator con inserciones reales vía SQLAlchemy a dim_* y fact_quotes.'],
    ['Orquestación falla desde la carga',
     'DAG ETL_Delivery3 end-to-end funcional, idempotente. Cuarentena persiste en tabla con JSONB.'],
    ['Visualizaciones desde las fuentes, no del warehouse',
     'Metabase conecta a vw_quotes_flat. Producer de Kafka lee de fact_quotes (no del WebSocket crudo).'],
], col_widths=[7*cm, 9*cm]))

# =================== 3. ARQUITECTURA ==============================
story.append(PageBreak())
story.append(Paragraph("3. Arquitectura", h1))
story.append(Paragraph(
    "La arquitectura sigue un patrón Lambda simplificado donde un único "
    "modelo dimensional alimenta tanto el plano analítico (Metabase) "
    "como el plano operativo (Kafka + Streamlit), garantizando consistencia "
    "entre ambos dashboards y eliminando la redundancia de fuentes de verdad.", body))

story.append(Paragraph("3.1. Stack tecnológico", h3))
story.append(tbl([
    ['Capa', 'Tecnología', 'Versión'],
    ['Orquestación',         'Apache Airflow',       '2.9.2'],
    ['Data Warehouse',       'PostgreSQL',           '15'],
    ['Streaming',            'Apache Kafka (Confluent)', '7.6.1'],
    ['Validación',           'Reglas declarativas en pandas', '-'],
    ['BI analítico',         'Metabase',             'latest'],
    ['Real-time dashboard',  'Streamlit + Plotly',   'latest'],
    ['Contenerización',      'Docker Compose',       'v2'],
    ['Lenguaje',             'Python',               '3.11'],
], col_widths=[4*cm, 8*cm, 3*cm]))

story.append(Paragraph("3.2. Flujo end-to-end", h3))
story.append(Paragraph(
    "1. <b>preparar.py</b> genera los CSVs iniciales (yahoo.csv, finhub.csv) "
    "tras elegir un par de divisas.<br/>"
    "2. <b>Airflow DAG (ETL_Delivery3)</b> corre cada 2 minutos con tres "
    "ramas paralelas (Yahoo / Finnhub / Alpha). Cada rama: extrae → "
    "transforma → valida → carga.<br/>"
    "3. <b>Kafka producer</b> consulta fact_quotes y publica al topic "
    "<font face='Courier'>quotes_stream</font> con un delay de 1 segundo "
    "entre mensajes, simulando CDC tal como pide el enunciado.<br/>"
    "4. <b>Streamlit</b> consume el topic en background y grafica con "
    "Plotly. Buffer rodante de 2000 ticks.<br/>"
    "5. <b>Metabase</b> conecta a la vista vw_quotes_flat (fact + dims) "
    "y permite construir dashboards drag-and-drop.", body))

# ============== 4. MODELO DIMENSIONAL =============================
story.append(Paragraph("4. Modelo dimensional", h1))
story.append(Paragraph(
    "Esquema en estrella sobre PostgreSQL, definido en "
    "<font face='Courier'>sql/01_init_schema.sql</font>. Se crea "
    "automáticamente la primera vez que arranca el contenedor de Postgres.", body))

story.append(tbl([
    ['Tabla', 'Tipo', 'Rol'],
    ['dim_symbol',         'Dimensión', 'Catálogo de pares (base/quote currency)'],
    ['dim_source',         'Dimensión', 'Yahoo / Finnhub / Alpha. Precargada por seed.'],
    ['dim_time',           'Dimensión', 'Timestamp desnormalizado (year, month, day, hour, minute, dow)'],
    ['fact_quotes',        'Hechos',    'OHLC + price con FKs y UNIQUE(time, symbol, source)'],
    ['quarantine_quotes',  'Soporte',   'Lotes rechazados con payload JSONB y razón'],
    ['vw_quotes_flat',     'Vista',     'fact + dims joineado, listo para BI'],
], col_widths=[4*cm, 2.5*cm, 8.5*cm]))

story.append(Paragraph("Justificación del diseño", h3))
for it in [
    "Estrella en lugar de copo de nieve: las dimensiones son pequeñas y no requieren jerarquías profundas.",
    "UNIQUE(time, symbol, source) en fact_quotes habilita reejecuciones idempotentes del DAG.",
    "dim_source con seed precargado evita race conditions en la primera ejecución.",
    "Vista vw_quotes_flat se expone como punto de entrada único para BI, ocultando los joins.",
]:
    story.append(Paragraph("• " + it, bullet))

# ============== 5. AIRFLOW DAG ====================================
story.append(PageBreak())
story.append(Paragraph("5. Diseño del DAG (ETL_Delivery3)", h1))
story.append(Paragraph(
    "El DAG está diseñado con tres ramas paralelas que convergen en "
    "una única tarea de carga al modelo dimensional. Cada rama es "
    "independiente: si una fuente falla validación, las otras continúan.", body))

story.append(Paragraph("Árbol de dependencias", h3))
story.append(Paragraph(
    "<font face='Courier' size='9'>"
    "extraccion_yahoo  &rarr; transformacion_yahoo  &rarr; validar_yahoo  &mdash;┐<br/>"
    "extraccion_finhub &rarr; transformacion_finhub &rarr; validar_finhub &mdash;┼&rarr; [cargar_db | cuarentena]<br/>"
    "extraccion_alpha  &rarr; transformacion_alpha  &rarr; validar_alpha  &mdash;┘"
    "</font>", code))

story.append(Paragraph("Tareas", h3))
story.append(tbl([
    ['Task ID', 'Tipo', 'Función'],
    ['extraccion_yahoo / finhub / alpha', 'PythonOperator', 'Lee CSV o hace request a Alpha. 3 tareas paralelas.'],
    ['transformacion_yahoo / finhub / alpha', 'PythonOperator', 'Normaliza al esquema unificado. Devuelve ruta vía XCom.'],
    ['validar_yahoo / finhub / alpha', 'BranchPythonOperator', 'Aplica reglas de calidad. Decide rama: cargar_db o cuarentena.'],
    ['cargar_db', 'PythonOperator', 'INSERT REAL en dim_* y fact_quotes vía SQLAlchemy.'],
    ['cuarentena', 'PythonOperator', 'INSERT REAL en quarantine_quotes con payload JSONB.'],
], col_widths=[4.5*cm, 3.5*cm, 7*cm]))

story.append(Paragraph("Diferencias clave vs entrega 2", h3))
story.append(tbl([
    ['Aspecto', 'Entrega 2', 'Entrega final'],
    ['cargar_db', 'BashOperator + echo', 'PythonOperator + SQLAlchemy'],
    ['Merge', 'BashOperator (fantasma)', 'Eliminada'],
    ['validar_merge', 'Sobre un echo', 'Eliminada'],
    ['cuarentena', 'BashOperator + echo', 'PythonOperator + JSONB'],
    ['Destino', 'SQLite, tabla por símbolo', 'PostgreSQL, esquema en estrella'],
], col_widths=[4*cm, 5*cm, 6*cm]))

# ============== 6. STREAMING ======================================
story.append(Paragraph("6. Streaming con Kafka", h1))
story.append(Paragraph(
    "<b>Producer (app/producer.py).</b> Implementa exactamente lo que "
    "el enunciado solicita: «a Python producer that iterates over the rows "
    "in your fact table and sends them to the Kafka topic with a slight "
    "time delay». Mantiene un cursor (último quote_id procesado) para "
    "no duplicar mensajes. Particiona por symbol para preservar orden. "
    "Reconexión automática a Kafka si falla.", body))
story.append(Paragraph(
    "<b>Dashboard (app/dashboard.py).</b> Thread daemon consume Kafka y "
    "llena un deque(maxlen=2000) thread-safe. Streamlit re-renderiza "
    "cada 2 segundos con un snapshot. UI con selector de símbolo, "
    "multiselect de fuentes, slider de ventana, 4 KPIs en vivo, gráfico "
    "Plotly con línea por fuente y tabla panorámica de todos los símbolos.", body))

# ============ 7. DASHBOARDS =======================================
story.append(Paragraph("7. Dashboards", h1))
story.append(Paragraph("7.1. Dashboard analítico (Metabase)", h3))
story.append(Paragraph(
    "Conecta a la BD <font face='Courier'>trading_db</font> y consume "
    "principalmente de la vista <font face='Courier'>vw_quotes_flat</font>. "
    "El proyecto incluye una biblioteca de 13 KPIs listos en "
    "<font face='Courier'>docs/kpis.sql</font>, adaptados al caso de uso "
    "(un par de divisas + tres fuentes).", body))

story.append(tbl([
    ['Sección', 'KPIs incluidos'],
    ['Salud del pipeline', 'Total registros, fuentes activas, % éxito validación, cuarentena, última carga'],
    ['Negocio (por fuente)', 'Evolución de close, estadísticas descriptivas, discrepancia Yahoo vs Finnhub'],
    ['Temporales', 'Cobertura por día y fuente, volatilidad por hora, vela OHLC'],
    ['Insights', 'Variación % diaria, distribución de precios'],
], col_widths=[5*cm, 11*cm]))

story.append(Paragraph("7.2. Dashboard real-time (Streamlit)", h3))
story.append(Paragraph(
    "Disponible en http://localhost:8501. Conecta al topic Kafka "
    "<font face='Courier'>quotes_stream</font> y muestra: último precio "
    "+ Δ%, máximo y mínimo de la ventana, gráfico Plotly con una traza por "
    "fuente, vista panorámica de todos los símbolos en buffer, expander con "
    "los últimos ticks crudos.", body))

# ============ 8. EDA ==============================================
story.append(PageBreak())
story.append(Paragraph("8. Análisis exploratorio (EDA)", h1))
story.append(tbl([
    ['Notebook', 'Contenido'],
    ['eda_dataset.ipynb',
     'EDA del batch (Yahoo): calidad del dato, validación lógica OHLC, '
     'distribución del precio de cierre, cobertura temporal, comparativa '
     'entre pares con normalización base-100.'],
    ['eda_api.ipynb',
     'EDA del stream (Finnhub): tasa de mensajes, inter-arrival times, '
     'justificación cuantitativa de la agregación a 1 minuto, '
     'comparación de volatilidad intra-minuto entre símbolos.'],
], col_widths=[5*cm, 11*cm]))

# ============ 9. VALUE GENERATION =================================
story.append(Paragraph("9. Value Generation Articulation", h1))
story.append(Paragraph("9.1. Valor operacional (real-time)", h3))
story.append(Paragraph(
    "El Kafka producer + consumer Streamlit soporta procesos operacionales "
    "donde la latencia importa: traders, sistemas de alertas o motores de "
    "decisión automatizados necesitan ver el precio actual con segundos "
    "de retardo. El topic <font face='Courier'>quotes_stream</font> actúa "
    "como un bus de eventos al que pueden conectarse múltiples consumidores. "
    "Por ejemplo, un servicio futuro de alertas que dispare notificaciones "
    "cuando un par cruce un umbral, o un microservicio de risk management. "
    "La arquitectura desacopla productores de consumidores, base de "
    "cualquier sistema reactivo en producción.", body))
story.append(Paragraph("9.2. Valor analítico (BI)", h3))
story.append(Paragraph(
    "El modelo dimensional + Metabase soporta procesos analíticos donde "
    "la profundidad histórica y la capacidad de cruzar dimensiones pesan "
    "más que la latencia. Permite responder preguntas como «¿cuál fue la "
    "volatilidad promedio del EUR/USD los lunes vs los viernes?» o «¿qué "
    "fuente reporta sistemáticamente precios más altos?». Es la base "
    "sobre la que se construirán los modelos predictivos futuros "
    "mencionados como objetivo del proyecto (ODS 1).", body))
story.append(Paragraph("9.3. Convergencia", h3))
story.append(Paragraph(
    "Crucialmente, ambos planos consumen del MISMO modelo dimensional: "
    "el producer de Kafka NO lee del stream crudo de Finnhub, sino de "
    "fact_quotes ya validada. Esto garantiza que el dashboard real-time "
    "muestra el mismo dato que el analítico (con el desfase de la "
    "simulación), eliminando la temida «verdad doble» típica de "
    "arquitecturas Lambda mal diseñadas.", body))

# ============ 10. DECISIONES TÉCNICAS =============================
story.append(PageBreak())
story.append(Paragraph("10. Decisiones técnicas y trade-offs", h1))
story.append(tbl([
    ['Decisión', 'Alternativa', 'Trade-off'],
    ['Postgres en lugar de SQLite', 'Mantener SQLite',
     '+ Conexión nativa a BI / + concurrencia. − Más memoria.'],
    ['Metabase para BI estático', 'Power BI / Looker',
     '+ Open source, integra en compose. − Menos polish.'],
    ['Streamlit para real-time', 'Dash / Bokeh',
     '+ Más simple, hot-reload. − Threading limitado.'],
    ['Producer lee de fact_quotes', 'Stream directo de Finnhub',
     '+ Garantiza calidad validada. − No es CDC real.'],
    ['Ramas paralelas del DAG', 'Pipeline secuencial',
     '+ Throughput. − Más complejidad en branching.'],
    ['Esquema dimensional', 'Tabla por símbolo (E2)',
     '+ Soporta BI. − Requiere upserts en dims.'],
    ['Validación pandas', 'Great Expectations 1.x+',
     '+ Menos deps, más rápido. − Pierde ecosistema GE.'],
], col_widths=[4*cm, 4*cm, 8*cm]))

# ============ 11. ENTREGABLES =====================================
story.append(Paragraph("11. Entregables", h1))
story.append(tbl([
    ['Archivo / Carpeta', 'Contenido'],
    ['docker-compose.yml', 'Stack completo de 9 servicios.'],
    ['dags/ETL.py', 'DAG ETL_Delivery3 (carga real al modelo dimensional).'],
    ['sql/01_init_schema.sql', 'Modelo dimensional (4 dims + 1 fact + cuarentena + vista).'],
    ['sql/00_create_metabase_db.sql', 'Crea la BD que Metabase usa para sus metadatos.'],
    ['app/producer.py', 'Kafka producer (simula CDC sobre fact_quotes).'],
    ['app/consumer.py', 'Consumer standalone para debug.'],
    ['app/dashboard.py', 'Dashboard Streamlit en tiempo real.'],
    ['notebooks/eda_dataset.ipynb', 'EDA del batch (Yahoo).'],
    ['notebooks/eda_api.ipynb', 'EDA del stream (Finnhub).'],
    ['docs/technical_report.md', 'Documento técnico completo.'],
    ['docs/kpis.sql', 'Biblioteca de 13 KPIs para Metabase.'],
    ['docs/architecture.png', 'Diagrama de arquitectura.'],
    ['README.md', 'Quick start, comandos útiles, decisiones clave.'],
    ['.gitignore', 'Excluye venv, logs, secretos, CSVs intermedios.'],
], col_widths=[6*cm, 10*cm]))

# ============ 12. CÓMO EJECUTAR ===================================
story.append(Paragraph("12. Cómo ejecutar el proyecto", h1))
story.append(Paragraph(
    "<font face='Courier' size='9'>"
    "# 1. Clonar el repositorio<br/>"
    "git clone https://github.com/&lt;usuario&gt;/ETL-Proyect.git<br/>"
    "cd ETL-Proyect<br/><br/>"
    "# 2. Levantar el stack<br/>"
    "docker compose up -d<br/><br/>"
    "# 3. Generar CSVs base<br/>"
    "docker compose exec airflow-scheduler python /opt/airflow/preparar.py<br/>"
    "#    Opción 1 + índice de divisa (ej. 0 = EURUSD)<br/><br/>"
    "# 4. Activar el DAG en Airflow UI<br/>"
    "#    http://localhost:8080 (admin / admin)<br/><br/>"
    "# 5. Abrir los dashboards<br/>"
    "#    Metabase:  http://localhost:3000<br/>"
    "#    Streamlit: http://localhost:8501"
    "</font>", code))

# ============ 13. PRÓXIMOS PASOS ==================================
story.append(Paragraph("13. Próximos pasos", h1))
for it in [
    "Migrar _PIP_ADDITIONAL_REQUIREMENTS a un Dockerfile propio para imagen reproducible.",
    "Implementar un consumer Kafka que persista métricas agregadas en una tabla fact_alerts.",
    "Entrenar el modelo predictivo de variación cambiaria (objetivo asociado al ODS 1).",
    "Añadir CI con GitHub Actions: linter de Python + tests unitarios.",
    "Documentar runbook de operaciones (cómo monitorear, cómo escalar, cómo debuggear).",
]:
    story.append(Paragraph("• " + it, bullet))

story.append(Spacer(1, 1*cm))
story.append(Paragraph(
    "<i>Generado automáticamente desde docs/generate_report.py · "
    "Universidad Autónoma de Occidente · Mayo 2026</i>",
    ParagraphStyle('foot', parent=small, alignment=TA_CENTER)))

doc.build(story)
print(f"PDF generado: {OUT}")
