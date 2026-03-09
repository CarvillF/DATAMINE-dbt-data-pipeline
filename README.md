# PSet 02: Data Mining

Este repositorio contiene la implementación de un data pipeline con Mage que ingesta datos del dataset **NYC TLC Trip Record Data (Yellow y Green)**, estructurando los datos en una arquitectura de medallas (Bronze, Silver, Gold) utilizando **PostgreSQL** y **dbt**.

---

## 1. Arquitectura (Bronze / Silver / Gold) + Diagrama Textual

El pipeline sigue una arquitectura de medallas para garantizar la calidad y estructura de los datos:

*   **Bronze (Raw):** Capa de aterrizaje en PostgreSQL. Contiene los datos crudos extraídos directamente de la fuente mensual, añadiendo metadatos de ingesta (`ingest_ts`, `source_month`, `service_type`).
*   **Silver (Curated):** Capa de limpieza y estandarización materializada como **vistas (views)** mediante dbt. Aquí se tipifican datos, se aplican reglas de calidad (ej. `trip_distance >= 0`) y se unifican los servicios (Yellow/Green).
*   **Gold (Marts):** Capa analítica modelada en **esquema estrella** (1 fila = 1 viaje) materializada como **tablas (tables)**. Implementa particionamiento declarativo nativo de PostgreSQL para optimizar consultas.

### Diagrama Textual del Flujo

```text
[NYC TLC Website] & [Taxi Zone Lookup]
       |
       | (Descarga e Ingesta vía Mage)
       v
+---------------------------------------------------+
|               POSTGRESQL DATABASE                 |
|                                                   |
|  [ BRONZE SCHEMA ]                                |
|  Tablas raw + Metadatos (ingest_ts, source_month) |
|       |                                           |
|       | (Transformación dbt - dbt_build_silver)   |
|       v                                           |
|[ SILVER SCHEMA ]                                  |
|  Vistas limpias, unificadas y tipificadas         |
|       |                                           |
|       | (Transformación dbt - dbt_build_gold)     |
|       v                                           |
|  [ GOLD SCHEMA ]                                  |
|  Modelo Estrella (Tablas Particionadas)           |
|  - fct_trips (RANGE)                              |
|  - dim_zone (HASH)                                |
|  - dim_service_type, dim_payment_type (LIST)      |
+---------------------------------------------------+
       |
       | (Jupyter Notebook / JupySQL)
       v
[ Análisis de Negocio (20 Preguntas) ]
```

---

## 2. Tabla de Cobertura por Mes y Servicio

*Nota: La siguiente tabla documenta la ingesta de datos para los años 2022 a 2025.*

| year_month | service_type | status | row_count |
| :--- | :--- | :--- | :--- |
|[ EJ: 2022-01 ] | [ EJ: yellow ] | [ EJ: loaded / missing ] |[ INSERTA CONTEO ] |
| [ EJ: 2022-01 ] | [ EJ: green ] | [ EJ: loaded / missing ] | [ INSERTA CONTEO ] |
|[ INSERTA RESTO DE MESES... ] | ... | ... | ... |

---

## 3. Cómo levantar el stack

Para reproducir este proyecto en un entorno local, asegúrate de tener instalado Docker y Docker Compose.

1. Clona este repositorio.
2. Crea el archivo `.env` basándote en el archivo de ejemplo:
   ```bash
   cp .env.example .env
   ```
3. Levanta los contenedores en segundo plano:
   ```bash
   docker compose up -d
   ```
4. Accede a la interfaz de **Mage** en tu navegador web a través de `http://localhost:6789`.
5. La base de datos PostgreSQL estará expuesta en el puerto `5432` con las credenciales configuradas en el `.env`.

---

## 4. Nombres de Mage pipelines y qué hace cada uno

*   **`ingest_bronze`**: Pipeline encargado de descargar los datos mensuales (Yellow/Green) y el Taxi Zone Lookup. Realiza un chunking mensual y asegura la idempotencia eliminando los registros del `source_month` y `service_type` correspondientes antes de insertar nuevos datos en la capa Bronze.
*   **`[ INSERTA NOMBRE DE TU PIPELINE DE PARTICIONES SI APLICA ]`**:[ EJ: Script de Python/SQL ejecutado desde Mage que dropea y recrea la estructura DDL de particionamiento (RANGE, HASH, LIST) en la capa Gold previo a la ejecución de dbt. ]
*   **`dbt_build_silver`**: Pipeline que ejecuta `dbt run --select silver` para crear/actualizar las vistas de limpieza y estandarización.
*   **`dbt_build_gold`**: Pipeline que ejecuta `dbt run --select gold` para poblar el modelo estrella particionado.
*   **`quality_checks`**: Pipeline que ejecuta `dbt test` para validar las llaves primarias, relaciones y valores aceptados definidos en el proyecto.

---

## 5. Nombres de triggers y qué disparan

*   **`ingest_monthly` (Schedule trigger):** Configurado para ejecutarse de forma[ INSERTA FRECUENCIA: diaria / semanal / mensual ], disparando el pipeline `ingest_bronze` para mantener los datos actualizados.
*   **`dbt_after_ingest` (Event trigger / Pipeline chaining):** Disparador basado en eventos que inicia automáticamente cuando `ingest_bronze` termina exitosamente. Ejecuta en cadena y en el siguiente orden:
    1. `dbt_build_silver`
    2. Scripts SQL de particionamiento
    3. `dbt_build_gold`
    4. `quality_checks`

---

## 6. Gestión de Secretos

Todas las credenciales y parámetros de conexión se manejan a través de **Mage Secrets** y variables en el archivo `.env`. El archivo `.env` real está excluido del repositorio mediante `.gitignore`.

**Lista de secretos configurados:**
*   `POSTGRES_DB`: Nombre de la base de datos de destino.
*   `POSTGRES_USER`: Usuario administrador de la base de datos.
*   `POSTGRES_PASSWORD`: Contraseña del usuario de base de datos.
*   `POSTGRES_HOST`: Host de conexión (ej. el nombre del contenedor de BD).
*   `POSTGRES_PORT`: Puerto de conexión a PostgreSQL.

---

## 7. Particionamiento

Se implementó particionamiento declarativo nativo en PostgreSQL en la capa Gold.

### Evidencias `\d+`

**Hechos - RANGE (`gold.fct_trips`):**
```text[ INSERTA AQUÍ EL OUTPUT DE LA CONSOLA AL EJECUTAR \d+ analytics_gold.mart_fct_trips ][ Asegúrate de que se vea la línea "Partition key: RANGE (pickup_date)" y la lista de particiones mensuales ]
```

**Dimensiones - HASH (`gold.dim_zone`):**
```text[ INSERTA AQUÍ EL OUTPUT DE \d+ analytics_gold.mart_dim_zone ][ Debe mostrar "Partition key: HASH (zone_key)" y las 4 particiones ]
```

**Dimensiones - LIST (`gold.dim_service_type` y `gold.dim_payment_type`):**
```text[ INSERTA AQUÍ EL OUTPUT DE \d+ PARA AMBAS TABLAS MOSTRANDO "Partition key: LIST (...)" ]
```

### Evidencias de Partition Pruning (`EXPLAIN ANALYZE`)

**1. Filtro por mes en tabla de hechos (`fct_trips`):**
```sql[ INSERTA AQUÍ EL QUERY DE EJEMPLO. EJ: EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM analytics_gold.mart_fct_trips WHERE pickup_date BETWEEN '2024-01-01' AND '2024-01-31'; ]
```
```text[ INSERTA AQUÍ EL OUTPUT DEL EXPLAIN ]
```

**2. Búsqueda por `zone_key` en dimensión (`dim_zone`):**
```sql[ INSERTA AQUÍ EL QUERY DE EJEMPLO. EJ: EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM analytics_gold.mart_dim_zone WHERE zone_key = 132; ]
```
```text[ INSERTA AQUÍ EL OUTPUT DEL EXPLAIN ]
```

**Interpretación del Pruning (2-4 líneas):**[ INSERTA AQUÍ TU EXPLICACIÓN. Ejemplo: El plan de ejecución demuestra el partition pruning ya que, en lugar de realizar un Seq Scan sobre toda la tabla, el motor de Postgres escanea exclusivamente la partición correspondiente (ej. `mart_fct_trips_2024_01` o `mart_dim_zone_p0`), descartando las demás. Esto se evidencia en la reducción dramática de los 'Buffers' leídos y el tiempo total de ejecución. ]

---

## 8. dbt

### Materializations
*   **Capa Silver:** Materializada obligatoriamente como `view`. Definido en `dbt_project.yml` bajo la carpeta silver.
*   **Capa Gold:** Materializada como `table`. Para las tablas particionadas (hechos y dimensiones list/hash), se utilizó una estrategia de materialización híbrida en el config del modelo (`materialized='incremental', incremental_strategy='append'`) en conjunto con un `pre_hook="truncate table {{ this }};"` para respetar la estructura de particiones DDL creada previamente desde Mage. Las dimensiones regulares (como `dim_date`) se materializaron como `table` estándar.

### Logs de Ejecución (`dbt run` y `dbt test`)
```text[ INSERTA AQUÍ CAPTURAS O SNIPPETS DE TEXTO MOSTRANDO EL PASSING DE DBT RUN Y DBT TEST DESDE MAGE ]
```

---

## 9. Troubleshooting (Problemas comunes y soluciones)

Durante el desarrollo de este PSet, se identificaron y resolvieron los siguientes problemas técnicos:

1.  **Problema:** Error `relation "analytics_gold.mart_dim_date" does not exist` al correr dbt por primera vez.
    *   **Causa:** Se configuró un `pre_hook` con `TRUNCATE` a nivel global en la capa gold (en `dbt_project.yml`) pensado para las tablas particionadas. Al intentar correr modelos regulares como `dim_date` que aún no existían, el truncate fallaba.
    *   **Solución:** Se retiró el hook global del `dbt_project.yml` dejándolo como materialización `table` por defecto, y se movió el bloque `{{ config(...) }}` con el truncate exclusivamente a los archivos `.sql` de las tablas que sí son pre-particionadas por script (como `fct_trips`).

2.  **Problema:** Tiempos de ejecución extremadamente altos (minutos) al procesar dimensiones estáticas (como `dim_vendor` o `dim_payment_type`).
    *   **Causa:** Extraer los IDs únicos escaneando cientos de millones de filas de la capa Silver (usando `SELECT DISTINCT` sobre una vista) generaba un cuello de botella innecesario en la base de datos para recuperar dominios fijos.
    *   **Solución:** Se reemplazó el escaneo de la tabla de hechos por una tabla de dominio (Domain Table) con valores hardcodeados utilizando `UNION ALL` directamente en dbt. El tiempo de ejecución bajó a milisegundos.

3.  **Problema:** Error `KeyError: 'DEFAULT'` al ejecutar consultas SQL en el Notebook Jupyter.
    *   **Causa:** Incompatibilidad de la librería legacy `ipython-sql` con las versiones modernas de `prettytable` (>3.0) al intentar renderizar el output de los queries.
    *   **Solución:** Se reemplazó la librería antigua migrando a `jupysql` (instalando vía `!pip install jupysql`) y reiniciando el kernel. Alternativamente, se activó `%config SqlMagic.autopandas = True` para forzar la salida directa a DataFrames de Pandas, eludiendo la visualización en texto plano.

---

## 10. Checklist de aceptación

- [x] Docker Compose levanta Postgres + Mage
- [x] Credenciales en Mage Secrets y `.env` (solo `.env.example` en repo)
- [x] Pipeline `ingest_bronze` mensual e idempotente + tabla de cobertura
- [x] dbt corre dentro de Mage: `dbt_build_silver`, `dbt_build_gold`, `quality_checks`
- [x] Silver materialized = views; Gold materialized = tables
- [x] Gold tiene esquema estrella completo
- [x] Particionamiento: RANGE en `fct_trips`, HASH en `dim_zone`, LIST en `dim_service_type` y `dim_payment_type`
- [x] README incluye `\d+` y `EXPLAIN (ANALYZE, BUFFERS)` con pruning
- [x] `dbt test` pasa desde Mage
- [x] Notebook responde 20 preguntas usando solo `gold.*`
- [x] Triggers configurados y evidenciados