# Parte B — Streaming ETL con Apache Kafka

La lógica se concentra en **dos archivos Python**: [producer.py](producer.py) y [consumer.py](consumer.py). Docker Compose configura Kafka; el consumidor se conecta a tu PostgreSQL local; [streaming.sql](../sql/streaming.sql) define el staging y el modelo dimensional. Las dependencias se encuentran en el [requirements.txt principal](../requirements.txt).

## Paso 5 — Productor

`producer.py` contiene la lectura del CSV, creación y comprobación del tópico, construcción del evento y envío a Kafka. Sus comentarios identifican cada requisito del ejercicio.

| Requisito | Implementación en `producer.py` |
| --- | --- |
| Histórico preparado | `data/processed/happiness_modeling.csv`. |
| Lectura registro a registro | `csv.DictReader` e iteración con `iter_events`. |
| Serialización JSON | `json.dumps(..., allow_nan=False)` y codificación UTF-8. |
| Clave por país | `key=event["country"].encode("utf-8")`. |
| Llegada simulada | `time.sleep(delay)`; demora configurable mediante `--delay`. |
| Nombre obligatorio | `TOPIC = "happiness-events"`. |
| Tres particiones, replicación uno | `NewTopic(..., num_partitions=3, replication_factor=1)` y comprobación del tópico existente. |
| Identificador trazable | UUID por ejecución y número de registro; evita repetir identificadores al volver a publicar el histórico. |
| Hora de emisión | `datetime.now(timezone.utc).isoformat(...)`. |

El productor espera la confirmación de cada registro antes de continuar. Si la entrega falla, informa el error. La clave dirige los eventos de un país a la misma partición; no existe un orden global entre particiones.

### Contrato mínimo

```json
{
  "event_id": "E001",
  "country": "Colombia",
  "year": 2019,
  "gdp": 1.2,
  "family": 0.8,
  "health": 0.9,
  "freedom": 0.6,
  "generosity": 0.3,
  "corruption": 0.1,
  "actual_happiness_score": 6.2,
  "event_time": "2026-10-30T15:30:00"
}
```

`event_id` y `country` son strings no vacíos; el identificador es único. `year` es el entero de la observación histórica. Los seis indicadores son numéricos y proceden del esquema de entrenamiento. `actual_happiness_score` se utiliza únicamente para evaluación. `event_time` representa la emisión en ISO-8601; el productor utiliza UTC con offset. Kafka transporta el mensaje; `validate_event`, dentro de `consumer.py`, comprueba los campos de negocio.

| Campo del evento | Columna del modelo, en orden de entrada |
| --- | --- |
| `gdp` | `gdp_per_capita` |
| `family` | `social_support` |
| `health` | `healthy_life_expectancy` |
| `freedom` | `freedom` |
| `generosity` | `generosity` |
| `corruption` | `corruption` |

## Paso 6 — Consumidor

`consumer.py` contiene validación, conexión a PostgreSQL, almacenamiento del mensaje original, inferencia y carga de dimensiones y hechos. Utiliza el grupo **`happiness-ml-team`** y carga una sola vez `models/model.pkl`.

1. Guarda y confirma los bytes originales en `raw_happiness_events`, antes de interpretar el JSON.
2. Valida campos obligatorios, tipos, strings, año histórico, valores numéricos finitos y compatibles con el modelo, puntaje real, fecha y clave Kafka.
3. Construye las seis columnas en el orden del entrenamiento y llama al pipeline existente. El puntaje real, país, año e identificadores no entran al modelo.
4. Resuelve el país en `dim_country`, inserta el día UTC de emisión en `dim_date` y guarda el resultado en `fact_predictions`. El error se almacena como valor real menos predicho.
5. Confirma el offset después de persistir el resultado o el rechazo.

Los inválidos conservan el original y un motivo, con estado `INVALID_SCHEMA`, `INVALID_VALUES` o `PREDICTION_ERROR`, sin detener los registros posteriores. `RECEIVED` indica pendiente y `VALID` indica predicción guardada. La clave única de posición Kafka evita repetir una predicción tras una redelivery; un índice único parcial y un bloqueo transaccional controlan identificadores repetidos, sin otra tabla técnica. Un fallo de escritura de la base detiene el proceso sin confirmar ese offset.

La base es **`happiness_dw` en PostgreSQL**, accesible en `localhost:5432`. El servidor local conserva sus tablas y datos; la base del proyecto es `happiness_dw` y el usuario es `postgres`. Los 165 países y sus regiones se cargan desde el dataset analítico preparado. Los errores sobre el histórico simulado no son una nueva evaluación independiente: `model.pkl` fue reentrenado con 2015–2019.

## Ejecución desde la raíz del proyecto

Con Docker Desktop iniciado, PostgreSQL local activo y el `.env` completado según las [instrucciones principales](../README.md#16-instrucciones-de-ejecución):

```powershell
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe kafka/consumer.py --init-db
docker compose up -d
```

Terminal 1 — consumidor:

```powershell
.venv\Scripts\python.exe kafka/consumer.py
```

Terminal 2 — productor:

```powershell
.venv\Scripts\python.exe kafka/producer.py --delay 0.1
```

`--data` permite señalar otro CSV preparado. El consumidor admite `--database-url` o la variable de entorno `DATABASE_URL` para indicar la conexión PostgreSQL. Ambos programas también pueden ejecutarse como módulos con `python -m kafka.producer` y `python -m kafka.consumer`. Detener los procesos Python con `Ctrl+C` y el entorno con `docker compose down`, que conserva el volumen Kafka. El servidor PostgreSQL local se administra independientemente de Docker.

La conexión principal utiliza el usuario `postgres` y la base `happiness_dw`. La contraseña se configura en `PGPASSWORD` dentro del `.env` de la raíz. Este archivo también centraliza host, puerto, base, usuario, broker, tópico, grupo e intervalo del productor. Se carga automáticamente y no se versiona. `--init-db` crea la base si falta y ejecuta `sql/streaming.sql` para preparar tablas y vistas, sin iniciar el consumidor:

```text
postgresql://postgres@localhost:5432/happiness_dw
```

Consultar el almacén desde el cliente local:

```powershell
& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -h localhost -p 5432 -U postgres -d happiness_dw -c 'SELECT * FROM v_event_quality_daily;'
```

Consultas SQL:

```sql
SELECT processing_status, COUNT(*)
FROM raw_happiness_events
GROUP BY processing_status;

SELECT country_name, observation_year, actual_score, predicted_score, prediction_error
FROM v_prediction_detail
ORDER BY prediction_id DESC LIMIT 10;
```

Las pruebas se concentran en un archivo y utilizan un esquema temporal aislado en PostgreSQL. Con el servicio activo:

```powershell
$env:RUN_POSTGRES_TESTS = '1'
.venv\Scripts\python.exe -m unittest discover -s tests -v
Remove-Item Env:RUN_POSTGRES_TESTS
```

## Parte C — Almacén analítico

**Cada fila en `fact_predictions` corresponde a una predicción generada para un evento válido de un país y año histórico, con la versión del modelo utilizada.** Una nueva simulación genera eventos nuevos; país y año histórico no constituyen la clave única del hecho.

El [diagrama dimensional completo](../README.md#122-modelo-dimensional) especifica los campos, claves primarias, claves foráneas y cardinalidades de las cuatro tablas.

`date_id` identifica el día de `event_time` en UTC; `observation_year` conserva el año del dataset. Las fechas sin zona se interpretan como UTC, conforme a la convención del ejercicio. `prediction_timestamp` registra cuándo se produjo la predicción. Los timestamps se almacenan como `TIMESTAMPTZ`.

Las vistas `v_event_quality_daily`, `v_prediction_metrics`, `v_prediction_activity_daily` y `v_country_error_ranking` responden a los cinco requerimientos analíticos aprobados. `v_prediction_detail` permite consultar el detalle y reconstruir la trazabilidad. `v_prediction_metrics` incluye niveles país–año, región–año y total por año: los nombres de país y región nulos identifican los niveles agregados. Debe filtrarse el nivel deseado. R² es nulo para grupos con menos de dos observaciones o sin variación en el valor real; RMSE se calcula a partir de los errores individuales, no promediando RMSE de subgrupos.

El [informe de verificación](../data/reports/streaming/verification.json) registra la recuperación del histórico Kafka en PostgreSQL y las comprobaciones del almacén. El [README principal](../README.md) describe los cinco requerimientos y el modelo estrella.

El servicio de base de Docker queda bajo el perfil opcional `database`, desactivado en la ejecución principal. No debe activarse mientras tu servidor local utilice el puerto 5432. Los datos del volumen de demostración se conservan; cambiar el destino de conexión no los migra automáticamente.
