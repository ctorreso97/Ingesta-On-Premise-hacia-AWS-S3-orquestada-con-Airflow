# Ingesta on-premise → AWS S3 orquestada con Airflow

> ⚠️ Repositorio **anonimizado y educativo**, derivado de un pipeline real en producción. No contiene nombres de empresa, credenciales, rutas reales, nombres de bucket ni datos de negocio. Rutas y destinos son placeholders configurables por variable de entorno.

Patrón de ingesta de archivos desde un entorno on-premise hacia S3, con un DAG de Airflow por dominio de datos, validación de entrega y política de borrado diferenciada según el tipo de archivo.

![Arquitectura del pipeline](docs/arquitectura.png)

## El problema

Una capa de extracción on-premise genera archivos en una carpeta local. Esos archivos deben llegar a S3 para ser procesados por un job de transformación y, en paralelo, entregados a un tercero por SFTP.

La particularidad que define el diseño: **el proceso consumidor retira los objetos del bucket poco después de la carga**. Eso rompe dos supuestos que uno suele dar por sentados.

## Aprendizaje 1 — Verificar una entrega que ya no está

Si el consumidor se lleva los objetos, revisar el prefijo de destino más tarde no prueba nada. Un prefijo vacío significa las dos cosas a la vez:

- el archivo nunca se subió, o
- se subió correctamente y el consumidor ya se lo llevó.

No hay forma de distinguirlas *a posteriori*. La verificación tiene que ocurrir **en el momento de la carga**, y su resultado tiene que persistirse en un lugar que el consumidor no toque.

La tarea `validate_upload` lista el prefijo destino inmediatamente después de subir, confirma cada archivo esperado, y escribe un manifiesto JSON en un prefijo de auditoría separado:

```json
{
  "ejecucion": "2026-01-11T10:00:14-05:00",
  "prefijo_destino": "<dominio>/",
  "esperados": 4,
  "confirmados": 4,
  "faltantes": [],
  "archivos": [
    { "archivo": "maestro_a.csv", "bytes": 184320, "modificado": "..." }
  ]
}
```

Detalle que suele pasarse por alto: un objeto de **0 bytes se sube sin error**. La validación lo trata como fallo, porque para el consumidor es equivalente a que no haya llegado.

## Aprendizaje 2 — El parámetro `erase` no es una constante global

La función de carga acepta un parámetro `erase` que borra el archivo local después de subirlo. En el DAG original venía en `True`, y funcionaba bien: los archivos transaccionales se regeneran cada día, así que borrarlos evita reenviar lo mismo dos veces.

Al replicar el patrón para los **archivos maestros**, ese mismo valor se volvió un problema. La diferencia no es de implementación, es de naturaleza del dato:

| | Transaccional | Maestro |
|---|---|---|
| Naturaleza | Incremento de un período | Archivo de referencia |
| Se regenera | Cada día | Por vigencia, irregular |
| Cadencia de carga | Diaria | Semanal |
| Si se pierde | Se regenera mañana | Hay que reconstruirlo |
| `erase` | `True` | **`False`** |

Con `erase=True` en un maestro, y sabiendo que el consumidor retira el objeto de S3, se llega a un estado donde **no queda ninguna copia**: ni local para verificar o reprocesar, ni en S3 para consultar. Y con cadencia semanal, el hueco que abre un fallo silencioso dura siete días, no uno.

La contrapartida real de `erase=False` es que la carpeta local crece sin control y `create_list` termina reenviando archivos viejos en cada corrida. Por eso no basta con apagar el flag: hay que reemplazarlo por una **política de retención por antigüedad** (`include/uploadfunctions/retention.py`), que corre *después* de que la entrega fue validada.

```python
# Transaccionales: el archivo de mañana reemplaza al de hoy
ERASE_SOURCE = True

# Maestros: se conserva la copia local, con retención por antigüedad
ERASE_SOURCE = False
RETENTION_DAYS = 30
```

El orden de las tareas importa: `upload → validate → retention`. Si la validación falla, no se toca ningún archivo local.

## Aprendizaje 3 — Zonas horarias entre orquestadores

Airflow y los *triggers* nativos de AWS Glue no interpretan el cron igual:

- **Airflow** lo evalúa en la zona horaria del `start_date`. Con `tzinfo=America/Bogota`, `0 10 * * 0` dispara a las 10:00 a. m. hora local.
- **Glue** lo evalúa siempre en UTC, sin parámetro de zona horaria. Las mismas 10:00 a. m. locales se escriben `cron(0 15 * * ? *)`.

Escribir la hora local en un trigger de Glue lo deja corriendo cinco horas corridas — un error que no falla ni alerta, sólo entrega tarde.

## Estructura

```
.
├── dags/
│   ├── upload_s3_transactional_daily.py   # diario · erase=True
│   └── upload_s3_masters_weekly.py        # semanal · erase=False + retención
├── include/uploadfunctions/
│   ├── audit.py                           # validación + manifiesto
│   └── retention.py                       # limpieza por antigüedad
├── docs/
│   └── arquitectura.png
└── README.md
```

## Decisiones de diseño

**Un DAG por dominio de datos.** Un solo DAG recorriendo todas las carpetas es más corto, pero acopla el destino de todos los dominios: un fallo en uno bloquea a los demás, y no se puede reprocesar uno solo ni darle cadencia propia. Separarlos permitió que los maestros pasaran a semanal sin tocar el resto.

**Fallo explícito ante configuración faltante.** En la versión original, las variables del YAML se asignaban dentro de un `if path.exists()`, pero se consumían fuera. Si el archivo no estaba, el DAG reventaba con un `NameError` a varias líneas de distancia de la causa real. Ahora se levanta un `AirflowFailException` con el path que se buscó.

**Secretos fuera del repositorio.** Las claves del YAML no son credenciales en claro sino nombres de secretos que se resuelven en tiempo de ejecución contra el gestor de secretos.

## Configuración

Variables de entorno esperadas:

| Variable | Descripción |
|---|---|
| `PIPELINE_CONFIG_DIR` | Carpeta que contiene los YAML de configuración |
| `STAGING_DIR` | Carpeta local desde donde se leen los archivos |
| `S3_BUCKET` | Bucket destino |

Estructura del YAML de configuración:

```yaml
carpetas:
  ruta_1: <nombre-carpeta-origen>
ambiente:
  prd:
    key_id: <nombre-del-secreto>
    secret: <nombre-del-secreto>
configuracion_destino:
  <nombre-carpeta-origen>: <prefijo/destino/en/s3>
```

## Stack

`Apache Airflow` · `Python` · `boto3` · `Amazon S3` · `AWS Glue` (consumidor downstream)

---

*Parte de mi portafolio de transición hacia ingeniería de datos / machine learning.*
