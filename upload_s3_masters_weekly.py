"""DAG de ingesta semanal: archivos maestros on-premise -> AWS S3.

Mismo patrón que `upload_s3_transactional_daily.py`, con dos diferencias
deliberadas:

  1. Cadencia semanal en vez de diaria.
  2. `ERASE_SOURCE = False` — no se borra el archivo local tras subirlo.

La segunda es la importante y está explicada en detalle en el README, sección
"El parámetro erase: por qué no es una constante global".
"""

import os
import yaml

from pytz import timezone
from pathlib import Path
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException

from include.uploadfunctions.functions import create_list
from include.uploadfunctions.ioupload import app_upload


doc = """
## Ingesta semanal de archivos maestros a S3

Envía a S3 los archivos maestros que la capa de extracción deja en la carpeta
de staging local. Se ejecuta con cadencia semanal porque los maestros se
actualizan por vigencias, no a diario.

### Programación
Semanal, domingos. La hora se define en la constante `SCHEDULE`.

### Tareas
1. **extract_list** — construye el listado de archivos a subir.
2. **send_s3_file** — sube los archivos a S3, **sin borrar el origen**.
3. **validate_upload** — verifica los objetos recién subidos y escribe el
   manifiesto de auditoría.

### Por qué este DAG NO borra el origen
Un maestro es un archivo de referencia, no un incremento diario. Si se borra
tras subirlo y el consumidor retira además el objeto de S3, no queda ninguna
copia: ni para verificar la entrega, ni para reprocesar. Con cadencia semanal,
el hueco de un fallo silencioso dura siete días.

La contrapartida es que la carpeta local crece. Se controla con una política
de retención por antigüedad, no borrando en cada carga.
"""

# ── Configuración (valores reales fuera del repositorio) ────────────────
YAML_ROUTE = os.environ.get('PIPELINE_CONFIG_DIR', '/opt/pipeline/config/')
YAML_FILE = 'upload_s3_masters.yaml'
BUCKET_NAME = os.environ.get('S3_BUCKET', '<bucket-destino>')
COMMON_PATH = os.environ.get('STAGING_DIR', '/opt/pipeline/staging/')

DIRECTORY = 'carpetas'
ENV = 'prd'
SCHEDULE = '0 10 * * 0'          # Domingos, 10:00 a. m. hora local
TZ = timezone('America/Bogota')

AUDIT_PREFIX = 'auditoria/maestros'

# Los maestros son archivos de referencia: se conserva la copia local.
# Ver el README antes de cambiar este valor.
ERASE_SOURCE = False

# Días que se conservan en la carpeta local antes de la limpieza por retención.
RETENTION_DAYS = 30


def load_config():
    """Carga el YAML de configuración y falla de forma explícita si no existe."""
    yaml_file = Path(os.path.join(YAML_ROUTE, YAML_FILE))
    if not yaml_file.exists():
        raise AirflowFailException(
            f'No se encontró el archivo de configuración: {yaml_file}'
        )
    with open(yaml_file, 'r') as config_file:
        return yaml.safe_load(config_file)


def basename_of(item):
    """Nombre del archivo, sin importar el formato que devuelva create_list."""
    if isinstance(item, dict):
        item = item.get('path') or item.get('source') or next(iter(item.values()))
    elif isinstance(item, (tuple, list)):
        item = item[0]
    return os.path.basename(str(item))


@dag(
    dag_id='upload_s3_masters_weekly',
    dag_display_name='Ingesta semanal — maestros a S3',
    doc_md=doc,
    description='Carga semanal de archivos maestros desde on-premise a S3',
    start_date=datetime(2026, 1, 1, 5, 0, tzinfo=TZ),
    schedule=SCHEDULE,
    catchup=False,
)
def upload_s3_masters():

    config = load_config()
    paths: dict = config[DIRECTORY]
    secrets: dict = config['ambiente'][ENV]
    setup: dict = config['configuracion_destino']

    for source in paths.values():
        group_id = 'route-' + str(source).replace('/', '-').lower()

        with TaskGroup(group_id=group_id):

            @task.python(task_id='extract_list')
            def extract_list(source, common_path):
                return create_list(source, common_path=common_path)

            @task.python(task_id='send_s3_file')
            def upload_s3(path_list, key_id, secret, bucket_name):
                app_upload(
                    path_list, key_id, secret, bucket_name, prd=True,
                    configuracion_destino=setup, erase=ERASE_SOURCE,
                )
                return [basename_of(p) for p in path_list]

            @task.python(task_id='validate_upload')
            def validate_upload(names, key_id, secret, bucket_name, source):
                from include.uploadfunctions.audit import verify_and_record
                return verify_and_record(
                    names, key_id, secret, bucket_name,
                    prefix=str(source).strip('/') + '/',
                    audit_prefix=AUDIT_PREFIX,
                    staging_dir=COMMON_PATH,
                    tz=TZ,
                )

            @task.python(task_id='apply_retention')
            def apply_retention(_upstream, common_path, source):
                """Limpieza por antigüedad, en reemplazo del borrado inmediato.

                Se ejecuta DESPUÉS de la validación: si la entrega no se pudo
                confirmar, no se toca ningún archivo local.
                """
                from include.uploadfunctions.retention import purge_older_than
                removed = purge_older_than(
                    os.path.join(common_path, str(source)),
                    days=RETENTION_DAYS,
                )
                print(f'Retención aplicada: {removed} archivo(s) retirados.')
                return removed

            file_list = extract_list(source, common_path=COMMON_PATH)
            uploaded = upload_s3(
                file_list, secrets['key_id'], secrets['secret'], BUCKET_NAME
            )
            validated = validate_upload(
                uploaded, secrets['key_id'], secrets['secret'],
                BUCKET_NAME, source
            )
            apply_retention(validated, COMMON_PATH, source)


upload_s3_masters()
