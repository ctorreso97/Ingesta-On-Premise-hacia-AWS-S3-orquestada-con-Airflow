"""DAG de ingesta diaria: archivos transaccionales on-premise -> AWS S3.

Patrón: un DAG por dominio de datos. Este cubre los archivos transaccionales,
que se regeneran cada día y se consumen una sola vez.

Para archivos maestros ver `upload_s3_weekly_masters.py`: mismo patrón, pero
con `erase=False`. La diferencia está documentada en el README.
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
## Ingesta diaria de archivos transaccionales a S3

Envía a S3 los archivos que la capa de extracción deja en la carpeta de
staging local. El mapeo de carpetas de origen y prefijos de destino vive en
un archivo YAML externo al repositorio.

### Programación
Diaria. La hora se define en la constante `SCHEDULE`.

### Tareas
1. **extract_list** — construye el listado de archivos a subir.
2. **send_s3_file** — sube los archivos a S3.
3. **validate_upload** — verifica los objetos recién subidos y escribe el
   manifiesto de auditoría.

### Nota sobre el borrado del origen
Este DAG usa `erase=True`: los archivos transaccionales se eliminan de la
carpeta local tras subirse, porque se regeneran cada día y volver a enviarlos
duplicaría la ingesta. Esta decisión NO aplica a los maestros.
"""

# ── Configuración (valores reales fuera del repositorio) ────────────────
YAML_ROUTE = os.environ.get('PIPELINE_CONFIG_DIR', '/opt/pipeline/config/')
YAML_FILE = 'upload_s3_transactional.yaml'
BUCKET_NAME = os.environ.get('S3_BUCKET', '<bucket-destino>')
COMMON_PATH = os.environ.get('STAGING_DIR', '/opt/pipeline/staging/')

DIRECTORY = 'carpetas'
ENV = 'prd'
SCHEDULE = '45 6 * * *'          # Diario, 6:45 a. m. hora local
TZ = timezone('America/Bogota')

# Prefijo de evidencia. Debe estar FUERA de la ruta que barre el consumidor.
AUDIT_PREFIX = 'auditoria/transaccionales'

# Los transaccionales se regeneran cada día: se borra el origen tras subir.
ERASE_SOURCE = True


def load_config():
    """Carga el YAML de configuración y falla de forma explícita si no existe.

    Sin este control, las variables quedarían indefinidas y el DAG reventaría
    más abajo con un NameError difícil de diagnosticar.
    """
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
    dag_id='upload_s3_transactional_daily',
    dag_display_name='Ingesta diaria — transaccionales a S3',
    doc_md=doc,
    description='Carga diaria de archivos transaccionales desde on-premise a S3',
    start_date=datetime(2026, 1, 1, 5, 0, tzinfo=TZ),
    schedule=SCHEDULE,
    catchup=False,
)
def upload_s3_transactional():

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

            file_list = extract_list(source, common_path=COMMON_PATH)
            uploaded = upload_s3(
                file_list, secrets['key_id'], secrets['secret'], BUCKET_NAME
            )
            validate_upload(
                uploaded, secrets['key_id'], secrets['secret'],
                BUCKET_NAME, source
            )


upload_s3_transactional()
