"""Validación de la entrega a S3 y registro de evidencia.

Problema que resuelve: cuando un proceso consumidor retira los objetos del
bucket poco después de la carga, revisar el prefijo destino más tarde no
prueba nada — un prefijo vacío significa "nunca llegó" o "ya se lo llevaron",
y no hay forma de distinguirlos.

La verificación se hace inmediatamente después de la subida y su resultado se
persiste en un prefijo de auditoría que el consumidor no barre.
"""

import os
import json

import boto3
from airflow.exceptions import AirflowFailException


def verify_and_record(names, key_id, secret, bucket_name, prefix,
                      audit_prefix, staging_dir, tz):
    """Confirma en S3 los objetos recién subidos y escribe el manifiesto.

    Args:
        names: nombres de archivo que se esperaba subir.
        key_id, secret: credenciales resueltas desde el gestor de secretos.
        bucket_name: bucket destino.
        prefix: prefijo donde deberían haber quedado los objetos.
        audit_prefix: prefijo donde se escribe el manifiesto de evidencia.
        staging_dir: carpeta local de origen, sólo para trazabilidad.
        tz: zona horaria para el sello temporal.

    Returns:
        dict con el manifiesto de la ejecución.

    Raises:
        AirflowFailException: si algún archivo no se confirma o pesa 0 bytes.
    """
    from datetime import datetime

    if not names:
        print('No había archivos para subir en esta corrida.')
        return {'esperados': 0, 'confirmados': 0, 'faltantes': []}

    s3 = boto3.client(
        's3', aws_access_key_id=key_id, aws_secret_access_key=secret
    )

    # Una sola pasada de listado, en lugar de un head_object por archivo
    found = {}
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            found[os.path.basename(obj['Key'])] = {
                'key': obj['Key'],
                'bytes': obj['Size'],
                'modificado': obj['LastModified'].isoformat(),
            }

    detail, missing = [], []
    for name in names:
        if name not in found:
            missing.append(name)
            print(f'   NO ENCONTRADO en S3: {name}')
            continue
        info = found[name]
        detail.append({'archivo': name, **info})
        if info['bytes'] == 0:
            # Un objeto vacío se "sube" sin error pero no sirve al consumidor
            missing.append(f'{name} (0 bytes)')
            print(f'   VACÍO: {name}')
        else:
            print(f"   OK  {name} -> {info['bytes']} bytes")

    run_at = datetime.now(tz)
    manifest = {
        'ejecucion': run_at.isoformat(),
        'origen_local': staging_dir,
        'prefijo_destino': prefix,
        'esperados': len(names),
        'confirmados': len(detail),
        'faltantes': missing,
        'archivos': detail,
    }

    s3.put_object(
        Bucket=bucket_name,
        Key=f"{audit_prefix}/{run_at.strftime('%Y%m%d_%H%M%S')}.json",
        Body=json.dumps(manifest, indent=2, ensure_ascii=False).encode('utf-8'),
        ContentType='application/json',
    )

    print(f'Confirmados {len(detail)} de {len(names)} archivos.')

    if missing:
        raise AirflowFailException(
            f'Archivos no confirmados en S3: {", ".join(missing)}'
        )

    return manifest
