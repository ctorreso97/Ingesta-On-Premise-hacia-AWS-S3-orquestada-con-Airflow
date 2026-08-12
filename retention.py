"""Retención por antigüedad de la carpeta de staging local.

Alternativa al borrado inmediato tras la carga (`erase=True`). Permite que el
archivo de origen sobreviva a la ejecución —quedando disponible para verificar
o reprocesar— sin que la carpeta crezca de forma indefinida.
"""

import os
import time
from pathlib import Path


def purge_older_than(directory, days=30, dry_run=False):
    """Elimina los archivos de `directory` con más de `days` días de antigüedad.

    Args:
        directory: carpeta a depurar. No recorre subcarpetas.
        days: antigüedad mínima, en días, para que un archivo sea retirado.
        dry_run: si es True, sólo registra en el log sin borrar nada.

    Returns:
        int: cantidad de archivos retirados (o que se habrían retirado).
    """
    path = Path(directory)
    if not path.is_dir():
        print(f'La carpeta no existe, no hay nada que depurar: {directory}')
        return 0

    cutoff = time.time() - (days * 86400)
    removed = 0

    for item in path.iterdir():
        if not item.is_file():
            continue
        if item.stat().st_mtime >= cutoff:
            continue
        if dry_run:
            print(f'   [simulación] se retiraría: {item.name}')
        else:
            os.remove(item)
            print(f'   retirado: {item.name}')
        removed += 1

    return removed
