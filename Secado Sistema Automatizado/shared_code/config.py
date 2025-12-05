"""
Configuración de folder IDs de Google Drive por planta.

Cada carpeta debe estar compartida con la Service Account:
- Carpetas de laboratorio: Permisos de Lector
- Carpetas de salida (processed): Permisos de Editor
"""

import os
import logging

logger = logging.getLogger(__name__)


# Mapeo de plantas a folder IDs de carpetas de laboratorio
LAB_FOLDERS = {
    "JPV": os.environ.get("LAB_FOLDER_JPV"),
    "RB": os.environ.get("LAB_FOLDER_RB"),
}

# Mapeo de plantas a folder IDs de carpetas de salida (processed)
PROCESSED_FOLDERS = {
    "JPV": os.environ.get("PROCESSED_FOLDER_JPV"),
    "RB": os.environ.get("PROCESSED_FOLDER_RB"),
}

# Mapeo de plantas a folder IDs de carpetas de archivos validados
# NOTA: Estos folder IDs deben configurarse como variables de entorno en Azure Function App Settings.
# Valores originales del script compilado_post_ml.py (para referencia):
#   JPV: "1JbzvdmUiK_qAEHvfFK7g4dyVU2j7JwB9"
#   RB:  "11q2vW9Fk8qYz5MIcpmmxmNhc0PiWYlaY"
VALIDATED_FOLDERS = {
    "JPV": os.environ.get("VALIDATED_FOLDER_JPV"),
    "RB": os.environ.get("VALIDATED_FOLDER_RB"),
}

# Mapeo de plantas a folder IDs de carpetas de reportes
# NOTA: Estos folder IDs deben configurarse como variables de entorno en Azure Function App Settings.
# Valores originales del script reporte.py (para referencia):
#   JPV: "1CP6KsGkIHq5l0WrN7KMx-RK4ip_AXz4k"
#   RB:  "181dqjsFvdu6pls_LLMcRD3J5PU-5eBR1"
REPORTS_FOLDERS = {
    "JPV": os.environ.get("REPORTS_FOLDER_JPV"),
    "RB": os.environ.get("REPORTS_FOLDER_RB"),
}


def get_lab_folder_id(planta: str) -> str:
    """
    Obtiene el folder_id de la carpeta de archivos de laboratorio para una planta.

    Args:
        planta: Código de planta (JPV o RB)

    Returns:
        folder_id de la carpeta de laboratorio

    Raises:
        ValueError: Si no existe configuración para la planta
    """
    planta_upper = planta.upper()
    folder_id = LAB_FOLDERS.get(planta_upper)

    if not folder_id:
        available = [p for p, fid in LAB_FOLDERS.items() if fid]
        logger.error(
            f"[Config] No hay folder_id de laboratorio para '{planta}'. "
            f"Variable requerida: LAB_FOLDER_{planta_upper}. "
            f"Plantas disponibles: {available}"
        )
        raise ValueError(
            f"No existe configuración de carpeta de laboratorio para '{planta}'. "
            f"Por favor configura la variable de entorno 'LAB_FOLDER_{planta_upper}' "
            f"en Azure Function App Settings."
        )

    logger.info(f"[Config] Carpeta de laboratorio para {planta}: {folder_id}")
    return folder_id


def get_processed_folder_id(planta: str) -> str:
    """
    Obtiene el folder_id de la carpeta de archivos procesados para una planta.

    Args:
        planta: Código de planta (JPV o RB)

    Returns:
        folder_id de la carpeta de salida

    Raises:
        ValueError: Si no existe configuración para la planta
    """
    planta_upper = planta.upper()
    folder_id = PROCESSED_FOLDERS.get(planta_upper)

    if not folder_id:
        available = [p for p, fid in PROCESSED_FOLDERS.items() if fid]
        logger.error(
            f"[Config] No hay folder_id de salida para '{planta}'. "
            f"Variable requerida: PROCESSED_FOLDER_{planta_upper}. "
            f"Plantas disponibles: {available}"
        )
        raise ValueError(
            f"No existe configuración de carpeta de salida para '{planta}'. "
            f"Por favor configura la variable de entorno 'PROCESSED_FOLDER_{planta_upper}' "
            f"en Azure Function App Settings."
        )

    logger.info(f"[Config] Carpeta de salida para {planta}: {folder_id}")
    return folder_id


def get_validated_folder_id(planta: str) -> str:
    """
    Obtiene el folder_id de la carpeta de archivos validados para una planta.

    Args:
        planta: Código de planta (JPV o RB)

    Returns:
        folder_id de la carpeta de archivos validados

    Raises:
        ValueError: Si no existe configuración para la planta
    """
    planta_upper = planta.upper()
    folder_id = VALIDATED_FOLDERS.get(planta_upper)

    if not folder_id:
        available = [p for p, fid in VALIDATED_FOLDERS.items() if fid]
        logger.error(
            f"[Config] No hay folder_id de archivos validados para '{planta}'. "
            f"Variable requerida: VALIDATED_FOLDER_{planta_upper}. "
            f"Plantas disponibles: {available}"
        )
        raise ValueError(
            f"No existe configuración de carpeta de archivos validados para '{planta}'. "
            f"Por favor configura la variable de entorno 'VALIDATED_FOLDER_{planta_upper}' "
            f"en Azure Function App Settings."
        )

    logger.info(f"[Config] Carpeta de archivos validados para {planta}: {folder_id}")
    return folder_id


def get_reports_folder_id(planta: str) -> str:
    """
    Obtiene el folder_id de la carpeta de reportes para una planta.

    Args:
        planta: Código de planta (JPV o RB)

    Returns:
        folder_id de la carpeta de reportes

    Raises:
        ValueError: Si no existe configuración para la planta
    """
    planta_upper = planta.upper()
    folder_id = REPORTS_FOLDERS.get(planta_upper)

    if not folder_id:
        available = [p for p, fid in REPORTS_FOLDERS.items() if fid]
        logger.error(
            f"[Config] No hay folder_id de reportes para '{planta}'. "
            f"Variable requerida: REPORTS_FOLDER_{planta_upper}. "
            f"Plantas disponibles: {available}"
        )
        raise ValueError(
            f"No existe configuración de carpeta de reportes para '{planta}'. "
            f"Por favor configura la variable de entorno 'REPORTS_FOLDER_{planta_upper}' "
            f"en Azure Function App Settings."
        )

    logger.info(f"[Config] Carpeta de reportes para {planta}: {folder_id}")
    return folder_id


__all__ = ["get_lab_folder_id", "get_processed_folder_id", "get_validated_folder_id", "get_reports_folder_id"]

