"""
Azure Function HTTP Trigger para generar reportes HTML de tachadas de secado.

Recibe POST desde Google Apps Script con:
- planta: Código de planta (JPV o RB)
"""

import json
import logging

import azure.functions as func

from shared_code.gdrive_client import GoogleDriveClient
from shared_code.reporte_builder import generar_reporte

logger = logging.getLogger(__name__)


def _json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    """Helper para crear respuestas JSON con el formato esperado."""
    return func.HttpResponse(
        body=json.dumps(payload, ensure_ascii=False),
        status_code=status_code,
        mimetype="application/json",
    )


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function HTTP Trigger para generar reportes HTML.
    
    Recibe POST con:
    - planta: Código de planta (JPV o RB)
    """
    try:
        # Validar método POST
        if req.method != "POST":
            return _json_response({
                "status": "error",
                "detalle": "Método no permitido. Se requiere POST."
            }, 405)

        # Parse JSON body
        try:
            data = req.get_json()
        except ValueError:
            return _json_response({
                "status": "error",
                "detalle": "Invalid JSON body"
            }, 400)

        # Leer parámetro 'planta'
        planta = data.get("planta")
        if not planta:
            return _json_response({
                "status": "error",
                "detalle": "Falta parámetro 'planta' en el body"
            }, 400)

        # Validar que planta sea JPV o RB
        planta = planta.strip().upper()
        if planta not in ["JPV", "RB"]:
            return _json_response({
                "status": "error",
                "detalle": f"Planta inválida: '{planta}'. Debe ser 'JPV' o 'RB'"
            }, 400)

        logger.info(f"[Reporte] Iniciando generación de reporte para planta: {planta}")

        # Crear cliente de Google Drive
        try:
            gdrive_client = GoogleDriveClient()
        except Exception as exc:
            logger.exception(f"[Reporte] Error inicializando GoogleDriveClient: {exc}")
            return _json_response({
                "status": "error",
                "detalle": f"Error inicializando cliente de Google Drive: {str(exc)}"
            }, 500)

        # Ejecutar generación de reporte
        try:
            resultado = generar_reporte(gdrive_client, planta)
            
            if resultado["success"]:
                return _json_response({
                    "status": "ok",
                    "filas": resultado["filas"],
                    "html": resultado["html"],
                    "mensaje": resultado["mensaje"]
                }, 200)
            else:
                return _json_response({
                    "status": "error",
                    "detalle": resultado["mensaje"],
                    "filas": resultado["filas"],
                    "html": resultado.get("html")
                }, 500)

        except Exception as exc:
            logger.exception(f"[Reporte] Error durante generación de reporte: {exc}")
            return _json_response({
                "status": "error",
                "detalle": str(exc)
            }, 500)

    except Exception as exc:
        logger.exception(f"[Reporte] Error no manejado: {exc}")
        return _json_response({
            "status": "error",
            "detalle": f"Error no manejado: {str(exc)}"
        }, 500)

