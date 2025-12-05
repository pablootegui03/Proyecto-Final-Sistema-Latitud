"""
Azure Function HTTP Trigger para ejecutar el modelo de ML.

Recibe desde Google Apps Script:
{
    "planta": "JPV",
    "archivo": "SENSOR20_processed_20251126T194042Z.csv"
}

Descarga el archivo desde la carpeta processed, ejecuta el pipeline ML,
y sube las predicciones a la carpeta validated.
"""

import json
import os
import sys

import azure.functions as func

# Ensure shared_code is importable when running in Functions context
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared_code.gdrive_client import GoogleDriveClient  # noqa: E402
from shared_code.ml_predictor import ejecutar_modelo_ml  # noqa: E402
from shared_code.minimal_logger import log, log_error  # noqa: E402


def _json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    """Helper para generar respuestas JSON."""
    return func.HttpResponse(
        json.dumps(data, ensure_ascii=False),
        status_code=status_code,
        mimetype="application/json"
    )


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function HTTP Trigger para ejecutar modelo ML.
    
    Recibe JSON con:
    - planta: Código de planta (JPV o RB)
    - archivo: Nombre del archivo procesado (ej: "SENSOR20_processed_20251126T194042Z.csv")
    """
    try:
        # Parse JSON body
        try:
            data = req.get_json()
        except ValueError:
            return _json_response({
                "success": False,
                "error": "Invalid JSON body",
                "details": "El body de la petición debe ser JSON válido"
            }, 400)
        
        if not data:
            return _json_response({
                "success": False,
                "error": "Empty request body",
                "details": "El body de la petición no puede estar vacío"
            }, 400)
        
        # Extraer parámetros
        planta = data.get("planta")
        archivo = data.get("archivo")
        
        log(f"ML Trigger recibido - planta={planta}, archivo={archivo}", "INFO", "ML_TRIGGER")
        
        # Validar campos requeridos
        if not planta:
            return _json_response({
                "success": False,
                "error": "Campo 'planta' requerido",
                "details": "Debe proporcionar el código de planta (JPV o RB)"
            }, 400)
        
        if not archivo:
            return _json_response({
                "success": False,
                "error": "Campo 'archivo' requerido",
                "details": "Debe proporcionar el nombre del archivo procesado"
            }, 400)
        
        # Validar que planta sea JPV o RB
        planta = planta.strip().upper()
        if planta not in ["JPV", "RB"]:
            return _json_response({
                "success": False,
                "error": "Planta inválida",
                "details": f"La planta debe ser 'JPV' o 'RB', recibido: '{planta}'"
            }, 400)
        
        # Inicializar cliente de Google Drive
        try:
            gdrive_client = GoogleDriveClient()
        except Exception as exc:
            log_error("ML_TRIGGER", exc, {"planta": planta, "archivo": archivo})
            return _json_response({
                "success": False,
                "error": "Error inicializando GoogleDriveClient",
                "details": str(exc)
            }, 500)
        
        # Ejecutar modelo ML
        try:
            resultado = ejecutar_modelo_ml(gdrive_client, planta, archivo)
            
            if resultado["success"]:
                return _json_response({
                    "success": True,
                    "planta": planta,
                    "archivo": archivo,
                    "filas_procesadas": resultado["filas"],
                    "archivo_output": resultado["nombre_output"],
                    "mensaje": resultado["mensaje"]
                }, 200)
            else:
                return _json_response({
                    "success": False,
                    "planta": planta,
                    "archivo": archivo,
                    "error": "Error en pipeline ML",
                    "details": resultado["mensaje"]
                }, 500)
                
        except Exception as exc:
            log_error("ML_TRIGGER", exc, {"planta": planta, "archivo": archivo})
            return _json_response({
                "success": False,
                "error": "Error ejecutando modelo ML",
                "details": str(exc)
            }, 500)
    
    except Exception as exc:
        log_error("ML_TRIGGER", exc)
        return _json_response({
            "success": False,
            "error": "Error interno del servidor",
            "details": str(exc)
        }, 500)
