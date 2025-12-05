import logging
import json
from datetime import datetime, timezone
import re
import sys
import os

import azure.functions as func
import pandas as pd


# Ensure shared_code is importable when running in Functions context
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared_code.gdrive_client import GoogleDriveClient  # noqa: E402
from shared_code.timestamp_manager import TimestampManager  # noqa: E402
from shared_code.config import get_processed_folder_id  # noqa: E402
from shared_code.etl_core import consolidate_sensor_data  # noqa: E402
from shared_code.lab_crosser import (  # noqa: E402
    cross_with_lab,
    get_lab_file_for_sensor,
    load_lab_control_file,
)
from shared_code.consolidar_sensores import to_wide  # noqa: E402
from shared_code.calibracion import (  # noqa: E402
    aplicar_curvas_calibracion,
    find_calibration_files,
    select_calibration_file,
)
from shared_code.etl_core import extract_sensor_id_from_name  # noqa: E402


logger = logging.getLogger(__name__)


def _json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    """Helper para crear respuestas JSON con el formato esperado."""
    return func.HttpResponse(
        body=json.dumps(payload, ensure_ascii=False),
        status_code=status_code,
        mimetype="application/json",
    )


def _detect_planta(name_or_path: str) -> str:
    """Detecta la planta desde el nombre o path (fallback si no viene en metadata)."""
    if re.search(r"\bJPV\b", name_or_path, re.IGNORECASE):
        return "JPV"
    if re.search(r"\bRB\b", name_or_path, re.IGNORECASE):
        return "RB"
    return ""


def _detect_year(name_or_path: str) -> int:
    """Detecta el año desde el nombre o path (fallback si no viene en metadata)."""
    m = re.search(r"\b(20[0-9]{2})\b", name_or_path)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    # Fallback to current year if not found
    return datetime.now(timezone.utc).year


def _parse_modified_time(modified_time_str: str) -> datetime:
    """Parsea el timestamp modifiedTime de Google Drive a datetime."""
    try:
        # Google Drive devuelve formato RFC 3339: "2025-11-19T14:30:00.000Z"
        if modified_time_str.endswith("Z"):
            modified_time_str = modified_time_str[:-1] + "+00:00"
        elif "+" not in modified_time_str and "-" in modified_time_str[-6:]:
            # Ya tiene timezone
            pass
        else:
            # Asumir UTC si no tiene timezone
            modified_time_str = modified_time_str + "+00:00"
        return datetime.fromisoformat(modified_time_str)
    except (ValueError, AttributeError) as e:
        logger.warning("Error parseando modifiedTime '%s': %s", modified_time_str, e)
        # Fallback a ahora si no se puede parsear
        return datetime.now(timezone.utc)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function HTTP Trigger para procesar archivos de sensores desde Google Drive.
    
    Recibe metadata de Google Apps Script con:
    - fileId: ID único del archivo en Google Drive
    - fileName: Nombre del archivo
    - secadora: Nombre de la secadora
    - planta: Planta de origen (JPV o RB)
    - folderId: ID de la carpeta en Drive
    - uploadDate: Timestamp ISO 8601
    - size, mimeType, fileUrl, driveUrl: Metadata adicional
    """
    start_ts = datetime.now(timezone.utc)
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

        # Extraer metadata de Google Apps Script
        file_id = data.get("fileId")
        file_name = data.get("fileName")
        secadora = data.get("secadora")
        planta = data.get("planta")
        folder_id = data.get("folderId")
        upload_date = data.get("uploadDate")
        file_url = data.get("fileUrl") or data.get("driveUrl")
        mime_type = data.get("mimeType")
        file_size = data.get("size")

        # Logging detallado
        logger.info("=== Nuevo archivo recibido desde Google Apps Script ===")
        logger.info("Planta: %s", planta)
        logger.info("Secadora: %s", secadora)
        logger.info("Archivo: %s", file_name)
        logger.info("FileID: %s", file_id)
        logger.info("FolderID: %s", folder_id)
        logger.info("Fecha de carga: %s", upload_date)
        logger.info("Tamaño: %s bytes", file_size)
        logger.info("MIME Type: %s", mime_type)

        # Validar campos requeridos
        # folderId y secadora son necesarios para el sistema de timestamps incremental
        required_fields = ["planta"]
        missing = [k for k in required_fields if not data.get(k)]
        if missing:
            return _json_response({
                "success": False,
                "error": f"Faltan campos requeridos en la metadata",
                "details": f"Campos faltantes: {', '.join(missing)}"
            }, 400)

        # Validar folderId y secadora para sistema incremental
        if not folder_id:
            logger.warning("[ETL] No se proporcionó folderId, usando modo de procesamiento único")
        if not secadora:
            logger.warning("[ETL] No se proporcionó secadora, usando modo de procesamiento único")

        # Validar que planta sea JPV o RB
        if planta:
            planta = planta.strip().upper()
            if planta not in ["JPV", "RB"]:
                return _json_response({
                    "success": False,
                    "error": "Planta inválida",
                    "details": f"La planta debe ser 'JPV' o 'RB', recibido: '{planta}'"
                }, 400)
        else:
            # Fallback: intentar detectar desde fileName
            planta = _detect_planta(file_name or "")
            if not planta:
                return _json_response({
                    "success": False,
                    "error": "No se pudo determinar la planta",
                    "details": "La planta debe estar en la metadata o ser detectable desde el nombre del archivo"
                }, 400)

        # Detectar año (desde uploadDate o fileName como fallback)
        year = datetime.now(timezone.utc).year
        if upload_date:
            try:
                upload_dt = datetime.fromisoformat(upload_date.replace("Z", "+00:00"))
                year = upload_dt.year
            except (ValueError, AttributeError):
                logger.warning("No se pudo parsear uploadDate, usando detección desde fileName")
                year = _detect_year(file_name or "")
        else:
            year = _detect_year(file_name or "")

        logger.info("[ETL] Procesando - Planta: %s, Año: %s, Secadora: %s", planta, year, secadora)

        # Inicializar cliente de Google Drive
        try:
            client = GoogleDriveClient()
        except Exception as exc:
            logger.exception("[ETL] Error inicializando GoogleDriveClient: %s", exc)
            return _json_response({
                "success": False,
                "error": "Error inicializando cliente de Google Drive",
                "details": str(exc)
            }, 500)

        # =====================================================================
        # SISTEMA DE TIMESTAMPS INCREMENTAL
        # =====================================================================
        # Si tenemos folderId y secadora, usar sistema incremental
        # Si no, procesar solo el archivo recibido (modo legacy)
        use_incremental = folder_id and secadora

        if use_incremental:
            logger.info("[ETL] Modo incremental activado - procesando archivos nuevos desde última ejecución")
            
            # 1. Inicializar TimestampManager
            timestamp_manager = TimestampManager(client)
            
            # 2. Obtener timestamp de última ejecución
            last_run = timestamp_manager.get_last_run_timestamp(planta, secadora)
            if last_run:
                logger.info("[ETL] Última ejecución: %s", last_run.isoformat())
            else:
                logger.info("[ETL] Primera ejecución para %s - %s", planta, secadora)
            
            # 3. Listar TODOS los archivos en la carpeta de esa secadora
            logger.info("[ETL] Listando archivos en carpeta (folderId: %s)...", folder_id)
            try:
                all_files = client.list_files_by_folder_id(folder_id)
                logger.info("[ETL] Encontrados %d archivos en la carpeta", len(all_files))
            except Exception as exc:
                logger.exception("[ETL] Error listando archivos en carpeta: %s", exc)
                return _json_response({
                    "success": False,
                    "error": "Error listando archivos en carpeta",
                    "details": str(exc)
                }, 500)
            
            # 4. Filtrar solo archivos nuevos (modificados después de last_run)
            if last_run:
                new_files = []
                for file_info in all_files:
                    if not file_info.get("modifiedTime"):
                        logger.warning("[ETL] Archivo sin modifiedTime: %s", file_info.get("name", "unknown"))
                        continue
                    file_modified = _parse_modified_time(file_info["modifiedTime"])
                    if file_modified > last_run:
                        new_files.append(file_info)
                logger.info("[ETL] Archivos nuevos desde última ejecución: %d de %d", len(new_files), len(all_files))
            else:
                new_files = all_files
                logger.info("[ETL] Primera ejecución: procesando todos los %d archivos", len(new_files))
            
            # 5. Procesar cada archivo nuevo
            processed_files = []
            total_records_processed = 0
            total_records_matched_lab = 0
            
            for file_info in new_files:
                file_id_to_process = file_info.get("id")
                file_name_to_process = file_info.get("name")
                file_modified_time = file_info.get("modifiedTime")
                
                logger.info("[ETL] Procesando archivo: %s (ID: %s, Modificado: %s)", 
                           file_name_to_process, file_id_to_process, file_modified_time)
                
                try:
                    # Descargar archivo
                    content = client.download_file(file_name_to_process or "", file_id=file_id_to_process)
                    
                    # Procesar datos del sensor
                    sensor_df = consolidate_sensor_data(content, file_name_to_process, planta)
                    records_processed = int(len(sensor_df))

                    # Intentar cruzar con laboratorio (formato largo)
                    records_matched_lab = 0
                    sensor_with_lab = sensor_df.copy()
                    try:
                        lab_bytes = get_lab_file_for_sensor(client, planta=planta, year=year)
                        lab_df = load_lab_control_file(lab_bytes, year=year, planta=planta)
                        sensor_with_lab = cross_with_lab(sensor_df, lab_df, require_sensor_match=True)
                        if "Variedad" in sensor_with_lab.columns:
                            records_matched_lab = int(sensor_with_lab["Variedad"].notna().sum())
                    except Exception as exc:
                        logger.warning("[ETL] Archivo de control de laboratorio no encontrado o cruce falló: %s", exc)

                    # Convertir a formato ancho (pivot)
                    logger.info("[ETL] Convirtiendo a formato ancho (pivot)...")
                    final_df = sensor_with_lab
                    try:
                        if "año" not in sensor_with_lab.columns:
                            sensor_with_lab["año"] = year
                        if "planta" not in sensor_with_lab.columns:
                            sensor_with_lab["planta"] = planta
                        if "sensor_id" not in sensor_with_lab.columns or sensor_with_lab["sensor_id"].isna().all():
                            sensor_id = extract_sensor_id_from_name(file_name_to_process or "")
                            sensor_with_lab["sensor_id"] = sensor_id

                        wide_df = to_wide(sensor_with_lab)
                        logger.info(
                            "[ETL] Formato ancho: %d filas, %d columnas",
                            len(wide_df),
                            len(wide_df.columns),
                        )

                        if "VOLT_HUM" not in wide_df.columns or "VOLT_TEM" not in wide_df.columns:
                            logger.error("[ETL] Pivot no generó VOLT_HUM/VOLT_TEM, usando formato largo")
                            final_df = sensor_with_lab
                        else:
                            final_df = wide_df
                    except Exception as exc:
                        logger.error("[ETL] Error en pivot, usando formato largo: %s", exc)
                        final_df = sensor_with_lab

                    # Aplicar calibración si corresponde
                    if "VOLT_HUM" in final_df.columns and "VOLT_TEM" in final_df.columns:
                        logger.info("[ETL] Aplicando curvas de calibración...")
                        try:
                            calibracion_files = find_calibration_files(
                                client, planta, f"Secado_Arroz/{planta}/raw"
                            )
                            seleccion = (
                                select_calibration_file(calibracion_files, year, planta)
                                if calibracion_files
                                else None
                            )

                            if seleccion:
                                año_calibracion, calibracion_path = seleccion
                                logger.info(
                                    "[ETL] Calibrando con curvas del año %s",
                                    año_calibracion,
                                )
                                final_df = aplicar_curvas_calibracion(
                                    final_df,
                                    client,
                                    planta,
                                    calibracion_path,
                                )
                            else:
                                logger.warning(
                                    "[ETL] No se encontró archivo de calibración para %s (año %s)",
                                    planta,
                                    year,
                                )
                        except Exception as exc:
                            logger.error("[ETL] Error en calibración: %s", exc)
                    else:
                        logger.warning("[ETL] Sin VOLT_HUM/VOLT_TEM, omitiendo calibración")

                    records_unmatched = int(records_processed - records_matched_lab)
                    
                    # Generar nombre de salida y subir archivo procesado
                    file_ts = datetime.now(timezone.utc)
                    ts_str = file_ts.strftime("%Y%m%dT%H%M%SZ")
                    base_name = os.path.splitext(os.path.basename(file_name_to_process))[0]
                    processed_file = f"{base_name}_processed_{ts_str}.csv"
                    processed_path = f"Secado_Arroz/{planta}/processed/{processed_file}"
                    
                    # Obtener folder_id de la carpeta de salida según la planta
                    try:
                        processed_folder_id = get_processed_folder_id(planta)
                        logger.info(
                            f"[ETL] Subiendo archivo procesado a carpeta de {planta} (folder: {processed_folder_id})"
                        )
                        
                        # Subir archivo procesado (formato ancho si está disponible)
                        if "VOLT_HUM" in final_df.columns:
                            cols = [
                                c
                                for c in [
                                    "planta",
                                    "año",
                                    "sensor_id",
                                    "timestamp",
                                    "VOLT_HUM",
                                    "VOLT_TEM",
                                    "TEMPERATURA",
                                    "HUMEDAD",
                                    "Variedad",
                                    "ID_tachada",
                                    "HumedadInicial",
                                    "HumedadFinal",
                                    "source_file",
                                    "source_path",
                                    "tirada_num",
                                    "tirada_fecha",
                                    "Date_raw",
                                    "LOC_time_raw",
                                    "TimeString",
                                ]
                                if c in final_df.columns
                            ]
                        else:
                            cols = [
                                c
                                for c in [
                                    "timestamp",
                                    "variable",
                                    "valor",
                                    "planta",
                                    "sensor_id",
                                    "source_file",
                                    "Variedad",
                                    "ID_tachada",
                                    "HumedadInicial",
                                    "HumedadFinal",
                                ]
                                if c in final_df.columns
                            ]

                        out_df = final_df[cols].copy()
                        csv_bytes = out_df.to_csv(index=False).encode("utf-8")
                        
                        client.upload_file_to_folder(
                            processed_folder_id, processed_file, csv_bytes, mime_type="text/csv"
                        )
                        logger.info(f"[ETL] ✓ Archivo procesado subido: {processed_file}")
                    except ValueError as e:
                        logger.error(f"[ETL] No se pudo subir archivo: {str(e)}")
                        raise
                    
                    logger.info("[ETL] Archivo procesado exitosamente: %s (%d registros)", 
                               file_name_to_process, records_processed)
                    
                    processed_files.append({
                        "fileId": file_id_to_process,
                        "fileName": file_name_to_process,
                        "processedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "status": "success",
                        "records_processed": records_processed,
                        "records_matched_lab": records_matched_lab,
                        "records_unmatched": records_unmatched,
                        "processed_file": processed_file,
                        "processed_path": processed_path,
                    })
                    
                    total_records_processed += records_processed
                    total_records_matched_lab += records_matched_lab
                    
                except Exception as exc:
                    logger.exception("[ETL] Error procesando archivo %s: %s", file_name_to_process, exc)
                    processed_files.append({
                        "fileId": file_id_to_process,
                        "fileName": file_name_to_process,
                        "processedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "status": "error",
                        "error": str(exc)
                    })
                    # Continuar con el siguiente archivo
            
            # 6. Actualizar timestamp después de procesar
            if processed_files:
                try:
                    # Usar el timestamp más reciente de los archivos procesados o ahora
                    latest_timestamp = datetime.now(timezone.utc)
                    for file_info in new_files:
                        if file_info.get("modifiedTime"):
                            file_modified = _parse_modified_time(file_info["modifiedTime"])
                            if file_modified > latest_timestamp:
                                latest_timestamp = file_modified
                    
                    timestamp_manager.update_timestamp(planta, secadora, processed_files, latest_timestamp)
                    logger.info("[ETL] Timestamp actualizado: %s", latest_timestamp.isoformat())
                except Exception as exc:
                    logger.error("[ETL] Error actualizando timestamp: %s", exc)
                    # No fallar la ejecución si solo falla el timestamp
            
            # Respuesta de éxito con múltiples archivos
            resp = {
                "success": True,
                "message": f"ETL incremental completado - {len(processed_files)} archivos procesados",
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "metadata": {
                    "planta": planta,
                    "secadora": secadora,
                    "year": year,
                    "total_files_processed": len(processed_files),
                    "total_records_processed": total_records_processed,
                    "total_records_matched_lab": total_records_matched_lab,
                    "total_records_unmatched": total_records_processed - total_records_matched_lab,
                    "last_run_timestamp": last_run.isoformat() if last_run else None,
                },
                "processed_files": processed_files
            }
            logger.info("[ETL] Procesamiento incremental completado - %d archivos, %d registros totales", 
                       len(processed_files), total_records_processed)
            return _json_response(resp, 200)
        
        else:
            # =====================================================================
            # MODO LEGACY: Procesar solo el archivo recibido
            # =====================================================================
            logger.info("[ETL] Modo legacy - procesando solo archivo recibido")
            
            if not file_id and not file_name:
                return _json_response({
                    "success": False,
                    "error": "No se puede procesar: falta fileId o fileName",
                    "details": "En modo legacy se requiere fileId o fileName"
                }, 400)
            
            # Descargar archivo desde Google Drive usando fileId (optimizado)
            logger.info("[ETL] Descargando archivo desde Google Drive usando fileId: %s", file_id)
            try:
                if file_id:
                    content = client.download_file(file_name or "", file_id=file_id)
                    logger.info("[ETL] Archivo descargado exitosamente usando fileId (optimizado)")
                elif folder_id and file_name:
                    logger.info("[ETL] Usando folderId para buscar archivo en carpeta específica")
                    content = client.download_file(file_name, file_id=None)
                else:
                    if not file_name:
                        raise ValueError("No se puede descargar el archivo: falta fileId, folderId o fileName")
                    content = client.download_file(file_name, file_id=None)
            except Exception as exc:
                logger.exception("[ETL] Error descargando archivo desde Google Drive: %s", exc)
                return _json_response({
                    "success": False,
                    "error": "Error descargando archivo desde Google Drive",
                    "details": str(exc),
                    "processedFile": file_name,
                }, 500)

            # Procesar datos del sensor
            logger.info("[ETL] Consolidando datos del sensor (planta=%s, secadora=%s)...", planta, secadora)
            try:
                sensor_df = consolidate_sensor_data(content, file_name, planta)
            except Exception as exc:
                logger.exception("[ETL] Error procesando archivo de sensor: %s", exc)
                return _json_response({
                    "success": False,
                    "error": "Error procesando archivo de sensor",
                    "details": str(exc),
                    "processedFile": file_name,
                }, 500)

            records_processed = int(len(sensor_df))

            # Intentar cruzar con laboratorio (formato largo)
            records_matched_lab = 0
            sensor_with_lab = sensor_df.copy()
            try:
                logger.info("[ETL] Buscando archivo de control de laboratorio para planta=%s año=%s", planta, year)
                lab_bytes = get_lab_file_for_sensor(client, planta=planta, year=year)
                lab_df = load_lab_control_file(lab_bytes, year=year, planta=planta)
                logger.info("[ETL] Cruzando datos de sensor con control de laboratorio (%d filas)", len(lab_df))
                sensor_with_lab = cross_with_lab(sensor_df, lab_df, require_sensor_match=True)
                if "Variedad" in sensor_with_lab.columns:
                    records_matched_lab = int(sensor_with_lab["Variedad"].notna().sum())
                logger.info("[ETL] Registros cruzados con laboratorio: %d/%d", records_matched_lab, records_processed)
            except Exception as exc:
                logger.warning("[ETL] Archivo de control de laboratorio no encontrado o cruce falló: %s", exc)

            # Convertir a formato ancho
            logger.info("[ETL] Convirtiendo a formato ancho (pivot)...")
            final_df = sensor_with_lab
            try:
                if "año" not in sensor_with_lab.columns:
                    sensor_with_lab["año"] = year
                if "planta" not in sensor_with_lab.columns:
                    sensor_with_lab["planta"] = planta
                if "sensor_id" not in sensor_with_lab.columns or sensor_with_lab["sensor_id"].isna().all():
                    sensor_id = extract_sensor_id_from_name(file_name or "")
                    sensor_with_lab["sensor_id"] = sensor_id

                wide_df = to_wide(sensor_with_lab)
                logger.info("[ETL] Formato ancho: %d filas, %d columnas", len(wide_df), len(wide_df.columns))

                if "VOLT_HUM" not in wide_df.columns or "VOLT_TEM" not in wide_df.columns:
                    logger.error("[ETL] Pivot no generó VOLT_HUM/VOLT_TEM, usando formato largo")
                    final_df = sensor_with_lab
                else:
                    final_df = wide_df
            except Exception as exc:
                logger.error("[ETL] Error en pivot, usando formato largo: %s", exc)
                final_df = sensor_with_lab

            # Aplicar calibración
            if "VOLT_HUM" in final_df.columns and "VOLT_TEM" in final_df.columns:
                logger.info("[ETL] Aplicando curvas de calibración...")
                try:
                    calibracion_files = find_calibration_files(client, planta, f"Secado_Arroz/{planta}/raw")
                    seleccion = (
                        select_calibration_file(calibracion_files, year, planta)
                        if calibracion_files
                        else None
                    )

                    if seleccion:
                        año_calibracion, calibracion_path = seleccion
                        logger.info("[ETL] Calibrando con curvas del año %s", año_calibracion)
                        final_df = aplicar_curvas_calibracion(
                            final_df,
                            client,
                            planta,
                            calibracion_path,
                        )
                    else:
                        logger.warning(
                            "[ETL] No se encontró archivo de calibración para %s (año %s)",
                            planta,
                            year,
                        )
                except Exception as exc:
                    logger.error("[ETL] Error en calibración: %s", exc)
            else:
                logger.warning("[ETL] Sin VOLT_HUM/VOLT_TEM, omitiendo calibración")

            records_unmatched = int(records_processed - records_matched_lab)

            # Generar nombre de salida y subir archivo procesado
            ts_str = start_ts.strftime("%Y%m%dT%H%M%SZ")
            base_name = os.path.splitext(os.path.basename(file_name))[0]
            processed_file = f"{base_name}_processed_{ts_str}.csv"
            processed_path = f"Secado_Arroz/{planta}/processed/{processed_file}"

            # Obtener folder_id de la carpeta de salida según la planta
            try:
                processed_folder_id = get_processed_folder_id(planta)
                logger.info(
                    f"[ETL] Subiendo archivo procesado a carpeta de {planta} (folder: {processed_folder_id})"
                )

                # Asegurar orden consistente de columnas
                if "VOLT_HUM" in final_df.columns:
                    cols = [
                        c
                        for c in [
                            "planta",
                            "año",
                            "sensor_id",
                            "timestamp",
                            "VOLT_HUM",
                            "VOLT_TEM",
                            "TEMPERATURA",
                            "HUMEDAD",
                            "Variedad",
                            "ID_tachada",
                            "HumedadInicial",
                            "HumedadFinal",
                            "source_file",
                            "source_path",
                            "tirada_num",
                            "tirada_fecha",
                            "Date_raw",
                            "LOC_time_raw",
                            "TimeString",
                        ]
                        if c in final_df.columns
                    ]
                else:
                    cols = [
                        c
                        for c in [
                            "timestamp",
                            "variable",
                            "valor",
                            "planta",
                            "sensor_id",
                            "source_file",
                            "Variedad",
                            "ID_tachada",
                            "HumedadInicial",
                            "HumedadFinal",
                        ]
                        if c in final_df.columns
                    ]
                out_df = final_df[cols].copy()
                csv_bytes = out_df.to_csv(index=False).encode("utf-8")

                client.upload_file_to_folder(
                    processed_folder_id, processed_file, csv_bytes, mime_type="text/csv"
                )
                logger.info(f"[ETL] ✓ Archivo procesado subido: {processed_file}")
            except ValueError as e:
                logger.error(f"[ETL] No se pudo subir archivo: {str(e)}")
                raise
            except Exception as exc:
                logger.exception("[ETL] Error subiendo archivo procesado: %s", exc)
                return _json_response({
                    "success": False,
                    "error": "Error subiendo archivo procesado",
                    "details": str(exc),
                    "processedFile": file_name,
                }, 500)

            # Respuesta de éxito
            resp = {
                "success": True,
                "message": "ETL iniciado correctamente",
                "processedFile": file_name,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "metadata": {
                    "planta": planta,
                    "secadora": secadora,
                    "year": year,
                    "records_processed": records_processed,
                    "records_matched_lab": records_matched_lab,
                    "records_unmatched": records_unmatched,
                    "processed_file": processed_file,
                    "processed_path": processed_path,
                }
            }
            logger.info("[ETL] Procesamiento completado exitosamente - %d registros procesados", records_processed)
            return _json_response(resp, 200)

    except Exception as exc:
        logger.exception("[ETL] Error no manejado: %s", exc)
        return _json_response({
            "success": False,
            "error": "Error no manejado durante el procesamiento",
            "details": str(exc)
        }, 500)

