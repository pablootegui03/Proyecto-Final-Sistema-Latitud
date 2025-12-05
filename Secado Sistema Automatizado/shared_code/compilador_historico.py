"""
Módulo para compilar archivos CSV validados en un dataset histórico único.

Replica la lógica de compilado_post_ml.py pero integrado al sistema actual:
- Usa GoogleDriveClient del sistema
- Usa get_validated_folder_id(planta)
- Sin OAuth manual
- Integrado al sistema de logging
"""

import io
import logging
from typing import Dict, Any, List

import pandas as pd
from googleapiclient.http import MediaIoBaseUpload

logger = logging.getLogger(__name__)

# Nombre del archivo histórico
NOMBRE_HISTORICO = "df_historico.csv"


def compilar_historico(gdrive_client, planta: str) -> Dict[str, Any]:
    """
    Compila todos los archivos CSV validados en un dataset histórico único.

    - Obtiene folder validated con get_validated_folder_id(planta)
    - Lista CSV en la carpeta
    - Ignora df_historico.csv
    - Compila un DataFrame único agregando columna 'archivo_origen'
    - Sube/actualiza df_historico.csv en la misma carpeta validated
    - Devuelve dict con resultado y filas procesadas

    Args:
        gdrive_client: Instancia de GoogleDriveClient
        planta: Código de planta (JPV o RB)

    Returns:
        dict con:
            - success: bool
            - filas: int (número de filas en el histórico compilado)
            - archivos_procesados: int (número de archivos CSV consolidados)
            - mensaje: str
    """
    try:
        from shared_code.config import get_validated_folder_id

        # Obtener folder_id de la carpeta validated
        folder_id = get_validated_folder_id(planta)
        logger.info(f"[Compilador] Planta: {planta}, Folder validated: {folder_id}")

        # Listar CSV en la carpeta (excluyendo el histórico)
        logger.info(f"[Compilador] Listando archivos CSV en carpeta validated...")
        archivos = gdrive_client.list_files_by_folder_id(folder_id, mime_type="text/csv")

        # Filtrar excluyendo df_historico.csv
        archivos_csv = [
            f for f in archivos
            if f.get("name") != NOMBRE_HISTORICO
        ]

        if not archivos_csv:
            logger.info(
                "[Compilador] No se encontraron CSV para consolidar "
                "(solo podría estar el histórico o nada)."
            )
            return {
                "success": True,
                "filas": 0,
                "archivos_procesados": 0,
                "mensaje": "No hay archivos CSV para consolidar"
            }

        logger.info(f"[Compilador] Archivos a consolidar: {len(archivos_csv)}")
        for a in archivos_csv:
            logger.debug(f"  - {a.get('name')}")

        # Descargar y compilar DataFrames
        dfs: List[pd.DataFrame] = []
        for archivo in archivos_csv:
            file_id = archivo.get("id")
            file_name = archivo.get("name", "unknown")
            
            try:
                logger.debug(f"[Compilador] Descargando {file_name}...")
                content = gdrive_client.download_file(file_name, file_id=file_id)
                
                # Leer CSV desde bytes
                df = pd.read_csv(io.BytesIO(content))
                
                # Agregar columna archivo_origen
                df["archivo_origen"] = file_name
                
                dfs.append(df)
                logger.debug(f"[Compilador] ✓ {file_name}: {len(df)} filas")
                
            except Exception as e:
                logger.warning(
                    f"[Compilador] ADVERTENCIA: no se pudo leer {file_name}: {e}"
                )
                continue

        if not dfs:
            logger.warning(
                "[Compilador] No se pudo leer ningún CSV. No se actualizará el histórico."
            )
            return {
                "success": False,
                "filas": 0,
                "archivos_procesados": 0,
                "mensaje": "No se pudo leer ningún archivo CSV"
            }

        # Concatenar todos los DataFrames
        df_historico = pd.concat(dfs, ignore_index=True)
        total_filas = len(df_historico)
        logger.info(f"[Compilador] Total filas histórico: {total_filas}")

        # Eliminar filas duplicadas según la columna "id"
        filas_antes = len(df_historico)
        df_historico = df_historico.drop_duplicates(subset=["ID_tachada"], keep="first")
        filas_despues = len(df_historico)
        total_filas = filas_despues
        if filas_antes != filas_despues:
            logger.info(
                f"[Compilador] Duplicados eliminados: {filas_antes} -> {filas_despues} "
                f"({filas_antes - filas_despues} filas eliminadas)"
            )

        # Subir/actualizar df_historico.csv
        logger.info(f"[Compilador] Subiendo/actualizando {NOMBRE_HISTORICO}...")
        _subir_o_actualizar_historico(gdrive_client, folder_id, NOMBRE_HISTORICO, df_historico)

        logger.info(
            f"[Compilador] ✓ Compilación completada: {total_filas} filas de {len(dfs)} archivos"
        )

        return {
            "success": True,
            "filas": total_filas,
            "archivos_procesados": len(dfs),
            "mensaje": f"Compilación exitosa: {total_filas} filas de {len(dfs)} archivos"
        }

    except Exception as e:
        logger.exception(f"[Compilador] Error durante compilación: {e}")
        return {
            "success": False,
            "filas": 0,
            "archivos_procesados": 0,
            "mensaje": f"Error: {str(e)}"
        }


def _subir_o_actualizar_historico(
    gdrive_client,
    folder_id: str,
    nombre_archivo: str,
    df: pd.DataFrame
) -> None:
    """
    Sube df como CSV a la carpeta. Si ya existe df_historico, lo actualiza.

    Args:
        gdrive_client: Instancia de GoogleDriveClient
        folder_id: ID de la carpeta en Google Drive
        nombre_archivo: Nombre del archivo (df_historico.csv)
        df: DataFrame a subir
    """
    # Convertir DataFrame a bytes
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    csv_bytes = buffer.getvalue()

    # Usar el método público upload_file_to_folder primero para crear/actualizar
    # Pero necesitamos verificar si existe primero para actualizar correctamente
    try:
        # Intentar usar el servicio directamente para buscar y actualizar
        # Acceder al servicio interno (método privado pero necesario para actualización)
        service = gdrive_client._get_service()
        
        # Escapar nombre del archivo para la query
        nombre_escapado = gdrive_client._escape(nombre_archivo) if hasattr(gdrive_client, '_escape') else nombre_archivo.replace("'", "\\'")
        
        # Buscar archivo por nombre en la carpeta
        query = (
            f"'{folder_id}' in parents and "
            f"name = '{nombre_escapado}' and "
            f"trashed = false"
        )
        
        result = service.files().list(
            q=query,
            spaces="drive",
            fields="files(id, name)",
        ).execute()
        
        files = result.get("files", [])
        existente_id = files[0]["id"] if files else None

        # Preparar media para subida
        media = MediaIoBaseUpload(
            io.BytesIO(csv_bytes),
            mimetype="text/csv",
            resumable=True
        )

        if existente_id is None:
            # Crear nuevo archivo usando upload_file_to_folder
            gdrive_client.upload_file_to_folder(
                folder_id=folder_id,
                file_name=nombre_archivo,
                content=csv_bytes,
                mime_type="text/csv"
            )
            logger.info(
                f"[Compilador] ✓ Creado histórico: {nombre_archivo}"
            )
        else:
            # Actualizar archivo existente usando el servicio directamente
            archivo = service.files().update(
                fileId=existente_id,
                media_body=media,
                fields="id, name",
            ).execute()
            logger.info(
                f"[Compilador] ✓ Actualizado histórico: {archivo['name']} (id={archivo['id']})"
            )
    except Exception as e:
        logger.error(f"[Compilador] Error al subir/actualizar histórico: {e}")
        # Fallback: intentar solo crear (puede fallar si ya existe)
        try:
            gdrive_client.upload_file_to_folder(
                folder_id=folder_id,
                file_name=nombre_archivo,
                content=csv_bytes,
                mime_type="text/csv"
            )
            logger.info(f"[Compilador] ✓ Archivo subido (fallback): {nombre_archivo}")
        except Exception as e2:
            logger.exception(f"[Compilador] Error en fallback: {e2}")
            raise

