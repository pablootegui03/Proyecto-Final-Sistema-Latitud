"""
Módulo para consolidar sensores JPV y RB desde Google Drive.

Basado en la lógica del notebook consolidar_sensores.ipynb, este módulo:
1. Construye inventario de archivos desde Google Drive
2. Procesa todos los archivos y los consolida en formato largo
3. Convierte a formato ancho (pivot) con VOLT_HUM/VOLT_TEM
4. Guarda resultados en Excel con múltiples hojas
"""

import io
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

from shared_code.etl_core import (
    read_jpv_txt,
    read_rb_csv,
    extract_sensor_id_from_name,
)
from shared_code.gdrive_client import GoogleDriveClient
from shared_code.time_utils import normalize_timestamp

# Configuración
RB_VOLT_SCALE = 0.01  # dividir voltajes de RB por 100 para equiparar con JPV
DROP_WIDE_COLS = ["TimeString", "HUMEDAD", "OFFSET", "TEMPERATURA"]
EXPORT_LONG = True  # Si True, exporta también los datos en "largo" (auditoría)


def is_plain_sensor_folder(name: str) -> bool:
    """
    True solo si la carpeta es exactamente SENSOR<1..6> (sin 'b' ni 'c').
    Aplica solo a JPV.
    """
    m = re.fullmatch(r"SENSOR([1-6])", name.upper())
    return m is not None


def parse_tirada_jpv(path: str) -> Tuple[Optional[int], Optional[datetime]]:
    """
    NUEVA LÓGICA: La información de tirada/fecha ya no se extrae de la ruta.
    Esta información ahora proviene del archivo de laboratorio mediante el cruce
    por rangos de tiempo (Inicio/Fin) y sensor_id.
    
    Devuelve None, None para indicar que no se puede inferir desde el path.
    """
    # La información de tirada/fecha se extrae del archivo de laboratorio, no de la ruta
    return None, None


def parse_tirada_rb(path: str) -> Optional[datetime]:
    """
    NUEVA LÓGICA: La información de tirada/fecha ya no se extrae de la ruta.
    Esta información ahora proviene del archivo de laboratorio mediante el cruce
    por rangos de tiempo (Inicio/Fin) y sensor_id.
    
    Devuelve None para indicar que no se puede inferir desde el path.
    """
    # La información de tirada/fecha se extrae del archivo de laboratorio, no de la ruta
    return None


def _detect_planta_from_path(path: str) -> Optional[str]:
    """Detecta automáticamente la planta desde el path"""
    if re.search(r"\bJPV\b", path, re.IGNORECASE):
        return "JPV"
    if re.search(r"\bRB\b", path, re.IGNORECASE):
        return "RB"
    return None


def build_inventory_from_gdrive(
    gdrive: GoogleDriveClient,
    raw_path: str,
) -> pd.DataFrame:
    """
    Construye inventario de archivos desde Google Drive.
    
    Busca recursivamente en subcarpetas dentro de raw:
    - Archivos .txt y .csv (sensores)
    - Detecta automáticamente la planta desde el path/nombre
    - No procesa archivos Excel (laboratorio)
    
    Args:
        gdrive: Cliente de Google Drive
        raw_path: Path completo desde la raíz (ej: "Secado_Arroz/JPV/raw")
    """
    rows = []
    folder_mime = "application/vnd.google-apps.folder"
    
    def _walk_folder(folder_path: str, relative_path: str = ""):
        """
        Función recursiva para caminar por carpetas.
        
        Args:
            folder_path: Path completo desde la raíz para list_files (ej: "Secado_Arroz/JPV/raw/USB 1/15.03.24")
            relative_path: Path relativo desde raw_path (no usado, se usa folder_path completo)
        """
        try:
            items = gdrive.list_files(folder_path)
        except Exception:
            return
        
        for item in items:
            item_name = item.get("name", "")
            item_mime = item.get("mimeType", "")
            # IMPORTANTE: Construir path completo desde la raíz INCLUYENDO todas las carpetas padre
            # folder_path ya es el path completo desde la raíz (ej: "Secado_Arroz/JPV/raw/USB 1/15.03.24/sensor_2")
            # Necesitamos usar folder_path (no raw_path) para que parse_tirada_jpv pueda encontrar "USB 1" y "15.03.24"
            full_path = f"{folder_path}/{item_name}" if folder_path else f"{raw_path}/{item_name}"
            
            if item_mime == folder_mime:
                # Es una carpeta
                # Si es carpeta laboratorio, saltarla (los archivos de lab se buscan aparte)
                if item_name.lower() == "laboratorio":
                    continue
                
                # Continuar buscando recursivamente en todas las carpetas
                next_relative_path = f"{relative_path}/{item_name}" if relative_path else item_name
                _walk_folder(f"{folder_path}/{item_name}", next_relative_path)
            else:
                # Es un archivo
                # Solo procesar archivos de sensores (.txt y .csv), no Excel
                if not (item_name.lower().endswith(".txt") or item_name.lower().endswith(".csv")):
                    continue
                
                # PRIORIZAR EXTENSIÓN DEL ARCHIVO sobre el path para detectar planta
                # La extensión es más confiable que el path
                detected_planta = None
                
                # Primero: heurística por extensión (más confiable)
                if item_name.lower().endswith(".txt"):
                    detected_planta = "JPV"
                elif item_name.lower().endswith(".csv"):
                    detected_planta = "RB"
                else:
                    continue  # No es un archivo de sensor conocido
                
                # Segundo: si el nombre del archivo tiene JPV o RB explícito, usarlo
                if re.search(r"\bJPV\b", item_name, re.IGNORECASE):
                    detected_planta = "JPV"
                elif re.search(r"\bRB\b", item_name, re.IGNORECASE):
                    detected_planta = "RB"
                
                # Si aún no detectamos, intentar desde el path como fallback
                if detected_planta is None:
                    detected_planta = _detect_planta_from_path(full_path)
                    
                # Si definitivamente no detectamos, saltar
                if detected_planta is None:
                    continue
                
                # Para JPV, verificar que esté dentro de una carpeta SENSOR válida
                if detected_planta == "JPV":
                    # Verificar si hay SENSOR[1-6] en algún lugar del path
                    has_sensor_folder = re.search(r"SENSOR[1-6]", full_path, re.IGNORECASE) is not None
                    if not has_sensor_folder:
                        # Saltar este archivo JPV si no está en carpeta SENSOR
                        continue
                
                # Extraer metadata
                # IMPORTANTE: full_path ahora incluye todas las carpetas padre desde la raíz
                # Esto permite que parse_tirada_jpv y parse_tirada_rb encuentren "USB 1", "15.03.24", etc.
                sensor_id = extract_sensor_id_from_name(item_name) or extract_sensor_id_from_name(full_path)
                
                if detected_planta == "JPV":
                    # parse_tirada_jpv necesita el path completo con todas las carpetas para buscar "USB 1" y fechas
                    tirada_num, tirada_dt = parse_tirada_jpv(full_path)
                    año = None
                    # Buscar año en múltiples lugares (más flexible):
                    # 1. En el path completo (ej: "2024 Datos Sensores JPV")
                    m = re.search(r"(20\d{2})\s+Datos\s+Sensores\s+JPV", full_path, flags=re.IGNORECASE)
                    if m:
                        año = int(m.group(1))
                    else:
                        # 2. En el nombre del archivo (ej: "SENSOR2_2024.txt")
                        m = re.search(r"(20\d{2})", item_name)
                        if m:
                            año = int(m.group(1))
                        else:
                            # 3. En cualquier parte del path (ej: "JPV/2024/raw/...")
                            m = re.search(r"(20\d{2})", full_path)
                            if m:
                                año = int(m.group(1))
                else:  # RB
                    tirada_num = None
                    tirada_dt = parse_tirada_rb(full_path)
                    año = None
                    # Buscar año en múltiples lugares (más flexible):
                    # 1. En el path completo (ej: "2024 Datos Sensores RB")
                    m = re.search(r"(20\d{2})\s+Datos\s+Sensores\s+RB", full_path, flags=re.IGNORECASE)
                    if m:
                        año = int(m.group(1))
                    else:
                        # 2. En el nombre del archivo (ej: "SENSOR1_2024.csv")
                        m = re.search(r"(20\d{2})", item_name)
                        if m:
                            año = int(m.group(1))
                        else:
                            # 3. En cualquier parte del path (ej: "RB/2024/raw/...")
                            m = re.search(r"(20\d{2})", full_path)
                            if m:
                                año = int(m.group(1))
                
                # DEBUG: Mostrar metadatos extraídos
                logger.debug(f"   📋 Metadatos extraídos para '{item_name}':")
                logger.debug(f"      Path completo: {full_path}")
                logger.debug(f"      Tirada num: {tirada_num}, Tirada fecha: {tirada_dt}")
                logger.debug(f"      Año: {año}, Sensor ID: {sensor_id}")
                
                rows.append({
                    "planta": detected_planta,
                    "año": año,
                    "tirada_num": tirada_num,
                    "tirada_fecha": tirada_dt,
                    "sensor_id": sensor_id,
                    "ext": Path(item_name).suffix.lower(),
                    "source_file": item_name,
                    "source_path": full_path,
                    "file_id": item.get("id") if "id" in item else None,
                })
    
    # Iniciar walk desde la carpeta raw
    # IMPORTANTE: folder_path debe ser el path completo desde la raíz
    # para que parse_tirada_jpv/rb pueda encontrar todas las carpetas padre
    _walk_folder(raw_path, relative_path="")
    
    # DEBUG: Mostrar estadísticas del inventario
    if rows:
        inv_temp = pd.DataFrame(rows)
        print(f"   📊 Inventario construido: {len(inv_temp)} archivos")
        if "tirada_num" in inv_temp.columns:
            tiradas_con_num = inv_temp["tirada_num"].notna().sum()
            print(f"      Tiradas con número: {tiradas_con_num}/{len(inv_temp)}")
        if "tirada_fecha" in inv_temp.columns:
            tiradas_con_fecha = inv_temp["tirada_fecha"].notna().sum()
            print(f"      Tiradas con fecha: {tiradas_con_fecha}/{len(inv_temp)}")
    
    inv = pd.DataFrame(rows)
    if len(inv) > 0:
        # Filtrar filas sin planta detectada
        inv = inv[inv["planta"].notna()]
        inv.sort_values(
            ["planta", "año", "tirada_fecha", "sensor_id", "source_file"],
            inplace=True,
            ignore_index=True,
        )
    return inv


def find_lab_files(
    gdrive: GoogleDriveClient,
    raw_path: str,
) -> Dict[str, Dict[str, str]]:
    """
    Busca archivos de laboratorio en la carpeta laboratorio.
    
    Returns: Dict[planta, Dict[year, file_path]]
    """
    lab_files = {}
    folder_mime = "application/vnd.google-apps.folder"
    
    # Buscar carpeta laboratorio en raw
    try:
        items = gdrive.list_files(raw_path)
        lab_folder_path = None
        
        for item in items:
            if item.get("mimeType") == folder_mime and item.get("name", "").lower() == "laboratorio":
                lab_folder_path = f"{raw_path}/laboratorio"
                break
        
        if lab_folder_path is None:
            return lab_files
        
        # Listar archivos en laboratorio
        lab_items = gdrive.list_files(lab_folder_path)
        
        for item in lab_items:
            item_name = item.get("name", "")
            # Buscar archivos Excel de control de laboratorio
            if item_name.lower().endswith(".xlsx") or item_name.lower().endswith(".xls"):
                # Detectar planta y año del nombre
                # Formato esperado: {PLANTA}_{YEAR}_Control_Tachadas.xlsx
                planta_match = re.search(r"\b(JPV|RB)\b", item_name, re.IGNORECASE)
                year_match = re.search(r"\b(20\d{2})\b", item_name)
                
                if planta_match and year_match:
                    planta = planta_match.group(1).upper()
                    year = year_match.group(1)
                    
                    if planta not in lab_files:
                        lab_files[planta] = {}
                    
                    file_path = f"{lab_folder_path}/{item_name}"
                    lab_files[planta][year] = file_path
                    
    except Exception:
        pass
    
    return lab_files


def process_files_from_inventory(
    gdrive: GoogleDriveClient,
    inv: pd.DataFrame,
    lab_files: Dict[str, Dict[str, str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Procesa archivos del inventario y devuelve (long_all, log_df, qa_resumen).
    
    Similar a process_files() del notebook pero trabaja con Google Drive.
    Si se proporcionan lab_files, intenta hacer cruces con datos de laboratorio.
    """
    if lab_files is None:
        lab_files = {}
    
    long_frames: List[pd.DataFrame] = []
    log_rows: List[dict] = []

    for _, r in inv.iterrows():
        file_path = r["source_path"]
        planta = r["planta"]
        
        try:
            # Descargar archivo desde Google Drive
            # Usar file_id si está disponible (más eficiente), sino usar el path
            file_id = r.get("file_id") if "file_id" in r else None
            if file_id:
                file_content = gdrive.download_file(file_path, file_id=file_id)
            else:
                file_content = gdrive.download_file(file_path)
            
            # Procesar según tipo de planta
            if planta == "JPV":
                df = read_jpv_txt(file_content, r["source_file"])
            else:  # RB
                df = read_rb_csv(file_content, r["source_file"])
            
            # Columnas meta comunes
            df["planta"] = planta
            
            # IMPORTANTE: Si el año no se detectó en el inventario, intentar inferirlo del timestamp
            # o usar el año de los archivos de laboratorio disponibles
            año_inv = r.get("año")
            if pd.isna(año_inv) or año_inv is None:
                # Intentar inferir el año del timestamp si está disponible
                if "timestamp" in df.columns and df["timestamp"].notna().any():
                    first_timestamp = df["timestamp"].dropna().iloc[0] if not df["timestamp"].dropna().empty else None
                    if first_timestamp is not None:
                        try:
                            año_inv = pd.to_datetime(first_timestamp).year
                            logger.debug(f"Inferido año {año_inv} desde timestamp para {r['source_file']}")
                        except Exception:
                            pass
                
                # Si aún no hay año y hay archivos de laboratorio disponibles, usar el primer año disponible
                if (pd.isna(año_inv) or año_inv is None) and planta in lab_files:
                    años_disponibles = list(lab_files[planta].keys())
                    if años_disponibles:
                        año_inv = int(años_disponibles[0])
                        logger.debug(f"Usando año {año_inv} desde archivos de laboratorio para {r['source_file']}")
            
            df["año"] = año_inv
            df["tirada_num"] = r["tirada_num"]
            df["tirada_fecha"] = pd.to_datetime(r["tirada_fecha"]) if pd.notna(r["tirada_fecha"]) else pd.NaT
            df["sensor_id"] = r["sensor_id"]
            df["source_file"] = r["source_file"]
            df["source_path"] = r["source_path"]
            
            # Intentar cruzar con datos de laboratorio si están disponibles
            # IMPORTANTE: Guardar columnas meta antes del cruce para no perderlas
            meta_cols_before = ["planta", "año", "tirada_num", "tirada_fecha", "sensor_id", "source_file", "source_path"]
            meta_values_before = {col: df[col].iloc[0] if col in df.columns and len(df) > 0 else None for col in meta_cols_before}
            
            año = año_inv  # Usar el año inferido
            if año and planta in lab_files:
                año_str = str(int(año)) if pd.notna(año) else None
                if año_str and año_str in lab_files[planta]:
                    try:
                        from shared_code.lab_crosser import load_lab_control_file, cross_with_lab
                        lab_file_path = lab_files[planta][año_str]
                        lab_content = gdrive.download_file(lab_file_path)
                        lab_df = load_lab_control_file(lab_content, year=int(año_str), planta=planta)
                        # Hacer el cruce (cross_with_lab ya normaliza timestamps)
                        df = cross_with_lab(df, lab_df, require_sensor_match=True)
                        
                        # Asegurar que las columnas meta se mantengan después del cruce
                        # cross_with_lab() puede no preservar todas las columnas
                        for col in meta_cols_before:
                            if col not in df.columns:
                                # Si la columna se perdió, restaurarla con los valores originales
                                if meta_values_before[col] is not None:
                                    df[col] = meta_values_before[col]
                                else:
                                    df[col] = np.nan
                    except Exception as lab_exc:
                        # Si falla el cruce, continuar sin él
                        log_rows.append({
                            "tipo": "error_cruce_lab",
                            "planta": planta,
                            "sensor_id": r.get("sensor_id"),
                            "timestamp": None,
                            "variable": None,
                            "source_file": r["source_file"],
                            "source_path": r["source_path"],
                            "detalle": f"Cruce con laboratorio falló: {lab_exc}",
                        })
            
            # Armonizar columnas crudas (por si no existen)
            for col in ["Date_raw", "LOC_time_raw", "VarName", "TimeString", "VarValue", "Validity", "Time_ms", "VarName_original"]:
                if col not in df.columns:
                    df[col] = pd.Series([np.nan] * len(df), dtype=object)
            
            # Clave de duplicado en largo
            df["dup_key"] = (
                df["planta"].astype(str) + "|" +
                df["sensor_id"].astype(str) + "|" +
                df["timestamp"].astype(str) + "|" +
                df["variable"].astype(str)
            )
            
            # Detectar duplicados exactos
            dups = df[df.duplicated(subset=["dup_key"], keep="first")]
            if len(dups) > 0:
                for _, dd in dups.iterrows():
                    log_rows.append({
                        "tipo": "duplicado_largo",
                        "planta": planta,
                        "sensor_id": r["sensor_id"],
                        "timestamp": dd["timestamp"],
                        "variable": dd["variable"],
                        "source_file": r["source_file"],
                        "source_path": r["source_path"],
                    })
                # Nos quedamos con la primera
                df = df.drop_duplicates(subset=["dup_key"], keep="first")
            
            long_frames.append(df.drop(columns=["dup_key"]))
            
        except Exception as e:
            log_rows.append({
                "tipo": "error_lectura",
                "planta": planta,
                "sensor_id": r.get("sensor_id"),
                "timestamp": None,
                "variable": None,
                "source_file": r["source_file"],
                "source_path": r["source_path"],
                "detalle": str(e),
            })
    
    # Unión y QA
    long_all = pd.concat(long_frames, ignore_index=True) if long_frames else pd.DataFrame()
    
    if not long_all.empty:
        qa = (
            long_all
            .groupby(["planta", "año", "sensor_id"])
            .agg(
                registros=("valor", "size"),
                fechas_min=("timestamp", "min"),
                fechas_max=("timestamp", "max"),
            )
            .reset_index()
        )
    else:
        qa = pd.DataFrame(columns=["planta", "año", "sensor_id", "registros", "fechas_min", "fechas_max"])
    
    log_df = pd.DataFrame(
        log_rows,
        columns=["tipo", "planta", "sensor_id", "timestamp", "variable", "source_file", "source_path", "detalle"],
    )
    
    # ACCIÓN 1: PROPAGAR METADATOS DE LABORATORIO
    # El cruce con laboratorio asigna 'Variedad' e 'ID_tachada' solo a las filas que hacen match directo
    # Necesitamos propagar estos valores a todas las filas (VOLT_HUME, VOLT_TEMP) que comparten el mismo timestamp
    if not long_all.empty and 'Variedad' in long_all.columns:
        logger.info("Propagando metadatos de laboratorio (Variedad, ID_tachada, HumedadInicial, HumedadFinal) a todas las variables...")
        
        # 1. Definir las columnas a propagar y las claves de agrupación
        cols_to_propagate = ['Variedad', 'ID_tachada', 'Descarte', 'En_duda', 'HumedadInicial', 'HumedadFinal']
        group_keys = ['planta', 'año', 'sensor_id', 'timestamp']
        
        # Filtrar columnas que realmente existen en el DataFrame
        cols_to_propagate = [c for c in cols_to_propagate if c in long_all.columns]
        
        if cols_to_propagate:
            # 2. Ordenar por tiempo (necesario para ffill/bfill)
            long_all = long_all.sort_values(by=group_keys)
            
            # 3. Agrupar y rellenar (ffill + bfill)
            # Esto toma la 'Variedad' (ej: 'Merin') de la fila 'VARIEDAD' y la copia
            # a las filas 'VOLT_HUME', 'VOLT_TEMP' que tienen el mismo timestamp.
            # Usar .transform() es más robusto para aplicar ffill/bfill dentro de cada grupo.
            for col in cols_to_propagate:
                long_all[col] = long_all.groupby(group_keys, dropna=False)[col].transform(lambda x: x.ffill().bfill())
            
            # 4. Asegurar que las columnas sean string ANTES de 'to_wide' para evitar el ValueError
            if 'Variedad' in long_all.columns:
                long_all['Variedad'] = long_all['Variedad'].astype(str)
            if 'ID_tachada' in long_all.columns:
                long_all['ID_tachada'] = long_all['ID_tachada'].astype(str)
            
            # DEBUG: Mostrar estadísticas después de la propagación
            var_valid = long_all['Variedad'].notna().sum() if 'Variedad' in long_all.columns else 0
            id_valid = long_all['ID_tachada'].notna().sum() if 'ID_tachada' in long_all.columns else 0
            hi_valid = long_all['HumedadInicial'].notna().sum() if 'HumedadInicial' in long_all.columns else 0
            hf_valid = long_all['HumedadFinal'].notna().sum() if 'HumedadFinal' in long_all.columns else 0
            logger.info(f"   Después de propagación: Variedad {var_valid}/{len(long_all)} válidos, ID_tachada {id_valid}/{len(long_all)} válidos")
            if 'HumedadInicial' in long_all.columns or 'HumedadFinal' in long_all.columns:
                logger.info(f"   HumedadInicial {hi_valid}/{len(long_all)} válidos, HumedadFinal {hf_valid}/{len(long_all)} válidos")
    
    return long_all, log_df, qa


def to_wide(long_all: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte datos de formato largo a formato ancho (pivot).
    
    Similar a to_wide() del notebook. Unifica voltajes en VOLT_HUM / VOLT_TEM,
    escala RB, excluye HUMEDAD/TEMPERATURA/OFFSET/VARIEDAD.
    """
    if long_all.empty:
        return long_all
    
    # 1) Claves estables para el pivot
    base_key_cols = ["planta", "año", "sensor_id", "timestamp"]
    key_cols = base_key_cols

    lab_meta_cols = [
        c
        for c in ["Variedad", "ID_tachada", "Descarte", "En_duda", "DESCARTAR", "HumedadInicial", "HumedadFinal"]
        if c in long_all.columns
    ]
    extra_meta_cols = [
        c for c in ["source_file", "source_path", "tirada_fecha"] if c in long_all.columns
    ]
    meta_cols_for_merge = lab_meta_cols + extra_meta_cols

    def _first_valid_meta(series: pd.Series):
        non_null = series.dropna()
        for value in non_null:
            if isinstance(value, str):
                if value.strip().lower() in {"", "nan", "none"}:
                    continue
            return value
        return non_null.iloc[0] if not non_null.empty else None
    
    # Verificar que las columnas clave estén presentes y tengan datos
    missing_cols = [c for c in base_key_cols if c not in long_all.columns]
    if missing_cols:
        logger.error(f"to_wide: Faltan columnas clave: {missing_cols}")
        for c in missing_cols:
            long_all[c] = np.nan
    
    # Verificar cuántas filas tienen NaN en key_cols BASE (no contar Variedad/ID_tachada como críticas)
    # Variedad e ID_tachada pueden tener NaN si no hay match con laboratorio, y eso está bien
    base_key_cols_nan_count = long_all[base_key_cols].isna().any(axis=1).sum()
    total_rows = len(long_all)
    if base_key_cols_nan_count == total_rows and total_rows > 0:
        logger.error(
            f"to_wide: TODAS las filas tienen NaN en key_cols base. "
            f"Verificando columnas en long_all: {list(long_all.columns)}"
        )
        # Debug: mostrar valores de key_cols en las primeras filas
        logger.error(f"to_wide: Primeras 5 filas de key_cols:\n{long_all[key_cols].head()}")
    
    # DEBUG: Información sobre Variedad e ID_tachada
    if "Variedad" in long_all.columns or "ID_tachada" in long_all.columns:
        var_valid = long_all["Variedad"].notna().sum() if "Variedad" in long_all.columns else 0
        id_valid = long_all["ID_tachada"].notna().sum() if "ID_tachada" in long_all.columns else 0
        print(f"   📊 Estado de columnas de laboratorio antes del pivot:")
        print(f"      Variedad: {var_valid}/{total_rows} válidos")
        print(f"      ID_tachada: {id_valid}/{total_rows} válidos")
        if var_valid == 0 and id_valid == 0:
            print(f"      ⚠️ ADVERTENCIA: Ninguna variedad o ID_tachada válida antes del pivot")
            print(f"         Esto puede indicar que el cruce con laboratorio no funcionó correctamente")
    
    logger.debug(f"to_wide: key_cols final: {key_cols}")
    logger.debug(f"to_wide: Tipos de datos de key_cols:\n{long_all[key_cols].dtypes}")
    
    # 2) Normalizar variable (EXACTO como notebook)
    svar = (
        long_all["variable"]
        .astype(str)
        .str.upper()
        .str.replace(r"[\s_\.\-]", "", regex=True)
    )
    
    # Aliases EXACTOS del notebook (JPV: VOLT_HUME, VOLT_TEMP; RB: V_HUM, V_TEM)
    hum_aliases = {"VOLTHUM", "VOLTHUME", "VHUM"}  # JPV: VOLT_HUME, RB: V_HUM
    tem_aliases = {"VOLTTEM", "VOLTTEMP", "VTEM", "VTEMP"}  # JPV: VOLT_TEMP, RB: V_TEM
    drop_aliases = {"HUMEDAD", "TEMPERATURA", "OFFSET", "VARIEDAD"}  # No incluir en wide
    
    mask_hum = svar.isin(hum_aliases)
    mask_tem = svar.isin(tem_aliases)
    mask_drop = svar.isin(drop_aliases)
    
    # LOGGING CRÍTICO (usar print para visibilidad):
    print(f"   📊 Análisis de variables en long_all:")
    print(f"      Total registros: {len(long_all)}")
    print(f"      Variables únicas originales: {long_all['variable'].nunique()}")
    print(f"      Muestra variables: {list(long_all['variable'].unique()[:10])}")
    print(f"      Variables normalizadas únicas: {svar.nunique()}")
    print(f"      Muestra normalizadas: {list(svar.unique()[:10])}")
    print(f"      Match HUMEDAD: {mask_hum.sum()} registros")
    print(f"      Match TEMPERATURA: {mask_tem.sum()} registros")
    print(f"      Descartadas: {mask_drop.sum()} registros")
    
    # VALIDACIÓN CRÍTICA:
    keep_mask = (mask_hum | mask_tem) & (~mask_drop)
    
    if keep_mask.sum() == 0:
        logger.error(f"❌ NINGUNA variable coincide con aliases esperados!")
        logger.error(f"   Variables encontradas: {list(svar.unique())}")
        logger.error(f"   Aliases esperados HUM: {hum_aliases}")
        logger.error(f"   Aliases esperados TEM: {tem_aliases}")
        # Retornar DataFrame vacío pero con estructura
        return pd.DataFrame(columns=key_cols + ["VOLT_HUM", "VOLT_TEM"])
    
    logger.info(f"   Registros a procesar: {keep_mask.sum()}")
    
    long_v = long_all[keep_mask].copy()
    
    # Verificar si hay filas válidas después del filtro
    if long_v.empty:
        logger.warning("to_wide: long_v está vacío después del filtro de variables de voltaje")
        return pd.DataFrame(columns=key_cols + ["VOLT_HUM", "VOLT_TEM"])
    
    # -----------------------------------------------------------------
    # FILTRAR filas cuya Variedad sea 'nan' (string) o None (NaN)
    # Estos registros no coincidieron con ningún intervalo del laboratorio y no se pueden calibrar.
    if 'Variedad' in long_v.columns:
        # Asegurarse de que la columna sea string para comparar con 'nan'
        variedad_str = long_v['Variedad'].astype(str)
        mask_valid_variedad = (variedad_str.notna()) & (variedad_str.str.lower() != 'nan') & (variedad_str.str.lower() != 'none')
        
        # Loggear cuántas filas de voltaje se descartan por falta de variedad
        descartados = len(long_v) - mask_valid_variedad.sum()
        if descartados > 0:
            logger.warning(f"to_wide: Descartando {descartados}/{len(long_v)} filas de voltaje sin cruce de laboratorio (Variedad es 'nan' o 'None')")
            
        long_v = long_v[mask_valid_variedad].copy()
        
        # Verificar si quedan filas después del filtro
        if long_v.empty:
            logger.warning("to_wide: long_v está vacío después de filtrar filas sin Variedad válida")
            return pd.DataFrame(columns=key_cols + ["VOLT_HUM", "VOLT_TEM"])
    # -----------------------------------------------------------------
    
    long_v = normalize_timestamp(long_v, "timestamp")
    
    # 3) Nombre normalizado + escala RB
    # IMPORTANTE: Recalcular máscaras usando long_v["variable"] ya que long_v es un DataFrame filtrado
    # con índices diferentes a long_all. No podemos usar mask_hum[keep_mask] directamente.
    
    # Canonicalizar variable en long_v (igual que en long_all)
    svar_v = (
        long_v["variable"]
        .astype(str)
        .str.upper()
        .str.replace(r"[\s_\.\-]", "", regex=True)
    )
    
    # Aliases para voltajes
    hum_aliases = {"VOLTHUM", "VOLTHUME", "VHUM"}
    tem_aliases = {"VOLTTEM", "VOLTTEMP", "VTEM", "VTEMP"}
    
    # Crear máscaras en long_v
    mask_hum_final = svar_v.isin(hum_aliases)
    mask_tem_final = svar_v.isin(tem_aliases)
    
    # Asignar var_norm usando las máscaras recalculadas
    long_v["var_norm"] = np.where(mask_hum_final, "VOLT_HUM", "VOLT_TEM")
    
    # Escala RB: dividir por 100 SOLO si es RB (RB_VOLT_SCALE = 0.01)
    scale = np.where(long_v["planta"].eq("RB"), 0.01, 1.0)  # RB_VOLT_SCALE = 0.01
    long_v["valor_norm"] = pd.to_numeric(long_v["valor"], errors="coerce") * scale
    
    # VALIDACIÓN valores escalados (usar print para visibilidad):
    valores_validos = long_v["valor_norm"].notna().sum()
    valores_no_cero = (long_v["valor_norm"] != 0).sum()
    print(f"      Valores escalados válidos: {valores_validos}/{len(long_v)}")
    print(f"      Valores no-cero: {valores_no_cero}")
    if valores_validos > 0:
        print(
            f"      Rango: min={long_v['valor_norm'].min():.4f}, "
            f"max={long_v['valor_norm'].max():.4f}, "
            f"mean={long_v['valor_norm'].mean():.4f}"
        )
    
    if valores_no_cero == 0:
        print(f"      ❌ ADVERTENCIA: TODOS los valores escalados son 0!")
        print(f"         Muestra valores pre-escala: {long_v['valor'].head(10).tolist()}")
        print(f"         Muestra valores post-escala: {long_v['valor_norm'].head(10).tolist()}")
    
    # Verificar que las columnas críticas existan
    # IMPORTANTE: Después de filtrar 'nan', todas las filas deberían tener Variedad válida
    # Pero aún necesitamos verificar que las columnas base existan
    missing_cols = [c for c in base_key_cols if c not in long_v.columns]
    if missing_cols:
        logger.error(f"to_wide: Faltan columnas críticas: {missing_cols}")
        return pd.DataFrame(columns=key_cols + ["VOLT_HUM", "VOLT_TEM"])
    
    # ANTES del pivot, verificar columnas críticas con logging detallado
    print(f"      🔍 PRE-PIVOT: Verificando columnas críticas en long_v:")
    print(f"         Total filas después de filtrar Variedad='nan': {len(long_v)}")
    
    for col in base_key_cols:
        null_count = long_v[col].isna().sum()
        print(f"         {col}: {null_count}/{len(long_v)} NaN")
        if null_count > 0 and null_count <= 10:
            # Mostrar muestra de filas con NaN solo si son pocas
            nan_rows = long_v[long_v[col].isna()].head(3)
            print(f"            Muestra filas con NaN en {col}:")
            print(f"               Variables: {nan_rows['variable'].tolist() if 'variable' in nan_rows.columns else 'N/A'}")
            print(f"               Timestamps: {nan_rows['timestamp'].tolist() if 'timestamp' in nan_rows.columns else 'N/A'}")
    
    # Verificar que no haya NaN en columnas críticas (base_key_cols)
    critical_mask = long_v[base_key_cols].notna().all(axis=1)
    if not critical_mask.all():
        invalid_count = (~critical_mask).sum()
        logger.warning(f"to_wide: {invalid_count}/{len(long_v)} filas tienen NaN en columnas críticas base")
        
        # Identificar QUÉ columnas tienen NaN
        for col in base_key_cols:
            if col in long_v.columns:
                nan_count = long_v[col].isna().sum()
                if nan_count > 0:
                    logger.warning(f"   Columna '{col}' tiene {nan_count} NaN")
        
        # INTENTAR RELLENAR valores faltantes antes de filtrar
        # Para columnas meta que pueden propagarse por timestamp
        for col in ["planta", "año", "sensor_id"]:
            if col in long_v.columns and long_v[col].isna().any():
                # Intentar rellenar usando ffill/bfill agrupado por timestamp (si existe)
                if "timestamp" in long_v.columns:
                    long_v[col] = long_v.groupby("timestamp", dropna=False)[col].transform(lambda x: x.ffill().bfill())
                    # Si aún hay NaN, intentar por variable
                    if long_v[col].isna().any():
                        long_v[col] = long_v.groupby("variable", dropna=False)[col].transform(lambda x: x.ffill().bfill())
                else:
                    long_v[col] = long_v.groupby("variable", dropna=False)[col].transform(lambda x: x.ffill().bfill())
        
        # Para timestamp, si hay NaN, intentar inferirlo desde otras columnas de tiempo
        if "timestamp" in long_v.columns and long_v["timestamp"].isna().any():
            # Intentar rellenar desde TimeString o otras columnas de tiempo
            time_cols = ["TimeString", "Date_raw", "LOC_time_raw"]
            for time_col in time_cols:
                if time_col in long_v.columns and long_v["timestamp"].isna().any():
                    # Para filas con timestamp NaN pero con TimeString válido
                    mask_ts_nan = long_v["timestamp"].isna()
                    if time_col == "TimeString":
                        long_v.loc[mask_ts_nan & long_v[time_col].notna(), "timestamp"] = pd.to_datetime(
                            long_v.loc[mask_ts_nan & long_v[time_col].notna(), time_col], errors="coerce"
                        )
                    # Si aún hay NaN, intentar combinar Date_raw y LOC_time_raw
                    if long_v["timestamp"].isna().any() and "Date_raw" in long_v.columns and "LOC_time_raw" in long_v.columns:
                        mask_ts_nan = long_v["timestamp"].isna()
                        date_raw_valid = long_v.loc[mask_ts_nan, "Date_raw"].notna()
                        time_raw_valid = long_v.loc[mask_ts_nan, "LOC_time_raw"].notna()
                        combined_valid = date_raw_valid & time_raw_valid
                        if combined_valid.any():
                            combined_datetime = pd.to_datetime(
                                long_v.loc[mask_ts_nan & combined_valid, "Date_raw"].astype(str) + " " + 
                                long_v.loc[mask_ts_nan & combined_valid, "LOC_time_raw"].astype(str),
                                errors="coerce"
                            )
                            long_v.loc[mask_ts_nan & combined_valid, "timestamp"] = combined_datetime
        
        # Recalcular critical_mask después de rellenar
        critical_mask = long_v[base_key_cols].notna().all(axis=1)
        still_invalid = (~critical_mask).sum()
        
        if still_invalid > 0:
            logger.warning(f"to_wide: Después de rellenar, aún quedan {still_invalid}/{len(long_v)} filas con NaN en columnas críticas")
            # SOLO filtrar si realmente no se pueden rellenar
            long_v = long_v[critical_mask].copy()
            if long_v.empty:
                logger.error("to_wide: No quedan filas válidas después de intentar rellenar y filtrar NaN en columnas críticas")
                return pd.DataFrame(columns=key_cols + ["VOLT_HUM", "VOLT_TEM"])
        else:
            print(f"      ✅ Todas las columnas críticas rellenadas correctamente")
    
    # 4) Pivot usando solo las claves base
    # Usar 'long_v' que ya está filtrado (sin 'nan' en Variedad y sin NaN en columnas críticas)
    print(f"      🔄 Ejecutando pivot con {len(long_v)} filas válidas...")
    try:
        wide = (
            long_v.pivot_table(
                index=key_cols,
                columns="var_norm",
                values="valor_norm",
                aggfunc="first",
            )
            .reset_index()
        )
    except Exception as e:
        logger.error(f"to_wide: Falló el pivot principal: {e}")
        logger.error(f"to_wide: long_v shape: {long_v.shape}")
        logger.error(f"to_wide: Valores nulos en key_cols:\n{long_v[key_cols].isna().sum()}")
        return pd.DataFrame(columns=key_cols + ["VOLT_HUM", "VOLT_TEM"])
    
    # AGREGAR validación post-pivot
    print(f"      🔍 POST-PIVOT: Estado de columnas críticas:")
    print(f"         Shape: {wide.shape}")
    if not wide.empty:
        for col in base_key_cols:
            if col in wide.columns:
                valid = wide[col].notna().sum()
                print(f"         {col}: {valid}/{len(wide)} válidos")
    else:
        print(f"         ⚠️ ADVERTENCIA: DataFrame wide está vacío después del pivot!")
    
    logger.info(f"✅ Pivot completado: {len(wide)} filas, {len(wide.columns)} columnas")
    
    # Verificar valores después del pivot (usar print para visibilidad)
    if "VOLT_HUM" in wide.columns:
        vh_valid = wide["VOLT_HUM"].notna().sum()
        vh_nonzero = (wide["VOLT_HUM"] != 0).sum()
        print(f"      VOLT_HUM después pivot: {vh_valid} válidos, {vh_nonzero} no-cero")
        if vh_valid > 0 and vh_nonzero == 0:
            print(f"         ⚠️ ADVERTENCIA: VOLT_HUM válido pero todos son 0!")
    if "VOLT_TEM" in wide.columns:
        vt_valid = wide["VOLT_TEM"].notna().sum()
        vt_nonzero = (wide["VOLT_TEM"] != 0).sum()
        print(f"      VOLT_TEM después pivot: {vt_valid} válidos, {vt_nonzero} no-cero")
        if vt_valid > 0 and vt_nonzero == 0:
            print(f"         ⚠️ ADVERTENCIA: VOLT_TEM válido pero todos son 0!")
    
    # 4.1) Adjuntar metadata del laboratorio agrupada por base_key_cols
    if meta_cols_for_merge:
        meta_cols_by_key = (
            long_all[base_key_cols + meta_cols_for_merge]
            .sort_values(base_key_cols)
            .groupby(base_key_cols, dropna=False)
            .agg({col: _first_valid_meta for col in meta_cols_for_merge})
            .reset_index()
        )
        wide = wide.merge(meta_cols_by_key, on=base_key_cols, how="left")
        logger.debug(f"to_wide: Metadata anexada: {meta_cols_for_merge}")
    else:
        logger.debug("to_wide: No hay columnas de metadata para anexar")
    
    # 4.2) Reinsertar columnas HumedadInicial y HumedadFinal desde long_all si no están ya presentes
    # Esto asegura que estas columnas sobrevivan el pivot incluso si no están en meta_cols_for_merge
    for col in ["HumedadInicial", "HumedadFinal"]:
        if col in long_all.columns and col not in wide.columns:
            # Agrupar por base_key_cols para obtener el valor único por timestamp
            meta_by_key = long_all.groupby(base_key_cols)[col].first().reset_index()
            # Merge con wide usando base_key_cols
            wide = wide.merge(meta_by_key[base_key_cols + [col]], on=base_key_cols, how="left")
            logger.debug(f"to_wide: Columna {col} reinsertada desde long_all")
    
    # 5) Re-anexar columnas 'raw' (las que NO están en key_cols)
    merge_keys = base_key_cols
    raw_keep_candidates = [
        "tirada_num",
        "Date_raw",
        "LOC_time_raw",
        "VarName",
        "TimeString",
        "VarValue",
        "Validity",
        "Time_ms",
        "VarName_original",
    ]
    raw_keep_cols = [c for c in raw_keep_candidates if c in long_all.columns]
    
    if raw_keep_cols:
        raw_data = (
            long_all[merge_keys + raw_keep_cols]
            .groupby(merge_keys, as_index=False)
            .first()
        )
        
        cols_before_merge = set(wide.columns)
        raw_data_cols_to_merge = [c for c in raw_keep_cols if c not in cols_before_merge]
        
        if raw_data_cols_to_merge:
            raw_data = normalize_timestamp(raw_data, "timestamp")
            wide = wide.merge(
                raw_data[merge_keys + raw_data_cols_to_merge], 
                on=merge_keys, 
                how="left", 
            )
        else:
            logger.debug("to_wide: No hay columnas nuevas para anexar desde raw_data")
    
    # 6) Orden de columnas (la lógica existente está bien, verificarla)
    volt_cols = [c for c in ["VOLT_HUM", "VOLT_TEM"] if c in wide.columns]
    lab_cols = [c for c in ["Variedad", "ID_tachada", "HumedadInicial", "HumedadFinal", "Descarte", "En_duda", "DESCARTAR"] if c in wide.columns]
    other_vars = [c for c in wide.columns if c not in (key_cols + raw_keep_cols + volt_cols + lab_cols)]
    
    # Asegurar un orden consistente de columnas
    final_cols_order = key_cols + volt_cols + lab_cols + raw_keep_cols + other_vars
    final_cols_existing = [c for c in final_cols_order if c in wide.columns]
    wide = wide[final_cols_existing]
    
    # 7) Drop seguro de columnas 'nan' (por si quedara alguna etiqueta rara)
    safe_cols = []
    for c in wide.columns:
        if isinstance(c, float) and pd.isna(c):
            continue
        if isinstance(c, str) and c.strip().lower() == "nan":
            continue
        safe_cols.append(c)
    wide = wide[safe_cols]
    
    # 8) Quitar columnas no deseadas configuradas
    # IMPORTANTE: Descartar HUMEDAD y TEMPERATURA originales del sensor
    # Solo debemos tener VOLT_HUM y VOLT_TEM. HUMEDAD y TEMPERATURA se calculan DESPUÉS desde las curvas.
    wide = wide.drop(columns=[c for c in DROP_WIDE_COLS if c in wide.columns], errors="ignore")
    
    # Verificar que no queden columnas HUMEDAD/TEMPERATURA originales (deben estar descartadas)
    original_temp_hum = [c for c in wide.columns if c in ["HUMEDAD", "TEMPERATURA"] and c not in ["VOLT_HUM", "VOLT_TEM"]]
    if original_temp_hum:
        logger.warning(f"to_wide: Se encontraron columnas de humedad/temperatura originales que deberían descartarse: {original_temp_hum}")
        wide = wide.drop(columns=original_temp_hum, errors="ignore")

    # Deduplicar nombres de columnas (seguridad final)
    if wide.columns.duplicated().any():
        duplicated_cols = list(wide.columns[wide.columns.duplicated()])
        logger.warning(
            "to_wide: Se detectaron columnas duplicadas en wide: %s. "
            "Conservando la primera aparición de cada una.",
            duplicated_cols,
        )
        wide = wide.loc[:, ~wide.columns.duplicated(keep="first")]
    
    logger.info("to_wide: resultado final: %d filas, %d columnas", len(wide), wide.shape[1])
    
    # Debug: si wide está vacío pero long_v no, hay un problema
    if wide.empty and not long_v.empty:
        # Verificar si hay NaN en key_cols que impida el pivot
        key_cols_with_nan = long_v[key_cols].isna().any(axis=1).sum()
        logger.warning(
            f"to_wide: long_v tiene {len(long_v)} filas pero wide está vacío. "
            f"Variables en long_v: {long_v['var_norm'].unique()}. "
            f"Filas con NaN en key_cols: {key_cols_with_nan}/{len(long_v)}"
        )
    
    return wide


def save_outputs_to_gdrive(
    gdrive: GoogleDriveClient,
    inv: pd.DataFrame,
    long_all: pd.DataFrame,
    wide: pd.DataFrame,
    log_df: pd.DataFrame,
    qa: pd.DataFrame,
    planta: str,
    output_path: str,
) -> Dict[str, Any]:
    """
    Guarda outputs consolidados a Google Drive como Excel.
    
    Similar a save_outputs() del notebook pero sube a Google Drive.
    """
    output = io.BytesIO()
    
    # Intentar usar xlsxwriter, si no está disponible usar openpyxl
    try:
        import xlsxwriter
        engine = "xlsxwriter"
        engine_kwargs = {"datetime_format": "yyyy-mm-dd HH:MM:SS"}
    except ImportError:
        engine = "openpyxl"
        engine_kwargs = {}
    
    # IMPORTANTE: Convertir timestamps con timezone a timezone-naive para Excel
    # Excel no soporta timestamps con timezone-aware, así que los convertimos a naive
    # Aplicar a wide, log_df y qa
    def _convert_timezone_aware_to_naive(df: pd.DataFrame) -> pd.DataFrame:
        """Convierte todas las columnas datetime con timezone a naive"""
        if df.empty:
            return df
        df = df.copy()
        for col in df.columns:
            try:
                # Verificar si es una columna datetime
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    continue
                
                # Convertir a datetime si no lo es
                ts = pd.to_datetime(df[col], errors="coerce")
                
                # Convertir TODOS los valores datetime a naive (forzar)
                # Usar apply para convertir cada valor individualmente si tiene timezone
                def convert_val(val):
                    if pd.isna(val):
                        return val
                    try:
                        if hasattr(val, "tz") and val.tz is not None:
                            return val.tz_convert("UTC").tz_localize(None)
                        return val
                    except Exception:
                        return val
                
                df[col] = ts.apply(convert_val)
            except Exception as e:
                # Si falla, intentar conversión directa de la serie
                try:
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        ts = pd.to_datetime(df[col], errors="coerce")
                        # Intentar convertir toda la serie de una vez
                        try:
                            df[col] = ts.dt.tz_convert("UTC").dt.tz_localize(None)
                        except (TypeError, AttributeError):
                            # Si no tiene timezone, dejar como está
                            df[col] = ts
                except Exception:
                    pass
        return df
    
    wide = _convert_timezone_aware_to_naive(wide)
    if not log_df.empty:
        log_df = _convert_timezone_aware_to_naive(log_df)
    if not qa.empty:
        qa = _convert_timezone_aware_to_naive(qa)
        # Asegurar que fechas_min y fechas_max estén en formato naive
        for col in ["fechas_min", "fechas_max"]:
            if col in qa.columns:
                try:
                    ts = pd.to_datetime(qa[col], errors="coerce")
                    # Convertir a naive si tiene timezone
                    if ts.dtype.name.startswith("datetime64[ns, UTC]") or (
                        hasattr(ts.dtype, "tz") and ts.dtype.tz is not None
                    ):
                        qa[col] = ts.dt.tz_convert("UTC").dt.tz_localize(None)
                    else:
                        # Verificar valores individuales
                        qa[col] = ts.apply(lambda x: x.tz_convert("UTC").tz_localize(None) if hasattr(x, "tz") and x.tz is not None and not pd.isna(x) else x)
                except Exception:
                    pass
    
    # NUEVO: ELIMINAR COLUMNAS DUPLICADAS ANTES DE GUARDAR
    print(f"   🔍 Limpiando columnas duplicadas en wide...")
    
    # 1. Identificar columnas duplicadas
    cols_before = list(wide.columns)
    
    # 2. Eliminar duplicados usando pandas (más robusto)
    duplicated_mask = wide.columns.duplicated(keep='first')
    if duplicated_mask.any():
        duplicated_cols = wide.columns[duplicated_mask].tolist()
        print(f"      ⚠️  Eliminando {len(duplicated_cols)} columnas duplicadas: {list(set(duplicated_cols))}")
        wide = wide.loc[:, ~duplicated_mask]
    
    # 3. Verificar que no queden duplicados
    duplicated_cols = [col for col in wide.columns if wide.columns.tolist().count(col) > 1]
    if duplicated_cols:
        print(f"      ❌ ADVERTENCIA: Aún quedan columnas duplicadas: {list(set(duplicated_cols))}")
        # Forzar eliminación usando drop_duplicates en columnas
        wide = wide.loc[:, ~wide.columns.duplicated(keep='first')]
    
    print(f"      ✅ Columnas después de limpieza: {len(wide.columns)} (antes: {len(cols_before)})")
    
    # VALIDACIÓN FINAL: Verificar estructura del DataFrame
    print(f"   📊 Estructura final del archivo Excel:")
    print(f"      Total filas: {len(wide)}")
    print(f"      Total columnas: {len(wide.columns)}")
    
    # Verificar duplicados una última vez
    duplicated_cols = [col for col in wide.columns if wide.columns.tolist().count(col) > 1]
    if duplicated_cols:
        print(f"      ❌ ERROR: Columnas duplicadas encontradas: {list(set(duplicated_cols))}")
        # Forzar eliminación
        wide = wide.loc[:, ~wide.columns.duplicated(keep='first')]
        print(f"      ✅ Duplicados eliminados. Columnas finales: {len(wide.columns)}")
    
    # Mostrar columnas en orden
    expected_order = [
        "planta", "año", "sensor_id", "timestamp", "Variedad", "ID_tachada",
        "VOLT_HUM", "VOLT_TEM", "TEMPERATURA", "HUMEDAD",
        "Descarte", "En_duda", "source_file", "source_path"
    ]
    present_cols = [c for c in expected_order if c in wide.columns]
    print(f"      Columnas principales: {', '.join(present_cols)}")
    
    with pd.ExcelWriter(output, engine=engine, **engine_kwargs) as writer:
        # Datos wide (hoja principal, como en el archivo de ejemplo)
        # Asegurar orden de columnas: planta, año, tirada_fecha, sensor_id, timestamp, tirada_num, VOLT_HUM, VOLT_TEM, TEMPERATURA, HUMEDAD
        wide_cols_order = [
            "planta", "año", "tirada_fecha", "sensor_id", "timestamp", "tirada_num",
            "VOLT_HUM", "VOLT_TEM", "TEMPERATURA", "HUMEDAD", "Variedad", "ID_tachada", "DESCARTAR"
        ]
        wide_cols_final = [c for c in wide_cols_order if c in wide.columns]
        # Agregar cualquier columna adicional que no esté en el orden
        wide_cols_final.extend([c for c in wide.columns if c not in wide_cols_final])
        wide_ordered = wide[wide_cols_final]
        wide_ordered.to_excel(writer, sheet_name="datos_wide", index=False)
        
        # Diccionario (formato exacto del archivo de ejemplo)
        # Solo incluir las columnas que realmente están en datos_wide
        dicc_cols = []
        dicc_desc = []
        
        for col, desc in [
            ("planta", "Planta origen (JPV)" if planta == "JPV" else "Planta origen (RB)"),
            ("año", "Año"),
            ("tirada_num", "N° tirada (si estaba)"),
            ("tirada_fecha", "Fecha tirada"),
            ("sensor_id", "ID sensor"),
            ("timestamp", "Timestamp unificado"),
        ]:
            if col in wide.columns:
                dicc_cols.append(col)
                dicc_desc.append(desc)
        
        # Agregar columnas específicas según tipo de planta
        if planta == "JPV":
            if "TimeString" in wide.columns:
                dicc_cols.append("TimeString")
                dicc_desc.append("Tiempo crudo JPV")
        else:  # RB
            if "Date_raw" in wide.columns:
                dicc_cols.append("Date_raw")
                dicc_desc.append("Fecha cruda RB")
            if "LOC_time_raw" in wide.columns:
                dicc_cols.append("LOC_time_raw")
                dicc_desc.append("Hora local cruda RB")
        
        # Voltajes (como en el notebook: menciona V_HUM/V_TEM pero las columnas son VOLT_HUM/VOLT_TEM)
        if "VOLT_HUM" in wide.columns:
            dicc_cols.append("VOLT_HUM")
            dicc_desc.append("Voltaje humedad" if planta == "JPV" else "Voltaje humedad (÷100)")
        if "VOLT_TEM" in wide.columns:
            dicc_cols.append("VOLT_TEM")
            dicc_desc.append("Voltaje temperatura" if planta == "JPV" else "Voltaje temperatura (÷100)")
        
        # Valores calibrados
        if "TEMPERATURA" in wide.columns:
            dicc_cols.append("TEMPERATURA")
            dicc_desc.append("Temperatura (°C) - calculada desde curvas de calibración")
        if "HUMEDAD" in wide.columns:
            dicc_cols.append("HUMEDAD")
            dicc_desc.append("Humedad (%) - calculada desde curvas de calibración")
        
        # Columnas de laboratorio
        if "Variedad" in wide.columns:
            dicc_cols.append("Variedad")
            dicc_desc.append("Variedad de arroz (cruzada con laboratorio)")
        if "ID_tachada" in wide.columns:
            dicc_cols.append("ID_tachada")
            dicc_desc.append("ID de tachada (cruzada con laboratorio)")
        if "DESCARTAR" in wide.columns:
            dicc_cols.append("DESCARTAR")
            dicc_desc.append("Flag de descarte (cruzada con laboratorio)")
        
        # Metadata de archivos
        if "source_file" in wide.columns:
            dicc_cols.append("source_file")
            dicc_desc.append("Archivo fuente")
        if "source_path" in wide.columns:
            dicc_cols.append("source_path")
            dicc_desc.append("Ruta fuente")
        
        dicc = pd.DataFrame({
            "columna": dicc_cols,
            "descripcion": dicc_desc,
        })
        dicc.to_excel(writer, sheet_name="diccionario", index=False)
        
        # QA resumen (formato exacto del archivo de ejemplo)
        qa.to_excel(writer, sheet_name="qa_resumen", index=False)
    
    output.seek(0)
    excel_bytes = output.read()
    
    # Subir a Google Drive
    upload_result = gdrive.upload_file(
        output_path,
        excel_bytes,
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    
    return upload_result

