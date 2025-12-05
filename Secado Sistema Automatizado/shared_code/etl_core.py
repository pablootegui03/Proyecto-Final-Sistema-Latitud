import io
import logging
import re
from typing import Optional, Dict, Any, List

import numpy as np
import pandas as pd

from shared_code.time_utils import normalize_timestamp


logger = logging.getLogger(__name__)


def _decode_bytes(content: bytes) -> str:
    """
    Decode bytes to text trying common encodings for sensor files.

    Tries UTF-16 first (JPV TXT), then UTF-8 with/without BOM.
    """
    for enc in ("utf-16", "utf-8-sig", "utf-8"):
        try:
            return content.decode(enc)
        except UnicodeDecodeError:
            continue
    # As last resort, ignore errors
    return content.decode("utf-8", errors="ignore")


def _canon(s: str) -> str:
    """Normaliza string: mayúsculas, sin espacios/guiones/puntos/paréntesis"""
    return re.sub(r"[\s\-\.\(\)]", "", str(s).upper())


def _parse_datetime_columns(df: pd.DataFrame, filename: str) -> pd.Series:
    """
    Build a pandas datetime series from common date/time column patterns.

    Handles cases where a single column represents the timestamp or where date
    and time are split across two columns.
    """
    # Para JPV: buscar TimeString explícitamente
    if "TimeString" in df.columns:
        ts = pd.to_datetime(df["TimeString"], errors="coerce", dayfirst=False)
        if ts.notna().any():
            return ts
    
    candidates_single = [
        "timestamp",
        "datetime",
        "date_time",
        "time_stamp",
        "time",
        "fecha_hora",
        "timestring",  # lowercase version
    ]
    lower_cols = {c.lower(): c for c in df.columns}
    for lc in candidates_single:
        if lc in lower_cols:
            ts = pd.to_datetime(df[lower_cols[lc]], errors="coerce", dayfirst=False)
            if ts.notna().any():
                return ts

    # Try separate date and time columns (RB) - usando _canon() como en el notebook
    date_col = None
    lt_col = None
    for c in df.columns:
        cc = _canon(c)
        if cc in ("DATE", "FECHA"):
            date_col = c
        if cc in ("LOCTIME", "LOCTIEMPO", "LOCALTIME"):
            lt_col = c
    
    if date_col and lt_col:
        dt = (
            df[date_col].astype(str).str.strip()
            + " "
            + df[lt_col].astype(str).str.strip()
        )
        ts = pd.to_datetime(dt, errors="coerce", dayfirst=False)
        return ts
    elif date_col:
        ts = pd.to_datetime(df[date_col], errors="coerce", dayfirst=False)
        return ts

    logger.warning("Timestamp columns not detected in file: %s", filename)
    return pd.to_datetime(pd.Series([pd.NaT] * len(df)))


def read_jpv_txt(file_content: bytes, filename: str) -> pd.DataFrame:
    """
    Read JPV TXT files and return a normalized long-format DataFrame.

    - Decodes as UTF-16 (fallbacks handled).
    - Filters out metadata variables starting with "$RT_".
    - Normalizes variable names and converts values to numeric.

    Returns a DataFrame with columns: timestamp, variable, valor.

    Example
    -------
    >>> data = "Time\tVarName\tVarValue\n2024-10-01 10:00:00\tV_HUM\t45,2".encode("utf-16")
    >>> df = read_jpv_txt(data, "SENSOR10_JPV_2024.txt")
    >>> set(df.columns) == {"timestamp", "variable", "valor"}
    True
    """
    # Intentos de lectura con diferentes codificaciones y delimitadores
    # JPV usa típicamente tabulaciones y UTF-16
    tried = [
        ("utf-16", "\t"),
        ("utf-16le", "\t"),
        ("utf-8", "\t"),
    ]
    
    df = None
    last_err = None
    
    for enc, sep in tried:
        try:
            text = file_content.decode(enc)
            buf = io.StringIO(text)
            df = pd.read_csv(
                buf,
                sep=sep,
                engine="python",
                on_bad_lines="skip",  # Saltar líneas malformadas
            )
            break
        except (UnicodeDecodeError, Exception) as e:
            last_err = e
            continue
    
    # Si falló todo, intentar con auto-detección
    if df is None:
        try:
            text = _decode_bytes(file_content)
            buf = io.StringIO(text)
            df = pd.read_csv(
                buf,
                sep=None,
                engine="python",
                on_bad_lines="skip",  # Saltar líneas malformadas
            )
        except Exception as e:
            last_err = e
    
    if df is None:
        logger.error("Failed to read JPV TXT '%s': %s", filename, last_err)
        raise last_err or RuntimeError(f"No se pudo leer {filename}")

    # Filtrar metadatos ($RT_*)
    if "VarName" in df.columns:
        df = df[~df["VarName"].astype(str).str.startswith("$RT_")]
    
    # Selección mínima, preservando trazabilidad (como en el notebook)
    keep_cols = [c for c in ["VarName", "TimeString", "VarValue", "Validity", "Time_ms"] if c in df.columns]
    df = df[keep_cols].copy()
    
    # Timestamp desde TimeString (como en el notebook)
    if "TimeString" in df.columns:
        df["timestamp"] = pd.to_datetime(df["TimeString"], errors="coerce")
    else:
        # Fallback a _parse_datetime_columns si no hay TimeString
        df["timestamp"] = _parse_datetime_columns(df, filename)
    
    # Variable normalizada y original (como en el notebook)
    if "VarName" in df.columns:
        df["VarName_original"] = df["VarName"].astype(str)
        df["variable"] = df["VarName"].astype(str).str.replace(r"^\d+_", "", regex=True)
    else:
        df["VarName_original"] = None
        df["variable"] = "Var"
    
    # VarValue -> número (coma decimal → punto, como en el notebook)
    if "VarValue" in df.columns:
        s = df["VarValue"].astype(str).str.strip()
        # Cambiamos coma por punto solo si parece número con coma
        s = pd.Series([
            val.replace(",", ".") if re.search(r"^\s*-?\d+,\d+\s*$", val) else val
            for val in s
        ])
        df["valor"] = pd.to_numeric(s, errors="coerce")
        
        # VALIDACIÓN CRÍTICA: Verificar que los valores se convirtieron correctamente
        valores_validos = df["valor"].notna().sum()
        valores_no_cero = (df["valor"] != 0).sum()
        
        # USAR print() para que sea visible siempre
        print(f"   📊 JPV '{filename}': {valores_validos}/{len(df)} valores válidos, {valores_no_cero} no-cero")
        
        if valores_validos == 0:
            print(f"   ⚠️ ADVERTENCIA: NINGÚN valor válido después de conversión en '{filename}'")
            # Mostrar muestra de valores originales
            print(f"      Muestra VarValue original: {df['VarValue'].head(10).tolist()}")
            print(f"      Muestra después de str.strip: {s.head(10).tolist()}")
        elif valores_no_cero == 0:
            print(f"   ⚠️ ADVERTENCIA: Todos los valores convertidos son 0 en '{filename}'")
            print(f"      Muestra VarValue original: {df['VarValue'].head(10).tolist()}")
            print(f"      Muestra valores convertidos: {df['valor'].head(10).tolist()}")
    else:
        logger.warning(f"JPV archivo '{filename}': No se encontró columna VarValue")
        df["valor"] = pd.NA
    
    # Debug: Variables y valores de muestra
    logger.debug(f"JPV archivo '{filename}': Variables únicas: {df['variable'].unique()[:10]}")
    if df["valor"].notna().any():
        logger.debug(
            f"JPV archivo '{filename}': Rango valores: "
            f"min={df['valor'].min():.4f}, max={df['valor'].max():.4f}, "
            f"mean={df['valor'].mean():.4f}"
        )
    
    # Retornar solo columnas necesarias
    result_cols = ["timestamp", "variable", "valor"]
    if "VarName_original" in df.columns:
        result_cols.insert(1, "VarName_original")
    if "TimeString" in df.columns:
        result_cols.append("TimeString")
    if "VarValue" in df.columns:
        result_cols.append("VarValue")
    
    out = df[[c for c in result_cols if c in df.columns]].copy()
    # Filtrar filas sin timestamp válido
    out = out[out["timestamp"].notna()].reset_index(drop=True)
    
    return out


def read_rb_csv(file_content: bytes, filename: str) -> pd.DataFrame:
    """
    Read RB CSV files and return a normalized long-format DataFrame.
    
    Devuelve EXACTAMENTE la misma estructura que read_jpv_txt():
    - Columnas: timestamp, variable, valor, Date_raw, LOC_time_raw
    - Variable mapeada: V_Hum/V_HUM -> VOLT_HUM, V_Tem/V_TEM -> VOLT_TEM (igual que JPV produce)
    - Valores divididos por 100 (RB_VOLT_SCALE = 0.01) para equiparar con JPV
    
    NOTA: La calibración se aplica después en el pipeline cuando hay información del laboratorio,
    al igual que JPV. Esta función solo prepara los voltajes normalizados.
    
    Maneja múltiples formatos de archivos RB:
    - Separadores: ';' o ','
    - Columnas de fecha: "Date", "Fecha"
    - Columnas de hora: "Time", "Hora", "LOC_time", "LOCTime"
    - Columnas de voltaje: "V_Hum", "V_HUM", "V_Hum", "V_Tem", "V_TEM", "V_Temp", etc.
    
    Returns a DataFrame with columns: timestamp, variable, valor, Date_raw, LOC_time_raw.
    
    Example
    -------
    >>> data = "Date;Time;V_Hum;V_Tem\n2024-10-01;10:00:00;4520;2380".encode("utf-8")
    >>> df = read_rb_csv(data, "SENSOR1_RB_2024.csv")
    >>> set(df["variable"].unique()) == {"VOLT_HUM", "VOLT_TEM"}
    True
    """
    text = _decode_bytes(file_content)
    
    # 1. Probar primero con separador ';' (formato común en RB)
    buf = io.StringIO(text)
    try:
        df = pd.read_csv(buf, sep=';', engine="python")
        # Si solo tiene 1 columna, probablemente el separador es incorrecto
        if len(df.columns) == 1:
            buf = io.StringIO(text)
            df = pd.read_csv(buf, sep=',', engine="python")
    except Exception:
        # Si falla con ';', intentar con ','
        try:
            buf = io.StringIO(text)
            df = pd.read_csv(buf, sep=',', engine="python")
        except Exception as exc:
            logger.error("Failed to read RB CSV '%s': %s", filename, exc)
            raise
    
    # 2. Detectar columnas de fecha/hora de manera robusta
    date_col = None
    time_col = None
    for c in df.columns:
        c_lower = str(c).lower().strip()
        # Detectar columna de fecha
        if c_lower in ("date", "fecha"):
            date_col = c
        # Detectar columna de hora (más flexible)
        if c_lower in ("time", "hora", "loc_time", "loctime", "localtime", "localtiempo"):
            time_col = c
    
    # Guardar Date_raw y LOC_time_raw para consistencia (igual que JPV tiene TimeString)
    if date_col:
        df["Date_raw"] = df[date_col].copy()
    else:
        df["Date_raw"] = None
    
    if time_col:
        df["LOC_time_raw"] = df[time_col].copy()
    else:
        df["LOC_time_raw"] = None
    
    # 3. Construir timestamp (igual que JPV)
    if date_col and time_col:
        df["timestamp"] = pd.to_datetime(
            df[date_col].astype(str).str.strip() + " " + df[time_col].astype(str).str.strip(),
            dayfirst=False,
            errors="coerce"
        )
    elif date_col:
        df["timestamp"] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=False)
    else:
        logger.warning("No se encontraron columnas de fecha/hora en archivo RB '%s'. Columnas disponibles: %s", filename, list(df.columns))
        df["timestamp"] = pd.NaT

    # 4. Detectar variables RB de manera robusta (V_Hum, V_HUM, V_Tem, V_TEM, etc.)
    # Normalizar nombres: quitar guiones bajos y convertir a minúsculas para comparar
    # Mapear directamente a VOLT_HUM y VOLT_TEM (como JPV)
    value_cols_map = {}
    for c in df.columns:
        c_normalized = str(c).lower().replace("_", "").replace("-", "").strip()
        # Detectar humedad: buscar "vhum" en el nombre (V_HUM)
        if c_normalized == "vhum":
            value_cols_map[c] = "VOLT_HUM"
        # Detectar temperatura: buscar "vtem" en el nombre (V_TEM)
        elif c_normalized in ("vtem", "vtemp"):
            value_cols_map[c] = "VOLT_TEM"

    value_cols = list(value_cols_map.keys())
    
    # Si no encontramos columnas de voltaje, devolver estructura vacía con columnas correctas
    if not value_cols:
        logger.warning("No se encontraron columnas V_HUM/V_TEM en archivo RB '%s'. Columnas disponibles: %s", filename, list(df.columns))
        out = pd.DataFrame({
            "timestamp": pd.NaT,
            "variable": None,
            "valor": None,
            "Date_raw": None,
            "LOC_time_raw": None
        })
        return out

    # 5. Melt a formato largo (igual que JPV)
    id_cols = ["timestamp", "Date_raw", "LOC_time_raw"]
    long_df = df[id_cols + value_cols].melt(
        id_vars=id_cols,
        var_name="variable_raw",
        value_name="valor_raw"
    )
    
    # Mapear directamente a VOLT_HUM/VOLT_TEM (como JPV)
    long_df["variable"] = long_df["variable_raw"].map(value_cols_map)

    # 6. Valor a numérico (coma→punto si corresponde, igual que JPV)
    s = long_df["valor_raw"].astype(str).str.strip()
    s = pd.Series([
        val.replace(",", ".") if re.search(r"^\s*-?\d+,\d+\s*$", val) else val
        for val in s
    ])
    long_df["valor"] = pd.to_numeric(s, errors="coerce")
    
    # 7. Aplicar escala RB: dividir por 100 (RB_VOLT_SCALE = 0.01)
    # Esto equipara los valores de RB con los de JPV
    long_df["valor"] = long_df["valor"] * 0.01
    
    # VALIDACIÓN CRÍTICA: Verificar que los valores se convirtieron correctamente
    valores_validos = long_df["valor"].notna().sum()
    valores_no_cero = (long_df["valor"] != 0).sum()
    
    # USAR print() para que sea visible siempre
    print(f"   📊 RB '{filename}': {valores_validos}/{len(long_df)} valores válidos, {valores_no_cero} no-cero")
    
    if valores_validos == 0:
        print(f"   ⚠️ ADVERTENCIA: NINGÚN valor válido en archivo RB '{filename}'")
        print(f"      Muestra valor_raw: {long_df['valor_raw'].head(10).tolist()}")
        print(f"      Muestra después de str.strip: {s.head(10).tolist()}")
    elif valores_no_cero == 0:
        print(f"   ⚠️ ADVERTENCIA: Todos los valores convertidos son 0 en archivo RB '{filename}'")
        print(f"      Muestra valor_raw: {long_df['valor_raw'].head(10).tolist()}")
        print(f"      Muestra valores convertidos (después de escala x0.01): {long_df['valor'].head(10).tolist()}")
    else:
        # Mostrar rango de valores después de aplicar escala
        print(f"      Rango valores (después de escala x0.01): min={long_df['valor'].min():.4f}, max={long_df['valor'].max():.4f}")

    # Debug: Variables y valores de muestra (igual que JPV)
    logger.debug(f"RB archivo '{filename}': Variables únicas: {long_df['variable'].unique()[:10]}")
    if long_df["valor"].notna().any():
        logger.debug(
            f"RB archivo '{filename}': Rango valores (después de escala x0.01): "
            f"min={long_df['valor'].min():.4f}, max={long_df['valor'].max():.4f}, "
            f"mean={long_df['valor'].mean():.4f}"
        )

    # 9. Retornar columnas en el orden esperado (igual que JPV pero con Date_raw y LOC_time_raw)
    # Eliminar cualquier columna cruda que no se use
    result_cols = ["timestamp", "variable", "valor", "Date_raw", "LOC_time_raw"]
    out = long_df[result_cols].copy()
    
    # Filtrar filas sin timestamp válido (igual que JPV)
    out = out[out["timestamp"].notna()].reset_index(drop=True)
    
    return out


def extract_sensor_id_from_name(name: str) -> Optional[int]:
    """
    Extract sensor ID from a filename or label.

    - JPV: SENSOR10,20,...,60 → 1..6
    - RB: SENSOR1,2,3,4 → 1..4

    Example
    -------
    >>> extract_sensor_id_from_name("JPV_SENSOR30_file.txt")
    3
    >>> extract_sensor_id_from_name("RB_SENSOR4_2024.csv")
    4
    """
    m = re.search(r"SENSOR\s*([0-9]+)", name, flags=re.IGNORECASE)
    if not m:
        return None
    num = int(m.group(1))
    if num >= 10 and num % 10 == 0:
        return num // 10
    return num


def parse_metadata_from_path(filename: str) -> Dict[str, Any]:
    """
    Parse metadata from a filename/path.

    Extracts: sensor_id, planta (JPV/RB), año (year).

    Example
    -------
    >>> parse_metadata_from_path("/data/JPV/SENSOR20_2023_log.txt")
    {'sensor_id': 2, 'planta': 'JPV', 'anio': 2023}
    """
    planta = None
    if re.search(r"\bJPV\b", filename, re.IGNORECASE):
        planta = "JPV"
    elif re.search(r"\bRB\b", filename, re.IGNORECASE):
        planta = "RB"

    sensor_id = extract_sensor_id_from_name(filename)

    year_match = re.search(r"\b(20[0-9]{2})\b", filename)
    anio = int(year_match.group(1)) if year_match else None

    return {"sensor_id": sensor_id, "planta": planta, "anio": anio}


def consolidate_sensor_data(
    file_content: bytes,
    filename: str,
    planta: str,
) -> pd.DataFrame:
    """
    Consolidate one sensor file into a normalized long-format DataFrame.

    - Detects format by `planta` (JPV or RB) and delegates to the proper reader.
    - Adds metadata columns: planta, sensor_id, source_file.
    - Drops duplicates and ensures consistent column names.

    Parameters
    ----------
    file_content : bytes
        In-memory file bytes (no direct file I/O performed).
    filename : str
        Original filename or path used only for metadata/logging.
    planta : str
        Either "JPV" or "RB". If ambiguous, detection based on filename is attempted.

    Returns
    -------
    pd.DataFrame
        Columns: timestamp, variable, valor, planta, sensor_id, source_file

    Example
    -------
    >>> csv = "Date,LOC_time,V_HUM,V_TEM\n2024-10-01,10:00:00,45.2,23.8".encode("utf-8")
    >>> df = consolidate_sensor_data(csv, "SENSOR1_RB_2024.csv", "RB")
    >>> set(["timestamp", "variable", "valor", "planta", "sensor_id", "source_file"]).issubset(df.columns)
    True
    """
    detected_planta = planta.upper().strip() if planta else None
    if detected_planta not in {"JPV", "RB"}:
        if re.search(r"\bJPV\b", filename, re.IGNORECASE):
            detected_planta = "JPV"
        elif re.search(r"\bRB\b", filename, re.IGNORECASE):
            detected_planta = "RB"

    if detected_planta == "JPV":
        try:
            df = read_jpv_txt(file_content, filename)
        except Exception as exc:
            logger.error("JPV parsing failed for '%s': %s", filename, exc)
            raise
    elif detected_planta == "RB":
        try:
            df = read_rb_csv(file_content, filename)
        except Exception as exc:
            logger.error("RB parsing failed for '%s': %s", filename, exc)
            raise
    else:
        logger.error("Unable to detect planta for file '%s'", filename)
        raise ValueError("Planta must be 'JPV' or 'RB'")

    meta = parse_metadata_from_path(filename)
    df["planta"] = detected_planta
    df["sensor_id"] = meta.get("sensor_id")
    df["source_file"] = filename

    df = df.drop_duplicates(subset=["timestamp", "variable"]).reset_index(drop=True)
    df = normalize_timestamp(df, "timestamp", assume_local=True)
    
    # Columnas base siempre presentes
    result_cols = ["timestamp", "variable", "valor", "planta", "sensor_id", "source_file"]
    
    # Incluir columnas de trazabilidad si existen (Date_raw, LOC_time_raw para RB; TimeString, etc. para JPV)
    optional_cols = ["Date_raw", "LOC_time_raw", "TimeString", "VarName_original", "VarValue"]
    for col in optional_cols:
        if col in df.columns:
            result_cols.append(col)
    
    return df[[c for c in result_cols if c in df.columns]]


