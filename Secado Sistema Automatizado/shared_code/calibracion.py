"""
Módulo para aplicar curvas de calibración de temperatura y humedad.

Convierte voltajes (VOLT_HUM, VOLT_TEM) a valores reales (HUMEDAD, TEMPERATURA)
usando curvas de calibración por planta, año y variedad.
"""

import io
import logging
import re
import tempfile
import unicodedata
from pathlib import Path
from typing import Dict, Optional, Tuple, Any

import numpy as np
import pandas as pd

from shared_code.time_utils import normalize_timestamp

logger = logging.getLogger(__name__)

# Equivalencias de variedades (robustez)
ALIAS_EQUIV = {
    "merin": {"merin", "l5903"},
    "l5903": {"merin", "l5903"},
    "slio9193": {"slio9193", "sli9193", "9193"},
    "sli9193": {"slio9193", "sli9193", "9193"},
    "9193": {"slio9193", "sli9193", "9193"},
    # Alias para INOV
    "inov": {"inov", "innov", "inovacion"},
    "innov": {"inov", "innov", "inovacion"},
    "inovacion": {"inov", "innov", "inovacion"},
}


def find_calibration_files(gdrive, planta: str, raw_path: str) -> Dict[int, str]:
    """
    Busca archivos de calibración recursivamente desde raw_path.
    
    Replica exactamente la lógica del notebook path_curvas():
    - Busca usando patrón "*Datos {PLANTA}*/*{AÑO}*Curvas*{PLANTA}*.xlsx"
    - Busca recursivamente (equivalente a rglob() en filesystem)
    
    Args:
        gdrive: Instancia de GoogleDriveClient
        planta: "JPV" o "RB"
        raw_path: Path base raw (ej: "Secado_Arroz/JPV/raw")
    
    Returns:
        {año: gdrive_path} ej: {2024: "Secado_Arroz/JPV/raw/calibracion/2024 Curvas JPV.xlsx"}
    """
    # Obtener base_path (equivalente a BASE_EDITABLE en el notebook)
    # El notebook busca desde BASE_EDITABLE recursivamente
    base_path = raw_path.split("/")[0] if "/" in raw_path else raw_path  # ej: "Secado_Arroz"
    
    # Patrones del notebook (exactos):
    # JPV: "*Datos JPV*/*{año}*Curvas*JPV*.xlsx"
    # RB:  "*Datos RB*/*{año}*Curvas*RB*.xlsx"
    # Pero como buscamos recursivamente, usamos un patrón más flexible que acepte cualquier ubicación
    
    calibracion_files = {}
    folder_mime = "application/vnd.google-apps.folder"
    files_checked = []  # Para debug
    
    def _search_recursive(folder_path: str, current_depth: int = 0, max_depth: int = 15):
        """
        Busca recursivamente archivos de curvas de calibración.
        Equivalente a rglob() del notebook.
        """
        if current_depth > max_depth:
            return
        
        try:
            items = gdrive.list_files(folder_path)
        except (FileNotFoundError, Exception) as e:
            logger.debug(f"Error listando {folder_path}: {e}")
            return
        
        for item in items:
            item_name = item.get("name", "")
            item_mime = item.get("mimeType", "")
            
            # Construir path completo para este item
            if folder_path == base_path:
                item_path = f"{folder_path}/{item_name}"
            else:
                item_path = f"{folder_path}/{item_name}"
            
            if item_mime == folder_mime:
                # Es una carpeta: buscar recursivamente (rglob behavior)
                # Omitir carpeta laboratorio (no contiene curvas)
                if item_name.lower() not in ["laboratorio"]:
                    _search_recursive(item_path, current_depth + 1, max_depth)
            else:
                # Es un archivo: verificar si es un archivo de curvas
                if not item_name.lower().endswith(".xlsx"):
                    continue
                
                files_checked.append(item_name)  # Para debug
                
                # Patrón del notebook: "*{AÑO}*Curvas*{PLANTA}*.xlsx"
                # Más flexible: buscar año y planta en cualquier orden
                pattern = rf"(\d{{4}}).*?Curvas.*?{re.escape(planta)}"
                m = re.search(pattern, item_name, re.IGNORECASE)
                
                if m:
                    year = int(m.group(1))
                    if year not in calibracion_files:  # Si hay duplicados, usar el primero encontrado
                        calibracion_files[year] = item_path
                        logger.info(f"Archivo de curvas encontrado: {item_path} (año {year})")
    
    # Buscar recursivamente desde base_path (como rglob() en el notebook)
    # El notebook usa: candidatos = list(BASE_EDITABLE.rglob(patron))
    logger.debug(f"Buscando archivos de curvas recursivamente desde: {base_path}")
    logger.debug(f"Patrón esperado: '*{{AÑO}}*Curvas*{planta}*.xlsx'")
    _search_recursive(base_path, max_depth=15)
    
    # Debug: si no se encontró nada, mostrar qué archivos Excel se vieron
    if not calibracion_files and files_checked:
        excel_files = [f for f in files_checked if f.lower().endswith(".xlsx")][:10]
        logger.debug(f"No se encontraron archivos de curvas. Archivos Excel vistos (primeros 5): {excel_files[:5]}")
    
    return calibracion_files


def select_calibration_file(
    calibracion_files: Dict[int, str],
    target_year: int,
    planta: str,
) -> Optional[Tuple[int, str]]:
    """
    Selecciona el archivo de calibración más adecuado para un año dado.
    
    Prioridad:
        1. Archivo del año exacto (target_year)
        2. Archivo más reciente anterior a target_year
        3. Archivo más reciente disponible (sin importar año)
    """
    if not calibracion_files:
        return None

    if target_year in calibracion_files:
        path = calibracion_files[target_year]
        logger.info(
            "Archivo de curvas encontrado: %s (año %s)",
            path,
            target_year,
        )
        return target_year, path

    previous_years = [year for year in calibracion_files if year < target_year]
    if previous_years:
        chosen_year = max(previous_years)
        path = calibracion_files[chosen_year]
        logger.warning(
            "No hay curvas específicas para año %s en %s. "
            "Usando curvas de año %s (%s).",
            target_year,
            planta,
            chosen_year,
            path,
        )
        return chosen_year, path

    chosen_year = max(calibracion_files.keys())
    path = calibracion_files[chosen_year]
    logger.warning(
        "No hay curvas anteriores a %s para %s. "
        "Usando curvas más recientes disponibles (%s, %s).",
        target_year,
        planta,
        chosen_year,
        path,
    )
    return chosen_year, path


def find_cell(df: pd.DataFrame, value: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Encuentra posición (fila, col) de valor en DataFrame.
    
    Returns:
        (row_index, col_index) o (None, None) si no se encuentra
    """
    matches = np.where(df.values == value)
    if len(matches[0]) == 0:
        return None, None
    return int(matches[0][0]), int(matches[1][0])


def guess_secadora(sensor_id) -> Optional[int]:
    """
    Extrae número 1-6 desde sensor_id.
    
    Args:
        sensor_id: Puede ser string o número
    
    Returns:
        Número de secadora (1-6) o None si no se puede extraer
    """
    if pd.isna(sensor_id):
        return None
    m = re.search(r"(\d+)", str(sensor_id))
    return int(m.group(1)) if m else None


def norm_str(s: str) -> str:
    """
    Normaliza string: sin acentos, minúsculas, sin espacios.
    
    Args:
        s: String a normalizar
    
    Returns:
        String normalizado
    """
    if not isinstance(s, str):
        return str(s).lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower().strip().replace(" ", "")


def resolve_variedad_key(key_norm: str, name_map: Dict[str, str]) -> Optional[str]:
    """
    Busca variedad en name_map usando equivalencias.
    
    Args:
        key_norm: Variedad normalizada a buscar
        name_map: {variedad_normalizada: nombre_hoja_real}
    
    Returns:
        Nombre de hoja encontrado o None
    """
    candidatos = ALIAS_EQUIV.get(key_norm, {key_norm})
    for cand in candidatos:
        cand_norm = norm_str(cand)
        if cand_norm in name_map:
            return cand_norm
        # También buscar directo en keys normalizadas
        for k, v in name_map.items():
            if cand_norm == k:
                return k
    return None


def parse_temperatura_sheet(df_raw: pd.DataFrame) -> Tuple[float, float, Dict[int, float], Dict[int, pd.DataFrame]]:
    """
    Parsea hoja TEMPERATURA del Excel de curvas.
    
    Estructura esperada:
    - Fila 0: [AT, BT, (vacías)]
    - Fila con "Fecha": inicio de tabla
    - Fila después de "Fecha": [fecha], [C_fix Sec1], [C_fix Sec2], ..., [C_fix Sec6]
    - Filas siguientes: [fechas], [C_var Sec1], [C_var Sec2], ..., [C_var Sec6]
    
    Returns:
        (AT, BT, cfix, cvar_tbl)
        - AT, BT: constantes globales
        - cfix: {sensor: valor_fijo} para sensores 1-6
        - cvar_tbl: {sensor: DataFrame[fecha, cvar]} con correcciones temporales
    """
    # Leer DataFrame con header=None para preservar estructura exacta
    if df_raw.empty:
        raise ValueError("Hoja TEMPERATURA está vacía")
    
    # Extraer AT, BT de fila 0 (primera fila después del header si existe, o fila 0)
    # Buscar en primeras filas por si hay headers
    at_val = None
    bt_val = None
    
    for i in range(min(5, len(df_raw))):
        row = df_raw.iloc[i]
        if pd.notna(row.iloc[0]) and pd.notna(row.iloc[1]):
            try:
                at_val = float(row.iloc[0])
                bt_val = float(row.iloc[1])
                break
            except (ValueError, TypeError):
                continue
    
    if at_val is None or bt_val is None:
        raise ValueError(f"No se encontraron AT y BT en las primeras filas de hoja TEMPERATURA")
    
    AT = at_val
    BT = bt_val
    
    # Buscar celda con "Fecha"
    r, c = find_cell(df_raw, "Fecha")
    if r is None:
        raise ValueError("No se encontró 'Fecha' en hoja TEMPERATURA")
    
    # C_fix: fila siguiente a "Fecha", columnas 1-6 (indices c+1 a c+6)
    cfix_row = r + 1
    if cfix_row >= len(df_raw):
        raise ValueError("No hay fila después de 'Fecha' para C_fix")
    
    cols = list(range(c + 1, c + 7))  # 6 sensores
    cfix = {}
    
    for i, col_idx in enumerate(cols, start=1):
        try:
            val = df_raw.iloc[cfix_row, col_idx]
            cfix[i] = float(val) if pd.notna(val) else 0.0
        except (IndexError, ValueError, TypeError):
            cfix[i] = 0.0
    
    # C_var: tabla desde fila (r+2) en adelante
    if r + 2 >= len(df_raw):
        # No hay datos de C_var, crear tablas vacías
        cvar_tbl = {i: pd.DataFrame(columns=["fecha", "cvar"]) for i in range(1, 7)}
        return AT, BT, cfix, cvar_tbl
    
    data = df_raw.iloc[(r + 2):, [c] + cols].copy()
    data.columns = ["fecha"] + [f"s{i}" for i in range(1, 7)]
    
    # Eliminar filas completamente vacías
    data = data.dropna(how="all")
    
    # Convertir fecha a datetime
    data["fecha"] = pd.to_datetime(data["fecha"], errors="coerce")
    data = data.dropna(subset=["fecha"])
    
    if data.empty:
        # No hay datos válidos, crear tablas vacías
        cvar_tbl = {i: pd.DataFrame(columns=["fecha", "cvar"]) for i in range(1, 7)}
        return AT, BT, cfix, cvar_tbl
    
    # Por sensor, crear tabla [fecha, cvar] con forward-fill
    cvar_tbl = {}
    for i in range(1, 7):
        col_name = f"s{i}"
        if col_name not in data.columns:
            cvar_tbl[i] = pd.DataFrame(columns=["fecha", "cvar"])
            continue
        
        tmp = data[["fecha", col_name]].copy().rename(columns={col_name: "craw"})
        tmp["craw"] = pd.to_numeric(tmp["craw"], errors="coerce")
        tmp["craw"] = tmp["craw"].replace(0, np.nan)
        tmp = tmp.sort_values("fecha").reset_index(drop=True)
        
        # Forward-fill de correcciones
        tmp["cvar"] = tmp["craw"].ffill().fillna(0.0)
        
        cvar_tbl[i] = tmp[["fecha", "cvar"]].copy()
    
    return AT, BT, cfix, cvar_tbl


def parse_humedad_sheet(df_raw: pd.DataFrame) -> Tuple[float, float, float, Dict[int, float], Dict[int, pd.DataFrame]]:
    """
    Parsea hoja de HUMEDAD (variedad específica) del Excel de curvas.
    
    Similar a parse_temperatura_sheet pero con AH, BH, CH.
    
    Returns:
        (AH, BH, CH, cfix, cvar_tbl)
    """
    if df_raw.empty:
        raise ValueError("Hoja de HUMEDAD está vacía")
    
    # Extraer AH, BH, CH de fila 0
    ah_val = None
    bh_val = None
    ch_val = None
    
    for i in range(min(5, len(df_raw))):
        row = df_raw.iloc[i]
        if pd.notna(row.iloc[0]) and pd.notna(row.iloc[1]) and pd.notna(row.iloc[2]):
            try:
                ah_val = float(row.iloc[0])
                bh_val = float(row.iloc[1])
                ch_val = float(row.iloc[2])
                break
            except (ValueError, TypeError, IndexError):
                continue
    
    if ah_val is None or bh_val is None or ch_val is None:
        raise ValueError(f"No se encontraron AH, BH y CH en las primeras filas de hoja de HUMEDAD")
    
    AH = ah_val
    BH = bh_val
    CH = ch_val
    
    # Buscar celda con "Fecha"
    r, c = find_cell(df_raw, "Fecha")
    if r is None:
        raise ValueError("No se encontró 'Fecha' en hoja de HUMEDAD")
    
    # C_fix: fila siguiente a "Fecha", columnas 1-6
    cfix_row = r + 1
    if cfix_row >= len(df_raw):
        raise ValueError("No hay fila después de 'Fecha' para C_fix")
    
    cols = list(range(c + 1, c + 7))
    cfix = {}
    
    for i, col_idx in enumerate(cols, start=1):
        try:
            val = df_raw.iloc[cfix_row, col_idx]
            cfix[i] = float(val) if pd.notna(val) else 0.0
        except (IndexError, ValueError, TypeError):
            cfix[i] = 0.0
    
    # C_var: tabla desde fila (r+2) en adelante
    if r + 2 >= len(df_raw):
        cvar_tbl = {i: pd.DataFrame(columns=["fecha", "cvar"]) for i in range(1, 7)}
        return AH, BH, CH, cfix, cvar_tbl
    
    data = df_raw.iloc[(r + 2):, [c] + cols].copy()
    data.columns = ["fecha"] + [f"s{i}" for i in range(1, 7)]
    data = data.dropna(how="all")
    data["fecha"] = pd.to_datetime(data["fecha"], errors="coerce")
    data = data.dropna(subset=["fecha"])
    
    if data.empty:
        cvar_tbl = {i: pd.DataFrame(columns=["fecha", "cvar"]) for i in range(1, 7)}
        return AH, BH, CH, cfix, cvar_tbl
    
    cvar_tbl = {}
    for i in range(1, 7):
        col_name = f"s{i}"
        if col_name not in data.columns:
            cvar_tbl[i] = pd.DataFrame(columns=["fecha", "cvar"])
            continue
        
        tmp = data[["fecha", col_name]].copy().rename(columns={col_name: "craw"})
        tmp["craw"] = pd.to_numeric(tmp["craw"], errors="coerce")
        tmp["craw"] = tmp["craw"].replace(0, np.nan)
        tmp = tmp.sort_values("fecha").reset_index(drop=True)
        tmp["cvar"] = tmp["craw"].ffill().fillna(0.0)
        
        cvar_tbl[i] = tmp[["fecha", "cvar"]].copy()
    
    return AH, BH, CH, cfix, cvar_tbl


def merge_asof_cvar(
    df: pd.DataFrame,
    key_fecha: str,
    key_secadora: str,
    cvar_tbl: Dict[int, pd.DataFrame],
    out_col: str
) -> pd.DataFrame:
    """
    Hace merge temporal de correcciones variables por sensor usando merge_asof.
    
    Args:
        df: DataFrame con columnas key_fecha (timestamp) y key_secadora (1-6)
        cvar_tbl: {sensor: DataFrame[fecha, cvar]}
        out_col: nombre de columna de salida para cvar
    
    Returns:
        DataFrame con columna out_col agregada
    """
    df = df.copy()
    df[out_col] = 0.0
    
    for s in range(1, 7):
        mask = df[key_secadora] == s
        if not mask.any():
            continue
        
        if s not in cvar_tbl or cvar_tbl[s].empty:
            continue
        
        base = df.loc[mask, [key_fecha]].copy().sort_values(key_fecha)
        cv = cvar_tbl[s].sort_values("fecha")
        
        if cv.empty:
            continue
        
        merged = pd.merge_asof(
            base,
            cv.rename(columns={"fecha": key_fecha}),
            on=key_fecha,
            direction="backward"
        )
        merged["cvar"] = merged["cvar"].fillna(0.0)
        
        idx_sorted = df.loc[mask].sort_values(key_fecha).index
        df.loc[idx_sorted, out_col] = merged["cvar"].values
    
    return df


def aplicar_curvas_calibracion(
    wide: pd.DataFrame,
    gdrive,
    planta: str,
    calibracion_file_path: str
) -> pd.DataFrame:
    """
    Aplica curvas de calibración al DataFrame wide con VALIDACIÓN EXHAUSTIVA.
    Agrega columnas TEMPERATURA y HUMEDAD.
    
    Args:
        wide: DataFrame con VOLT_HUM, VOLT_TEM, Variedad, sensor_id, timestamp, año
        gdrive: cliente GoogleDriveClient para descargar Excel de curvas
        planta: "JPV" o "RB"
        calibracion_file_path: path en GDrive del Excel de curvas
    
    Returns:
        DataFrame con columnas TEMPERATURA y HUMEDAD agregadas
    """
    # VALIDACIÓN INICIAL (CRÍTICA) - usar print para visibilidad
    print(f"      📊 PRE-CALIBRACIÓN {planta}:")
    print(f"         Input shape: {wide.shape}")
    
    wide = normalize_timestamp(wide, "timestamp", assume_local=True)
    
    # Verificar columnas críticas
    required = ["VOLT_HUM", "VOLT_TEM", "Variedad", "sensor_id", "timestamp"]
    missing = [c for c in required if c not in wide.columns]
    if missing:
        print(f"         ❌ Faltan columnas para calibración: {missing}")
        wide = wide.copy()
        wide["TEMPERATURA"] = np.nan
        wide["HUMEDAD"] = np.nan
        return wide
    
    # Verificar valores de voltaje
    vh_valid = wide["VOLT_HUM"].notna().sum()
    vt_valid = wide["VOLT_TEM"].notna().sum()
    vh_nonzero = (wide["VOLT_HUM"] != 0).sum()
    vt_nonzero = (wide["VOLT_TEM"] != 0).sum()
    
    print(f"         VOLT_HUM: {vh_valid} válidos, {vh_nonzero} no-cero")
    print(f"         VOLT_TEM: {vt_valid} válidos, {vt_nonzero} no-cero")
    
    if vh_nonzero > 0 or vt_nonzero > 0:
        if "VOLT_HUM" in wide.columns and vh_nonzero > 0:
            print(f"         Rango VOLT_HUM: min={wide['VOLT_HUM'].min():.4f}, max={wide['VOLT_HUM'].max():.4f}")
        if "VOLT_TEM" in wide.columns and vt_nonzero > 0:
            print(f"         Rango VOLT_TEM: min={wide['VOLT_TEM'].min():.4f}, max={wide['VOLT_TEM'].max():.4f}")
    
    if vh_nonzero == 0 and vt_nonzero == 0:
        print(f"         ❌ TODOS los voltajes son 0 o NaN - NO SE PUEDE CALIBRAR")
        print(f"            Muestra VOLT_HUM: {wide['VOLT_HUM'].head(10).tolist()}")
        print(f"            Muestra VOLT_TEM: {wide['VOLT_TEM'].head(10).tolist()}")
        wide = wide.copy()
        wide["TEMPERATURA"] = np.nan
        wide["HUMEDAD"] = np.nan
        return wide
    
    # Verificar variedades
    # IMPORTANTE: Asegurarse de que 'Variedad' sea una Serie, no un DataFrame
    # Esto puede ocurrir si hay columnas duplicadas después de un merge
    if "Variedad" in wide.columns:
        variedad_col = wide["Variedad"]
        # Si es un DataFrame (columnas duplicadas), tomar la primera columna
        if isinstance(variedad_col, pd.DataFrame):
            logger.warning("calibracion: 'Variedad' es un DataFrame (columnas duplicadas), usando la primera columna")
            variedad_col = variedad_col.iloc[:, 0]
        
        var_valid = variedad_col.notna().sum()
        print(f"         Variedad: {var_valid}/{len(wide)} válidas")
        variedades_unicas = variedad_col.dropna().unique()[:10]
        print(f"         Variedades únicas: {list(variedades_unicas)}")
    else:
        var_valid = 0
        variedades_unicas = []
        print(f"         Variedad: Columna 'Variedad' no encontrada en wide")
    
    if var_valid == 0:
        print(f"         ⚠️ NINGUNA variedad válida - solo se calculará TEMPERATURA")
    
    # Descargar archivo de curvas a archivo temporal
    tmp_path = None
    try:
        # Descargar bytes desde Google Drive
        excel_bytes = gdrive.download_file(calibracion_file_path)
        
        # Crear archivo temporal
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(excel_bytes)
            tmp_path = Path(tmp.name)
    except Exception as e:
        logger.warning(f"No se pudo descargar archivo de calibración {calibracion_file_path}: {e}")
        wide = wide.copy()
        wide["TEMPERATURA"] = np.nan
        wide["HUMEDAD"] = np.nan
        return wide
    
    try:
        wide = wide.copy()
        
        # Extraer secadora de sensor_id
        wide["secadora"] = wide["sensor_id"].apply(guess_secadora)
        
        # Timestamp con hora (sin normalize: respeta hora, como en el notebook)
        fecha_ref = pd.to_datetime(wide["timestamp"], errors="coerce")  # sin normalize: respeta hora
        
        # Voltajes
        vh = pd.to_numeric(wide["VOLT_HUM"], errors="coerce")
        vt = pd.to_numeric(wide["VOLT_TEM"], errors="coerce")
        mask_vh_zero = (vh.fillna(0) == 0)
        mask_vt_zero = (vt.fillna(0) == 0)
        
        # --- TEMPERATURA (global) ---
        try:
            df_temp_raw = pd.read_excel(tmp_path, sheet_name="TEMPERATURA", header=None)
            AT, BT, cfix_T, cvar_T = parse_temperatura_sheet(df_temp_raw)
            
            # Preparar DataFrame auxiliar para merge_asof
            aux_T = pd.DataFrame({
                "fecha_ref": fecha_ref,
                "secadora": wide["secadora"]
            })
            aux_T = merge_asof_cvar(aux_T, "fecha_ref", "secadora", cvar_T, "cvar_T")
            
            # Mapear C_fix por secadora
            cfix_T_series = aux_T["secadora"].map(cfix_T).astype(float)
            
            # Fórmula: TEMPERATURA = VT * AT + BT + C_fix_T[sensor] - C_var_T[sensor, timestamp]
            TEMPERATURA = vt * AT + BT + cfix_T_series - aux_T["cvar_T"]
            TEMPERATURA = TEMPERATURA.mask(mask_vt_zero | vt.isna(), np.nan)
        except Exception as e:
            logger.warning(f"Error procesando curvas de TEMPERATURA: {e}")
            TEMPERATURA = pd.Series(np.nan, index=wide.index, dtype=float)
        
        # --- HUMEDAD (por variedad) ---
        try:
            xl = pd.ExcelFile(tmp_path)
            hojas_hum = [s for s in xl.sheet_names if norm_str(s) != "temperatura"]
            name_map = {norm_str(s): s for s in hojas_hum}
            
            # DEBUG: Mostrar todas las hojas disponibles (SOLUCIÓN 2)
            print(f"         📋 Hojas disponibles en archivo de curvas:")
            for hoja_original in xl.sheet_names:
                hoja_norm = norm_str(hoja_original)
                if hoja_norm != "temperatura":
                    print(f"            - '{hoja_original}' -> normalizado: '{hoja_norm}'")
            
            HUMEDAD = pd.Series(np.nan, index=wide.index, dtype=float)
            # IMPORTANTE: Asegurarse de que 'Variedad' sea una Serie, no un DataFrame
            variedad_col = wide["Variedad"]
            if isinstance(variedad_col, pd.DataFrame):
                variedad_col = variedad_col.iloc[:, 0]
            variedad_norm = variedad_col.astype(str).map(norm_str)
            
            faltantes = []
            cache_params = {}
            
            # Variedad por defecto para fallback (SOLUCIÓN 3)
            # Intentar usar variedades comunes como fallback
            default_varieties = ["guri", "gurí", "elpaso", "el paso", "merin"]
            default_variety = None
            for dv in default_varieties:
                dv_norm = norm_str(dv)
                if dv_norm in name_map:
                    default_variety = dv_norm
                    break
            
            for key_norm in variedad_norm.dropna().unique():
                key_lookup = resolve_variedad_key(key_norm, name_map)
                
                if key_lookup is None:
                    # FALLBACK: Usar curvas de variedad por defecto (SOLUCIÓN 3)
                    if default_variety and default_variety in name_map:
                        logger.warning(
                            f"[{planta}] Variedad '{key_norm}' sin curvas específicas. "
                            f"Usando curvas de '{default_variety}' como fallback."
                        )
                        key_lookup = default_variety
                    else:
                        faltantes.append(key_norm)
                        logger.warning(
                            f"[{planta}] Variedad '{key_norm}' sin curvas y sin fallback disponible. "
                            f"Se omitirá el cálculo de humedad para esta variedad."
                        )
                        continue
                
                # Cachear parámetros por hoja
                if key_lookup not in cache_params:
                    try:
                        raw = pd.read_excel(tmp_path, sheet_name=name_map[key_lookup], header=None)
                        cache_params[key_lookup] = parse_humedad_sheet(raw)
                    except Exception as e:
                        logger.warning(f"Error parseando hoja {name_map[key_lookup]}: {e}")
                        faltantes.append(key_norm)
                        continue
                
                AH, BH, CH, cfix_H, cvar_H = cache_params[key_lookup]
                mask = (variedad_norm == key_norm)
                
                if not mask.any():
                    continue
                
                # Preparar DataFrame auxiliar para merge_asof
                aux_H = pd.DataFrame({
                    "fecha_ref": fecha_ref[mask],
                    "secadora": wide.loc[mask, "secadora"]
                })
                aux_H = merge_asof_cvar(aux_H, "fecha_ref", "secadora", cvar_H, "cvar_H")
                
                # Mapear C_fix por secadora
                cfix_H_series = aux_H["secadora"].map(cfix_H).astype(float)
                
                # Fórmula: HUMEDAD = (VH^2) * AH + VH * BH + CH + C_fix_H[sensor] - C_var_H[sensor, timestamp]
                HUMEDAD.loc[mask] = (
                    (vh[mask] ** 2) * AH + 
                    vh[mask] * BH + 
                    CH + 
                    cfix_H_series - 
                    aux_H["cvar_H"]
                )
            
            if faltantes:
                logger.warning(f"[{planta}] Variedades sin curvas: {', '.join(sorted(set(faltantes)))}")
        except Exception as e:
            logger.warning(f"Error procesando curvas de HUMEDAD: {e}")
            HUMEDAD = pd.Series(np.nan, index=wide.index, dtype=float)
        
        # Aplicar máscaras finales
        HUMEDAD = HUMEDAD.mask(mask_vh_zero | vh.isna(), np.nan)
        
        # Agregar columnas
        wide["TEMPERATURA"] = TEMPERATURA
        wide["HUMEDAD"] = HUMEDAD
        
    finally:
        # Limpiar archivo temporal
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass
    
    temp_non_null = wide["TEMPERATURA"].notna().sum() if "TEMPERATURA" in wide.columns else 0
    hum_non_null = wide["HUMEDAD"].notna().sum() if "HUMEDAD" in wide.columns else 0
    logger.info(
        "[CALIB] TEMPERATURA calculada: %d filas no nulas",
        temp_non_null,
    )
    logger.info(
        "[CALIB] HUMEDAD calculada: %d filas no nulas",
        hum_non_null,
    )
    
    return wide

