import io
import logging
import unicodedata
import re
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd
from shared_code.time_utils import normalize_timestamp

logger = logging.getLogger(__name__)


def normalize_id(x: Any) -> Optional[str]:
    """
    Normalize lab identifiers to consistent strings.

    - Integers/floats become integer strings (e.g., 12.0 -> "12").
    - Alphanumerics are preserved as stripped strings.
    - NaN/None -> None.

    Examples
    --------
    >>> normalize_id(12.0)
    '12'
    >>> normalize_id('  A-01 ')
    'A-01'
    >>> normalize_id(None) is None
    True
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, (int, np.integer)):
        return str(int(x))
    if isinstance(x, (float, np.floating)) and not np.isnan(x):
        # Convert 12.0 -> '12', 12.5 -> '12.5'
        as_int = int(x)
        return str(as_int) if x == as_int else str(x)
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    return s


def load_lab_control_file(file_content: bytes, year: int, planta: str) -> pd.DataFrame:
    """
    Load and normalize a "Control Tachadas" Excel file.

    - Auto-detect header row by finding a row containing expected headers.
    - Normalize columns to: Variedad, ID_tachada, Inicio, Fin, sensor_id, HumedadInicial, HumedadFinal.
    - Parse dates with dayfirst=True and convert to UTC.
    - Drop invalid records (missing time bounds or sensor if required).

    Returns a clean DataFrame suitable for interval joins.
    """
    expected = {"variedad", "identificador", "inicio", "fin", "sensor", "humedad"}
    with io.BytesIO(file_content) as bio:
        xls = pd.ExcelFile(bio)
        df_raw = pd.read_excel(xls, sheet_name=0, header=None)

    header_row = None
    for i in range(min(20, len(df_raw))):
        row_vals = df_raw.iloc[i].astype(str).str.strip().str.lower().tolist()
        if any(h in expected for h in row_vals):
            header_row = i
            break
    if header_row is None:
        header_row = 0

    with io.BytesIO(file_content) as bio2:
        df = pd.read_excel(bio2, sheet_name=0, header=header_row)

    # Normalize column names
    cols = {c: str(c).strip() for c in df.columns}
    df.rename(columns=cols, inplace=True)
    lower = {c.lower(): c for c in df.columns}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n.lower() in lower:
                return lower[n.lower()]
        return None

    col_var = pick("Variedad")
    col_id = pick("Identificador", "ID", "ID_tachada")
    col_ini = pick("Inicio")
    col_fin = pick("Fin")
    col_sensor = pick("Sensor", "sensor_id")
    
    # Detección robusta de columnas de humedad inicial y final
    # Normalizar nombre de columna: minúsculas, sin tildes, sin %, sin espacios extras
    def normalize_column_name(col_name: str) -> str:
        """Normaliza nombre de columna para búsqueda robusta."""
        if pd.isna(col_name):
            return ""
        s = str(col_name).strip()
        # Quitar tildes
        s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
        # A minúsculas
        s = s.lower()
        # Quitar %
        s = s.replace("%", "")
        # Normalizar espacios (múltiples espacios a uno solo)
        s = re.sub(r'\s+', ' ', s).strip()
        return s
    
    col_humedad_inicial = None
    col_humedad_final = None
    
    # Recorrer todas las columnas y detectar por contenido
    for col in df.columns:
        normalized = normalize_column_name(col)
        
        # Detectar humedad inicial: debe contener "humedad" Y ("inicio" o "inicial")
        if "humedad" in normalized:
            if "inicio" in normalized or "inicial" in normalized:
                if col_humedad_inicial is None:  # Usar la primera que coincida
                    col_humedad_inicial = col
                    logger.debug(f"Columna de humedad inicial detectada: '{col}' (normalizada: '{normalized}')")
        
        # Detectar humedad final: debe contener "humedad" Y "final"
        if "humedad" in normalized and "final" in normalized:
            if col_humedad_final is None:  # Usar la primera que coincida
                col_humedad_final = col
                logger.debug(f"Columna de humedad final detectada: '{col}' (normalizada: '{normalized}')")

    result = pd.DataFrame()
    if col_var is not None:
        result["Variedad"] = df[col_var].astype(str).str.strip()
    else:
        result["Variedad"] = None

    result["ID_tachada"] = df[col_id] if col_id is not None else None
    result["ID_tachada"] = result["ID_tachada"].map(normalize_id)

    ini = pd.to_datetime(df[col_ini], errors="coerce", dayfirst=True) if col_ini is not None else pd.NaT
    fin = pd.to_datetime(df[col_fin], errors="coerce", dayfirst=True) if col_fin is not None else pd.NaT
    result["Inicio"] = ini
    result["Fin"] = fin
    # Normalizar timestamps del laboratorio asumiendo hora local (UTC-3)
    result = normalize_timestamp(result, "Inicio", assume_local=True)
    result = normalize_timestamp(result, "Fin", assume_local=True)

    # Sensor id normalization (keep ints)
    if col_sensor is not None:
        sid = df[col_sensor]
        # Accept both SENSOR10 style and bare numbers
        sid_norm = sid.astype(str).str.extract(r"(\d+)", expand=False)
        result["sensor_id"] = pd.to_numeric(sid_norm, errors="coerce").astype("Int64")
    else:
        result["sensor_id"] = pd.Series([pd.NA] * len(df), dtype="Int64")

    # Asignar columnas de humedad inicial y final con normalización
    if col_humedad_inicial is not None:
        # Normalizar valores: convertir a string, reemplazar coma por punto, strip
        s = df[col_humedad_inicial].astype(str).str.replace(",", ".", regex=False).str.strip()
        result["HumedadInicial"] = pd.to_numeric(s, errors="coerce")
    else:
        result["HumedadInicial"] = None
    
    if col_humedad_final is not None:
        # Normalizar valores: convertir a string, reemplazar coma por punto, strip
        s = df[col_humedad_final].astype(str).str.replace(",", ".", regex=False).str.strip()
        result["HumedadFinal"] = pd.to_numeric(s, errors="coerce")
    else:
        result["HumedadFinal"] = None
    
    # Garantizar que las columnas existan en result
    if "HumedadInicial" not in result.columns:
        result["HumedadInicial"] = None
    if "HumedadFinal" not in result.columns:
        result["HumedadFinal"] = None

    # Add context columns if needed later
    result["planta"] = str(planta).upper().strip() if planta else None
    result["anio"] = int(year) if year is not None else None

    # Clean invalid rows: require Inicio and Fin present and Inicio <= Fin
    valid = result["Inicio"].notna() & result["Fin"].notna() & (result["Inicio"] <= result["Fin"])
    result = result[valid].reset_index(drop=True)
    return result


def cross_with_lab(
    sensor_df: pd.DataFrame,
    lab_df: pd.DataFrame,
    require_sensor_match: bool = True,
) -> pd.DataFrame:
    """
    Interval join sensor data with lab "tachadas" using vectorized search.

    - If require_sensor_match is True, matches by sensor_id and time interval.
    - If False, matches only by time interval (uses all lab intervals).
    - Adds columns: Variedad, ID_tachada, HumedadInicial, HumedadFinal.
    - Assumes timestamps are timezone-aware UTC; converts if needed.
    """
    if sensor_df.empty or lab_df.empty:
        return sensor_df.assign(
            Variedad=None, 
            ID_tachada=None, 
            HumedadInicial=None,
            HumedadFinal=None
        )

    sdf = sensor_df.copy()
    # IMPORTANTE: Preservar todas las columnas originales del sensor_df
    # cross_with_lab() solo debe agregar columnas de laboratorio, no filtrar ni perder columnas
    
    # Normalizar timestamps del sensor a UTC-3 (naive)
    timestamp_before = sdf["timestamp"].notna().sum()
    sdf = normalize_timestamp(sdf, "timestamp", assume_local=True)
    timestamp_after = sdf["timestamp"].notna().sum()
    if timestamp_after < timestamp_before:
        logger.warning(
            "cross_with_lab: %d timestamps se convirtieron a NaN durante normalización. Total filas: %d",
            timestamp_before - timestamp_after,
            len(sdf),
        )
    
    # ACCIÓN 2: Redondear timestamp del sensor a segundos (eliminar milisegundos)
    # El laboratorio suele registrar al segundo, mientras que el sensor puede tener milisegundos
    # Esto asegura que las comparaciones de intervalo funcionen correctamente
    if "timestamp" in sdf.columns:
        # Redondear al segundo inferior (floor) para asegurar que esté dentro del intervalo
        sdf["timestamp_seg"] = sdf["timestamp"].dt.floor("s")  # "s" en minúscula para evitar FutureWarning
        logger.debug(f"   Timestamps redondeados a segundos: {sdf['timestamp_seg'].notna().sum()} válidos")
    else:
        logger.error("cross_with_lab: No se encontró columna 'timestamp' en sensor_df")
        return sdf.assign(
            Variedad=None, 
            ID_tachada=None, 
            HumedadInicial=None,
            HumedadFinal=None
        )

    ldf = lab_df.copy()
    # Normalizar timestamps del laboratorio asumiendo hora local (UTC-3) para consistencia con sensores
    ldf = normalize_timestamp(ldf, "Inicio", assume_local=True)
    ldf = normalize_timestamp(ldf, "Fin", assume_local=True)
    
    # Log para verificar columnas en lab después de normalización
    logger.info(f"[LAB] Columnas lab después de normalización: {list(ldf.columns)}")
    
    # DEBUG: Verificar rangos de tiempo
    if not sdf["timestamp_seg"].isna().all() and not ldf.empty:
        logger.debug(f"   Rango timestamps sensores: {sdf['timestamp_seg'].min()} a {sdf['timestamp_seg'].max()}")
        logger.debug(f"   Rango fechas laboratorio: {ldf['Inicio'].min()} a {ldf['Fin'].max()}")

    # Prepare output columns - asegurar que siempre estén presentes
    sdf["Variedad"] = None
    sdf["ID_tachada"] = None
    for col in ["HumedadInicial", "HumedadFinal"]:
        if col not in sdf.columns:
            sdf[col] = None

    if not require_sensor_match:
        # Modo time-only: usar timestamp redondeado a segundos
        L = ldf.sort_values("Inicio")
        starts = L["Inicio"].values
        ends = L["Fin"].values
        t = sdf["timestamp_seg"].values  # USAR timestamp_seg (redondeado a segundos)
        pos = np.searchsorted(starts, t, side="right") - 1
        # Asegurar que pos sea válido antes de indexar
        valid_pos = (pos >= 0) & (pos < len(ends))
        # Verificar que timestamp esté dentro del intervalo [Inicio, Fin]
        if len(starts) > 0 and len(ends) > 0:
            # Usar indexación condicional para evitar errores
            starts_at_pos = starts[np.clip(pos, 0, len(starts)-1)]
            ends_at_pos = ends[np.clip(pos, 0, len(ends)-1)]
            valid = valid_pos & (t >= starts_at_pos) & (t <= ends_at_pos)
        else:
            valid = np.zeros(len(t), dtype=bool)
        matched = pd.Series(pos, index=sdf.index)
        matched = matched.where(valid, other=-1)
        mask = matched >= 0
        if mask.any():
            matched_indices = matched[mask].astype(int)
            sdf.loc[mask, "Variedad"] = L["Variedad"].values[matched_indices]
            sdf.loc[mask, "ID_tachada"] = L["ID_tachada"].values[matched_indices]
            if "HumedadInicial" in L.columns and L["HumedadInicial"].notna().any():
                sdf.loc[mask, "HumedadInicial"] = (
                    L["HumedadInicial"].reset_index(drop=True).iloc[matched_indices].values
                )
            if "HumedadFinal" in L.columns and L["HumedadFinal"].notna().any():
                sdf.loc[mask, "HumedadFinal"] = (
                    L["HumedadFinal"].reset_index(drop=True).iloc[matched_indices].values
                )

        unmatched = (~mask).sum()
        if unmatched:
            logger.info("Interval join: %d sensor rows unmatched (time-only mode)", unmatched)
        return sdf

    # Sensor-aware join
    total_unmatched = 0
    total_matched = 0
    
    logger.info(f"📋 Cruce laboratorio: {len(sdf)} registros de sensores, {len(ldf)} tachadas en lab")
    logger.info(f"   Sensores en datos: {sorted(sdf['sensor_id'].dropna().unique())}")
    logger.info(f"   Sensores en lab: {sorted(ldf['sensor_id'].dropna().unique())}")
    if not ldf.empty:
        logger.info(f"   Rango fechas lab: {ldf['Inicio'].min()} a {ldf['Fin'].max()}")
    
    for sid, sub in sdf.groupby("sensor_id", dropna=False):
        if pd.isna(sid):
            # Skip rows with missing sensor_id
            total_unmatched += len(sub)
            continue
        lid = ldf[ldf["sensor_id"] == sid]
        if lid.empty:
            total_unmatched += len(sub)
            logger.debug(f"   Sensor {sid}: Sin tachadas en lab ({len(sub)} registros sin match)")
            continue
        L = lid.sort_values("Inicio")
        
        # Logs cuando existan columnas HumedadInicial / Final
        if "HumedadInicial" in L.columns:
            logger.debug(f"   HumedadInicial disponibles en lab: {L['HumedadInicial'].dropna().unique()[:5].tolist()}")
        if "HumedadFinal" in L.columns:
            logger.debug(f"   HumedadFinal disponibles en lab: {L['HumedadFinal'].dropna().unique()[:5].tolist()}")
        
        starts = L["Inicio"].values
        ends = L["Fin"].values
        # ACCIÓN 3: Usar timestamp_seg (redondeado a segundos) en lugar de timestamp original
        t = sub["timestamp_seg"].values  # USAR timestamp_seg para el cruce
        
        # Lógica de interval join mejorada:
        # 1. Encontrar la posición del último intervalo donde starts <= t
        pos = np.searchsorted(starts, t, side="right") - 1
        # 2. Verificar que pos sea válido (>= 0 y < len(ends))
        valid_pos = (pos >= 0) & (pos < len(ends))
        # 3. Verificar que el timestamp esté dentro del intervalo [Inicio, Fin]
        #    t debe ser >= Inicio[pos] Y <= Fin[pos]
        if len(starts) > 0 and len(ends) > 0:
            # Usar indexación segura con np.clip para evitar errores de índice
            starts_at_pos = starts[np.clip(pos, 0, len(starts)-1)]
            ends_at_pos = ends[np.clip(pos, 0, len(ends)-1)]
            # Comparar timestamps (asegurar que sean comparables)
            valid = valid_pos & (t >= starts_at_pos) & (t <= ends_at_pos)
        else:
            valid = np.zeros(len(t), dtype=bool)

        idx = sub.index
        matched = pd.Series(pos, index=idx)
        matched = matched.where(valid, other=-1)
        mask = matched >= 0
        if mask.any():
            matched_indices = matched[mask].astype(int)
            sdf.loc[idx[mask], "Variedad"] = L["Variedad"].values[matched_indices]
            sdf.loc[idx[mask], "ID_tachada"] = L["ID_tachada"].values[matched_indices]
            if "HumedadInicial" in L.columns and L["HumedadInicial"].notna().any():
                sdf.loc[idx[mask], "HumedadInicial"] = (
                    L["HumedadInicial"].reset_index(drop=True).iloc[matched_indices].values
                )
            if "HumedadFinal" in L.columns and L["HumedadFinal"].notna().any():
                sdf.loc[idx[mask], "HumedadFinal"] = (
                    L["HumedadFinal"].reset_index(drop=True).iloc[matched_indices].values
                )
            total_matched += mask.sum()
            logger.debug(f"   Sensor {sid}: {mask.sum()}/{len(sub)} registros matched")
        total_unmatched += (~mask).sum()

    matched_count = sdf["Variedad"].notna().sum()
    logger.info(f"✅ Cruce laboratorio completado: {matched_count}/{len(sdf)} registros matched ({matched_count/len(sdf)*100:.1f}%)")
    
    if matched_count == 0:
        logger.warning(f"⚠️ NINGÚN registro hizo match con laboratorio")
        if not sdf["timestamp"].isna().all():
            logger.debug(f"   Rango timestamps datos: {sdf['timestamp'].min()} a {sdf['timestamp'].max()}")
    
    if total_unmatched:
        logger.info("Interval join: %d sensor rows unmatched (sensor+time mode)", total_unmatched)
    return sdf


def get_lab_file_for_sensor(gdrive_client: Any, planta: str, year: int) -> bytes:
    """
    Busca y descarga el archivo de laboratorio para una planta y año.

    Modificado para usar folder_id configurado por planta y buscar archivos
    dinámicamente sin nombres hardcodeados.

    Args:
        gdrive_client: GoogleDriveClient instance
        planta: Planta (JPV o RB)
        year: Año (2025, 2024, etc.)

    Returns:
        Contenido del archivo Excel en bytes

    Raises:
        FileNotFoundError: Si no se encuentra archivo de laboratorio
    """
    try:
        from shared_code.config import get_lab_folder_id

        # Obtener folder_id de la carpeta de laboratorio para esta planta
        folder_id = get_lab_folder_id(planta)

        logger.info(
            f"[LAB] Buscando archivos de laboratorio para {planta} {year} en folder {folder_id}"
        )

        # Listar todos los archivos Excel en la carpeta
        files = gdrive_client.list_files_by_folder_id(
            folder_id=folder_id,
            mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        if not files:
            raise FileNotFoundError(
                f"No se encontraron archivos Excel en la carpeta de laboratorio para {planta}"
            )

        logger.info(f"[LAB] Encontrados {len(files)} archivos Excel en laboratorio de {planta}")

        # Filtrar archivos que coincidan con el año (opcional)
        matching_files = []
        for file_info in files:
            file_name = file_info.get("name", "")

            # Buscar año en el nombre del archivo
            if str(year) in file_name:
                matching_files.append(file_info)
                logger.info(f"[LAB] Archivo candidato: {file_name}")

        # Si no hay coincidencias por año, usar el más reciente
        if not matching_files:
            logger.warning(
                f"[LAB] No se encontró archivo específico para año {year}, "
                f"usando el más reciente"
            )
            matching_files = files[:1]

        # Usar el archivo más reciente
        lab_file = matching_files[0]
        lab_file_name = lab_file.get("name")
        lab_file_id = lab_file.get("id")

        logger.info(f"[LAB] Usando archivo: {lab_file_name} (ID: {lab_file_id})")

        # Descargar archivo
        content = gdrive_client.download_file(lab_file_name, file_id=lab_file_id)

        logger.info(f"[LAB] Archivo descargado exitosamente ({len(content)} bytes)")

        return content

    except ValueError as e:
        # Error de configuración
        logger.error(f"[LAB] Error de configuración: {str(e)}")
        raise FileNotFoundError(str(e))
    except Exception as e:
        logger.error(f"[LAB] Error obteniendo archivo de laboratorio: {str(e)}")
        raise FileNotFoundError(
            f"No se encontró archivo de laboratorio para {planta} {year}: {str(e)}"
        )


