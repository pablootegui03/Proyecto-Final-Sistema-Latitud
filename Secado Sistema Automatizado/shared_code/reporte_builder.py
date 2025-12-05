"""
Módulo para generar reportes HTML de tachadas de secado.

Replica la lógica del script reporte.py pero integrado al sistema actual:
- Usa GoogleDriveClient del sistema
- Sin OAuth manual ni pydrive
- Integrado al sistema de logging
"""

import io
from io import BytesIO
import base64
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Backend sin GUI para Azure (DEBE ir ANTES de pyplot)
import matplotlib.pyplot as plt
import seaborn as sns

logger = logging.getLogger(__name__)

# Nombre del archivo histórico
NOMBRE_HISTORICO = "df_historico.csv"

# Colores para gráficos
COLOR_PROBLEMA = "#d62728"  # rojo sobrio
COLOR_OK = "#1f77b4"        # azul para "total" o "sin problema"


# ================================================================
# FUNCIONES HELPER PARA DETECCIÓN DINÁMICA DE COLUMNAS
# ================================================================

def _detectar_columna_temp_max(df):
    """
    Detecta la columna de temperatura máxima con fallbacks.
    Prioridad: TEMPERATURA_max > temp_max
    Nota: No calcula desde TEMPERATURA porque df_historico ya está agregado por tachada.
    """
    if "TEMPERATURA_max" in df.columns:
        return "TEMPERATURA_max"
    elif "temp_max" in df.columns:
        return "temp_max"
    else:
        return None


def _detectar_columna_humedad_mean(df):
    """
    Detecta la columna de humedad promedio con fallbacks.
    Prioridad: HUMEDAD_mean > humedad_mean
    """
    if "HUMEDAD_mean" in df.columns:
        return "HUMEDAD_mean"
    elif "humedad_mean" in df.columns:
        return "humedad_mean"
    else:
        return None


def _detectar_columna_humedad_max(df):
    """
    Detecta la columna de humedad máxima con fallbacks.
    Prioridad: HUMEDAD_max > humedad_max
    """
    if "HUMEDAD_max" in df.columns:
        return "HUMEDAD_max"
    elif "humedad_max" in df.columns:
        return "humedad_max"
    else:
        return None


def _detectar_columna_humedad_en_temp_max(df):
    """
    Detecta la columna de humedad en temperatura máxima con fallbacks.
    Prioridad: humedad_en_temp_max > HUMEDAD_mean > HUMEDAD_max > humedad_mean > humedad_max
    """
    if "humedad_en_temp_max" in df.columns:
        return "humedad_en_temp_max"
    else:
        # Usar fallback a humedad promedio o máxima
        col = _detectar_columna_humedad_mean(df)
        if col:
            return col
        col = _detectar_columna_humedad_max(df)
        if col:
            return col
        return None


def _detectar_columna_humedad_inicial(df):
    """
    Detecta la columna de humedad inicial de laboratorio.
    Prioridad: HumedadInicial > hum_lab_ini
    """
    if "HumedadInicial" in df.columns:
        return "HumedadInicial"
    elif "hum_lab_ini" in df.columns:
        return "hum_lab_ini"
    else:
        return None


def _detectar_columna_humedad_final(df):
    """
    Detecta la columna de humedad final de laboratorio.
    Prioridad: HumedadFinal > hum_lab_fin
    """
    if "HumedadFinal" in df.columns:
        return "HumedadFinal"
    elif "hum_lab_fin" in df.columns:
        return "hum_lab_fin"
    else:
        return None


def _buscar_archivo_por_nombre(gdrive_client, folder_id: str, nombre_archivo: str) -> Optional[str]:
    """Busca un archivo por nombre en una carpeta y devuelve su ID."""
    service = gdrive_client._get_service()
    nombre_escapado = gdrive_client._escape(nombre_archivo) if hasattr(gdrive_client, '_escape') else nombre_archivo.replace("'", "\\'")
    
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
    return files[0]["id"] if files else None


def _descargar_csv_a_dataframe(gdrive_client, file_id: str, file_name: str) -> pd.DataFrame:
    """Descarga un CSV de Drive como DataFrame y agrega columna archivo_origen."""
    content = gdrive_client.download_file(file_name, file_id=file_id)
    df = pd.read_csv(io.BytesIO(content))
    df["archivo_origen"] = file_name
    return df


def split_periods(df, date_col="fecha_fin", ref_date=None, days=7):
    """
    Devuelve:
      - df_last: tachadas de los últimos `days` días hasta `ref_date`
      - df_total: todo el df
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    if ref_date is None:
        ref_date = df[date_col].max()

    ref_date = pd.to_datetime(ref_date)
    start_date = ref_date - timedelta(days=days)

    mask_last = (df[date_col] >= start_date) & (df[date_col] <= ref_date)
    df_last = df.loc[mask_last].copy()
    return df_last, df


def resumen_cantidad_tachadas(df, col_problema="prediccion", group_cols=None):
    """
    Devuelve un DataFrame con:
      - n_tachadas
      - n_problema
      - pct_problema

    Si group_cols es None o [] -> resumen global (1 fila).
    Si group_cols es lista con nombres de columnas -> resumen por grupo.
    """
    df = df.copy()

    # asumimos que col_problema es 0/1 o algo casteable a int
    df["es_problema"] = df[col_problema].fillna(0).astype(int)

    # --- Caso 1: resumen global (sin groupby) ---
    if not group_cols:  # None o lista vacía
        n_tachadas = len(df)
        n_problema = int(df["es_problema"].sum())
        pct_problema = round(n_problema / n_tachadas * 100, 1) if n_tachadas > 0 else 0.0

        resumen = pd.DataFrame(
            {
                "ambito": ["global"],
                "n_tachadas": [n_tachadas],
                "n_problema": [n_problema],
                "pct_problema": [pct_problema],
            }
        )
        return resumen

    # --- Caso 2: resumen por grupo ---
    agg = (
        df
        .groupby(group_cols, dropna=False)
        .agg(
            n_tachadas=("es_problema", "count"),
            n_problema=("es_problema", "sum"),
        )
        .reset_index()
    )

    agg["pct_problema"] = (agg["n_problema"] / agg["n_tachadas"] * 100).round(1)

    return agg


def resumen_temp_y_hum(
    df,
    group_cols,
    temp_col=None,
    hum_tempmax_col=None,
    hum_proxy_col=None,
):
    """
    Devuelve, por cada grupo:
      - temp_max_grados: máximo de temp_max (°C) en las tachadas del grupo
      - hum_al_temp_max: promedio de humedad en el momento de la temp máxima
    
    Detecta automáticamente las columnas con fallbacks si no se especifican.
    """
    df = df.copy()

    # Detectar columna de temperatura máxima
    if temp_col is None:
        temp_col = _detectar_columna_temp_max(df)
        if temp_col is None:
            logger.warning("No se encontró columna de temperatura máxima. Usando None.")
            return pd.DataFrame({})
    
    if temp_col not in df.columns:
        logger.warning(f"Columna de temperatura '{temp_col}' no existe en el DataFrame.")
        return pd.DataFrame({})

    # Detectar columna de humedad
    if hum_tempmax_col is None:
        hum_col = _detectar_columna_humedad_en_temp_max(df)
    else:
        hum_col = hum_tempmax_col if hum_tempmax_col in df.columns else None
    
    if hum_col is None:
        # Fallback a hum_proxy_col si se especifica
        if hum_proxy_col and hum_proxy_col in df.columns:
            hum_col = hum_proxy_col
        else:
            # Intentar detectar automáticamente
            hum_col = _detectar_columna_humedad_mean(df)
            if hum_col is None:
                hum_col = _detectar_columna_humedad_max(df)
        
        if hum_col:
            logger.debug(f"Usando '{hum_col}' como proxy de humedad en temp máxima.")
        else:
            logger.warning("No se encontró columna de humedad. Omitiendo humedad en resumen.")
            # Devolver solo temperatura
            agg = (
                df
                .groupby(group_cols, dropna=False)
                .agg(
                    temp_max_grados=(temp_col, "max"),
                )
                .reset_index()
            )
            agg["temp_max_grados"] = agg["temp_max_grados"].round(1)
            return agg

    # Agregación con ambas columnas
    agg = (
        df
        .groupby(group_cols, dropna=False)
        .agg(
            temp_max_grados=(temp_col, "max"),
            hum_al_temp_max=(hum_col, "mean"),
        )
        .reset_index()
    )

    # Redondeos prolijos
    agg["temp_max_grados"] = agg["temp_max_grados"].round(1)
    agg["hum_al_temp_max"] = agg["hum_al_temp_max"].round(2)

    return agg


def resumen_duracion(
    df,
    col_dur="duracion_horas",
    group_cols=None
):
    """
    Resumen de duración de tachadas.

    Métricas:
      - n_tachadas
      - duracion_mean (promedio)
      - duracion_median
      - duracion_min
      - duracion_max
      - duracion_p25, duracion_p75
    """
    df = df.copy()

    # Nos aseguramos de que la duración sea numérica
    df[col_dur] = pd.to_numeric(df[col_dur], errors="coerce")

    if not group_cols:  # resumen global
        serie = df[col_dur].dropna()
        n_tachadas = len(serie)

        if n_tachadas == 0:
            resumen = pd.DataFrame(
                {
                    "ambito": ["global"],
                    "n_tachadas": [0],
                    "duracion_mean": [None],
                    "duracion_median": [None],
                    "duracion_min": [None],
                    "duracion_max": [None],
                    "duracion_p25": [None],
                    "duracion_p75": [None],
                }
            )
            return resumen

        resumen = pd.DataFrame(
            {
                "ambito": ["global"],
                "n_tachadas": [n_tachadas],
                "duracion_mean": [serie.mean()],
                "duracion_median": [serie.median()],
                "duracion_min": [serie.min()],
                "duracion_max": [serie.max()],
                "duracion_p25": [serie.quantile(0.25)],
                "duracion_p75": [serie.quantile(0.75)],
            }
        )

    else:  # resumen por grupo
        agg = (
            df
            .groupby(group_cols, dropna=False)[col_dur]
            .agg(
                n_tachadas="count",
                duracion_mean="mean",
                duracion_median="median",
                duracion_min="min",
                duracion_max="max",
                duracion_p25=lambda x: x.quantile(0.25),
                duracion_p75=lambda x: x.quantile(0.75),
            )
            .reset_index()
        )
        resumen = agg

    # Redondeos prolijos
    cols_redondear = [
        "duracion_mean",
        "duracion_median",
        "duracion_min",
        "duracion_max",
        "duracion_p25",
        "duracion_p75",
    ]
    for c in cols_redondear:
        if c in resumen.columns:
            resumen[c] = resumen[c].round(2)

    return resumen


def resumen_laboratorio(
    df,
    hum_ini_col="hum_ini_lab",
    hum_fin_col="hum_fin_lab",
    hum_30fin_col="hum_30fin_prom",
    diff_col="diff_hum_lab_vs_30fin",
    group_cols=None
):
    """
    Devuelve, por grupo:
      - humedad_inicial_lab_prom
      - humedad_final_lab_prom
      - humedad_30fin_prom
      - diferencia_lab_30fin_prom
    
    Detecta automáticamente las columnas de laboratorio con fallbacks.
    """

    df = df.copy()

    # Crear columna de diferencia lab - sensor si no existe
    if "hum_fin_lab" in df.columns and "hum_30fin_prom" in df.columns and "diff_hum_lab_vs_30fin" not in df.columns:
        df = df.copy()
        df["diff_hum_lab_vs_30fin"] = df["hum_fin_lab"] - df["hum_30fin_prom"]

    # Detectar columnas de laboratorio automáticamente
    if hum_ini_col is None:
        hum_ini_col = _detectar_columna_humedad_inicial(df)
    if hum_fin_col is None:
        hum_fin_col = _detectar_columna_humedad_final(df)

    columnas_existentes = df.columns

    def col_or_none(col):
        return col if col and col in columnas_existentes else None

    hum_ini = col_or_none(hum_ini_col)
    hum_fin = col_or_none(hum_fin_col)
    hum_30fin = col_or_none(hum_30fin_col)
    diff = col_or_none(diff_col)

    # --- Si group_cols es None → resumen global ---
    if not group_cols:

        resumen = {
            "ambito": ["global"],
            "hum_ini_lab": [df[hum_ini].mean() if hum_ini else None],
            "hum_fin_lab_prom": [df[hum_fin].mean() if hum_fin else None],
            "hum_30fin_prom": [df[hum_30fin].mean() if hum_30fin else None],
            "diff_lab_30fin_prom": [df[diff].mean() if diff else None],
        }

        return pd.DataFrame(resumen).round(2)

    # --- Caso por grupo ---
    def agg_mean(col):
        if col is None:
            return lambda x: None
        return "mean"

    agg_dict = {
        "hum_ini_lab": (hum_ini, agg_mean(hum_ini)),
        "hum_fin_lab_prom": (hum_fin, agg_mean(hum_fin)),
        "hum_30fin_prom": (hum_30fin, agg_mean(hum_30fin)),
        "diff_lab_30fin_prom": (diff, agg_mean(diff)),
    }

    # Filtrar solo columnas válidas
    agg_aplicar = {k: v for k, v in agg_dict.items() if v[0] is not None}

    # Si ninguna existe, devolver DF vacío
    if len(agg_aplicar) == 0:
        return pd.DataFrame({"mensaje": ["No hay columnas de laboratorio disponibles."]})

    out = (
        df
        .groupby(group_cols, dropna=False)
        .agg(**{k: (v[0], agg_mean(v[0])) for k, v in agg_aplicar.items()})
        .reset_index()
        .round(2)
    )

    return out


def preparar_tabla(df, rename=None, int_cols=None, dec1_cols=None, dec2_cols=None, pct_cols=None):
    """
    Devuelve una copia del df con:
      - columnas renombradas
      - columnas formateadas como string (para presentación en HTML)
    """
    df2 = df.copy()

    if rename:
        df2 = df2.rename(columns=rename)

    int_cols = int_cols or []
    dec1_cols = dec1_cols or []
    dec2_cols = dec2_cols or []
    pct_cols = pct_cols or []

    for c in int_cols:
        if c in df2.columns:
            df2[c] = df2[c].map(lambda x: f"{x:,.0f}")

    for c in dec1_cols:
        if c in df2.columns:
            df2[c] = df2[c].map(lambda x: f"{x:.1f}")

    for c in dec2_cols:
        if c in df2.columns:
            df2[c] = df2[c].map(lambda x: f"{x:.2f}")

    for c in pct_cols:
        if c in df2.columns:
            df2[c] = df2[c].map(lambda x: f"{x:.1f}%")

    return df2


def img_inline(path: Path, alt: str = "") -> str:
    """
    Lee la imagen en 'path' y devuelve un tag <img> con el contenido embebido en base64.
    Si el archivo no existe, devuelve un pequeño placeholder HTML.
    """
    if not path.exists():
        alt_text = alt or "Imagen no disponible"
        return (
            f'<div style="padding:8px; border:1px dashed #ccc; border-radius:4px; '
            f'font-size:12px; color:#666; background:#fafafa;">'
            f'{alt_text} (imagen no disponible para este período)</div>'
        )

    with path.open("rb") as f:
        data = base64.b64encode(f.read()).decode("utf-8")

    alt_attr = f' alt="{alt}"' if alt else ""
    return f'<img src="data:image/png;base64,{data}"{alt_attr}>'


def _subir_o_actualizar_archivo(
    gdrive_client,
    folder_id: str,
    nombre_archivo: str,
    content: bytes,
    mime_type: str
) -> None:
    """Sube o actualiza un archivo en Google Drive."""
    from googleapiclient.http import MediaIoBaseUpload
    
    service = gdrive_client._get_service()
    nombre_escapado = gdrive_client._escape(nombre_archivo) if hasattr(gdrive_client, '_escape') else nombre_archivo.replace("'", "\\'")
    
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
    
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime_type, resumable=True)
    
    if existente_id is None:
        file_metadata = {"name": nombre_archivo, "parents": [folder_id]}
        archivo = service.files().create(body=file_metadata, media_body=media, fields="id, name").execute()
        logger.info(f"[Reporte] ✓ Creado archivo: {archivo['name']} (id={archivo['id']})")
    else:
        archivo = service.files().update(fileId=existente_id, media_body=media, fields="id, name").execute()
        logger.info(f"[Reporte] ✓ Actualizado archivo: {archivo['name']} (id={archivo['id']})")


def generar_reporte(gdrive_client, planta: str) -> Dict[str, Any]:
    """
    Genera el reporte HTML completo de tachadas de secado.
    
    Args:
        gdrive_client: Instancia de GoogleDriveClient
        planta: Código de planta (JPV o RB)
        
    Returns:
        dict con status, filas procesadas, nombre del HTML generado
    """
    try:
        from shared_code.config import get_validated_folder_id, get_reports_folder_id
        
        logger.info(f"[Reporte] Iniciando generación de reporte para planta: {planta}")
        
        # 1. Descargar df_historico.csv desde carpeta validated
        validated_folder_id = get_validated_folder_id(planta)
        logger.info(f"[Reporte] Folder validated: {validated_folder_id}")
        
        file_id = _buscar_archivo_por_nombre(gdrive_client, validated_folder_id, NOMBRE_HISTORICO)
        if not file_id:
            logger.error(f"[Reporte] No se encontró {NOMBRE_HISTORICO} en la carpeta validated")
            return {
                "success": False,
                "filas": 0,
                "html": None,
                "mensaje": f"No se encontró {NOMBRE_HISTORICO} en la carpeta validated"
            }
        
        logger.info(f"[Reporte] Descargando {NOMBRE_HISTORICO}...")
        df = _descargar_csv_a_dataframe(gdrive_client, file_id, NOMBRE_HISTORICO)
        logger.info(f"[Reporte] ✓ DataFrame cargado: {len(df)} filas")
        
        # 2. Preparar datos (compatibilidad con formato antiguo)
        df = df.copy()
        if "fecha_fin" in df.columns:
            df["fecha_fin"] = pd.to_datetime(df["fecha_fin"])
        elif "timestamp_max" in df.columns:
            logger.info("[Reporte] Normalizando 'fecha_fin' a partir de 'timestamp_max'")
            df["fecha_fin"] = pd.to_datetime(df["timestamp_max"])
        else:
            logger.warning("[Reporte] El DataFrame no tiene columnas 'fecha_fin' ni 'timestamp_max'. Generando fechas sintéticas...")
            hoy = pd.to_datetime(datetime.now())
            df["fecha_fin"] = hoy - pd.to_timedelta(range(len(df))[::-1], unit="h")
            logger.info("[Reporte] ✓ Columna 'fecha_fin' generada sintéticamente")
        
        # Split períodos
        hoy = df["fecha_fin"].max()
        df_last_week, df_total = split_periods(df, date_col="fecha_fin", ref_date=hoy)
        
        # Crear directorio temporal para gráficos
        with tempfile.TemporaryDirectory() as temp_dir:
            figs_dir = Path(temp_dir)
            logger.info(f"[Reporte] Directorio temporal para gráficos: {figs_dir}")
            
            # 3. Calcular todos los resúmenes
            logger.info("[Reporte] Calculando resúmenes...")
            
            # Bloque 1: Cantidad de tachadas
            resumen_cant_global_last = resumen_cantidad_tachadas(df_last_week, col_problema="prediccion", group_cols=None)
            resumen_cant_global_total = resumen_cantidad_tachadas(df_total, col_problema="prediccion", group_cols=None)
            resumen_cant_secadora_last = resumen_cantidad_tachadas(df_last_week, col_problema="prediccion", group_cols=["sensor_id"])
            resumen_cant_secadora_total = resumen_cantidad_tachadas(df_total, col_problema="prediccion", group_cols=["sensor_id"])
            
            # Bloque 2: Temperaturas
            temp_turno_last = resumen_temp_y_hum(df_last_week, group_cols=["momento_dia"])
            temp_variedad_last = resumen_temp_y_hum(df_last_week, group_cols=["variedad"])
            
            # Bloque 3: Duración
            duracion_global_last = resumen_duracion(df_last_week, col_dur="duracion_horas", group_cols=None)
            duracion_global_total = resumen_duracion(df_total, col_dur="duracion_horas", group_cols=None)
            duracion_secadora_last = resumen_duracion(df_last_week, col_dur="duracion_horas", group_cols=["sensor_id"])
            
            # Bloque 4: Laboratorio
            lab_global_last = resumen_laboratorio(df_last_week, group_cols=None)
            lab_global_total = resumen_laboratorio(df_total, group_cols=None)
            lab_secadora_last = resumen_laboratorio(df_last_week, group_cols=["sensor_id"])
            
            logger.info("[Reporte] ✓ Resúmenes calculados")
            
            # 4. Generar gráficos
            logger.info("[Reporte] Generando gráficos...")
            _generar_graficos(
                df, df_last_week, df_total,
                resumen_cant_global_last, resumen_cant_global_total,
                figs_dir
            )
            logger.info("[Reporte] ✓ Gráficos generados")
            
            # 5. Preparar tablas formateadas
            logger.info("[Reporte] Preparando tablas...")
            tablas = _preparar_tablas(
                resumen_cant_global_last, resumen_cant_global_total,
                resumen_cant_secadora_last, resumen_cant_secadora_total,
                temp_turno_last, temp_variedad_last,
                duracion_global_last, duracion_global_total,
                duracion_secadora_last,
                lab_global_last, lab_global_total, lab_secadora_last,
                df, df_last_week
            )
            logger.info("[Reporte] ✓ Tablas preparadas")
            
            # 6. Generar comentarios interpretativos
            comentarios = _generar_comentarios(
                resumen_cant_global_last, resumen_cant_global_total,
                duracion_global_last, duracion_global_total,
                df_last_week, df,
                lab_global_last, lab_global_total
            )
            
            # 6.5. Cargar logo de la empresa
            logo_b64 = None
            try:
                reports_folder_id = get_reports_folder_id(planta)
                logo_file_id = _buscar_archivo_por_nombre(gdrive_client, reports_folder_id, "logo_latitud_2.png")
                if logo_file_id:
                    logo_bytes = gdrive_client.download_file("logo_latitud_2.png", file_id=logo_file_id)
                    logo_b64 = base64.b64encode(logo_bytes).decode("utf-8")
                    logger.info("[Reporte] ✓ Logo cargado exitosamente")
            except Exception as e:
                logger.warning("[Reporte] No se pudo cargar el logo: %s", e)
            
            # 7. Generar HTML
            logger.info("[Reporte] Generando HTML...")
            html_content = _generar_html(
                planta, figs_dir, tablas, comentarios,
                resumen_cant_global_last, resumen_cant_global_total,
                duracion_global_last, duracion_global_total,
                temp_turno_last, df_last_week, df, tablas.get("dur_semana"),
                logo_b64=logo_b64
            )
            logger.info("[Reporte] ✓ HTML generado")
            
            # 8. Subir gráficos y HTML a Google Drive
            reports_folder_id = get_reports_folder_id(planta)
            logger.info(f"[Reporte] Subiendo archivos a carpeta reports: {reports_folder_id}")
            
            # Subir todos los gráficos
            for archivo_png in figs_dir.glob("*.png"):
                with open(archivo_png, "rb") as f:
                    contenido = f.read()
                _subir_o_actualizar_archivo(
                    gdrive_client, reports_folder_id,
                    archivo_png.name, contenido, "image/png"
                )
            
            # Subir HTML
            nombre_html = f"reporte_tachadas_{planta}.html"
            html_bytes = html_content.encode("utf-8")
            _subir_o_actualizar_archivo(
                gdrive_client, reports_folder_id,
                nombre_html, html_bytes, "text/html"
            )
            
            logger.info(f"[Reporte] ✓ Reporte completo generado: {nombre_html}")
            
            return {
                "success": True,
                "filas": len(df),
                "html": nombre_html,
                "mensaje": f"Reporte generado exitosamente: {nombre_html}"
            }
            
    except Exception as e:
        logger.exception(f"[Reporte] Error durante generación: {e}")
        return {
            "success": False,
            "filas": 0,
            "html": None,
            "mensaje": f"Error: {str(e)}"
        }


# Funciones helper para generación de reporte

def _generar_graficos(
    df, df_last_week, df_total,
    resumen_cant_global_last, resumen_cant_global_total,
    figs_dir: Path
) -> None:
    """Genera todos los gráficos del reporte."""
    # Configurar matplotlib
    plt.rcParams["figure.dpi"] = 120
    
    # Preparar datos para gráficos
    df["semana"] = df["fecha_fin"].dt.to_period("W").apply(lambda r: r.start_time)
    df["es_problema"] = df["prediccion"].fillna(0).astype(int)
    
    # BLOQUE 1: Cantidad de tachadas
    # 1.1 Pie charts global
    row_last = resumen_cant_global_last.iloc[0]
    row_total = resumen_cant_global_total.iloc[0]
    
    n_tach_last = int(row_last["n_tachadas"])
    n_prob_last = int(row_last["n_problema"])
    n_ok_last = n_tach_last - n_prob_last
    
    n_tach_tot = int(row_total["n_tachadas"])
    n_prob_tot = int(row_total["n_problema"])
    n_ok_tot = n_tach_tot - n_prob_tot
    
    fig, axes = plt.subplots(1, 2, figsize=(8, 4))
    axes[0].pie([n_prob_last, n_ok_last], labels=["Con problema", "Sin problema"],
                autopct="%1.1f%%", startangle=90, colors=[COLOR_PROBLEMA, "#D9D9D9"])
    axes[0].set_title("Última semana")
    axes[1].pie([n_prob_tot, n_ok_tot], labels=["Con problema", "Sin problema"],
                autopct="%1.1f%%", startangle=90, colors=[COLOR_PROBLEMA, "#D9D9D9"])
    axes[1].set_title("Total histórico")
    plt.tight_layout()
    plt.savefig(figs_dir / "b1_pie_global_last_vs_total.png", bbox_inches="tight")
    plt.close()
    
    # 1.2 Evolución semanal
    cant_semana = (
        df.groupby("semana", as_index=False)
        .agg(n_tachadas=("es_problema", "count"), n_problema=("es_problema", "sum"))
    )
    cant_semana["pct_problema"] = cant_semana["n_problema"] / cant_semana["n_tachadas"] * 100
    
    plt.figure(figsize=(10, 5))
    plt.plot(cant_semana["semana"], cant_semana["n_tachadas"], label="N° tachadas", color=COLOR_OK)
    plt.plot(cant_semana["semana"], cant_semana["n_problema"], label="N° tachadas con problema", color=COLOR_PROBLEMA)
    plt.xticks(rotation=45)
    plt.ylabel("Cantidad")
    plt.title("Evolución semanal de tachadas y tachadas con problema")
    plt.legend()
    plt.tight_layout()
    plt.savefig(figs_dir / "b1_tachadas_vs_problemas_semanal.png", bbox_inches="tight")
    plt.close()
    
    plt.figure(figsize=(10, 4))
    plt.plot(cant_semana["semana"], cant_semana["pct_problema"], color=COLOR_PROBLEMA)
    plt.xticks(rotation=45)
    plt.ylabel("% con problemas")
    plt.title("Evolución semanal del % de tachadas con problema")
    plt.tight_layout()
    plt.savefig(figs_dir / "b1_pct_problemas_semanal.png", bbox_inches="tight")
    plt.close()
    
    # 1.3 Última semana por secadora
    df_last_week_cpy = df_last_week.copy()
    df_last_week_cpy["es_problema"] = df_last_week_cpy["prediccion"].astype(int)
    resumen_last_sec = (
        df_last_week_cpy.groupby("sensor_id", as_index=False)
        .agg(n_tachadas=("es_problema", "count"), n_problema=("es_problema", "sum"))
    )
    resumen_last_sec["pct_problema"] = resumen_last_sec["n_problema"] / resumen_last_sec["n_tachadas"] * 100
    
    # Ordenar por sensor_id para mejor visualización
    resumen_last_sec = resumen_last_sec.sort_values("sensor_id")
    
    # Ajustar ancho de figura dinámicamente según número de secadoras
    fig_width = max(6, len(resumen_last_sec) * 2)
    plt.figure(figsize=(fig_width, 6))
    # Usar posiciones discretas (0, 1, 2, ...) en lugar de valores numéricos directos
    x_positions = range(len(resumen_last_sec))
    bars = plt.bar(x_positions, resumen_last_sec["pct_problema"], color=COLOR_PROBLEMA, width=0.3)
    plt.ylabel("% con problemas", fontsize=12)
    plt.xlabel("Secadora", fontsize=12)
    plt.title("Última semana: % de tachadas con problema por secadora", fontsize=14, fontweight="bold")
    # Configurar etiquetas categóricas en el eje X
    plt.xticks(x_positions, resumen_last_sec["sensor_id"], rotation=0)
    plt.grid(axis="y", alpha=0.3, linestyle="--")
    
    # Añadir etiquetas encima de cada barra con el porcentaje
    plt.bar_label(bars, labels=[f"{val:.1f}%" for val in resumen_last_sec["pct_problema"]], padding=3, fontsize=10)
    
    # Configurar eje Y hasta 100%
    plt.ylim(0, 100)
    
    plt.tight_layout()
    plt.savefig(figs_dir / "b1_pct_problemas_ultima_semana_por_secadora.png", bbox_inches="tight", dpi=120)
    plt.close()
    
    # BLOQUE 2: Temperaturas
    # Detectar columna de temperatura máxima
    col_temp_max = _detectar_columna_temp_max(df)
    
    if col_temp_max:
        # 2.1 Boxplot por secadora
        try:
            plt.figure(figsize=(18, 10))
            df.boxplot(column=col_temp_max, by="sensor_id")
            plt.title("Distribución de temperatura máxima por secadora", fontsize=16)
            plt.suptitle("")
            plt.ylabel("Temp. máxima (°C)", fontsize=14)
            plt.xlabel("Secadora", fontsize=14)
            plt.tight_layout()
            plt.savefig(figs_dir / "b2_boxplot_temp_max_por_secadora.png", bbox_inches="tight")
            plt.close()
        except Exception as e:
            logger.warning(f"Error generando boxplot temp por secadora: {e}")
        
        # 2.2 Boxplot por turno
        try:
            plt.figure(figsize=(18, 10))
            df.boxplot(column=col_temp_max, by="momento_dia")
            plt.title("Distribución de temperatura máxima por turno", fontsize=16)
            plt.suptitle("")
            plt.ylabel("Temp. máxima (°C)", fontsize=14)
            plt.xlabel("Turno", fontsize=14)
            plt.tight_layout()
            plt.savefig(figs_dir / "b2_boxplot_temp_max_por_turno.png", bbox_inches="tight")
            plt.close()
        except Exception as e:
            logger.warning(f"Error generando boxplot temp por turno: {e}")
        
        # 2.3 Boxplot por variedad
        try:
            plt.figure(figsize=(40, 10))
            df.boxplot(column=col_temp_max, by="variedad")
            plt.title("Distribución de temperatura máxima por variedad", fontsize=16)
            plt.suptitle("")
            plt.ylabel("Temp. máxima (°C)", fontsize=12)
            plt.xlabel("Variedad", fontsize=12)
            plt.xticks(rotation=45, ha='right', fontsize=12)
            plt.tight_layout()
            plt.savefig(figs_dir / "b2_boxplot_temp_max_por_variedad.png", bbox_inches="tight")
            plt.close()
        except Exception as e:
            logger.warning(f"Error generando boxplot temp por variedad: {e}")
        
        # 2.4 Scatter temp vs humedad
        col_hum = _detectar_columna_humedad_en_temp_max(df)
        if col_hum and col_temp_max in df.columns and col_hum in df.columns:
            try:
                plt.figure(figsize=(6, 5))
                plt.scatter(df[col_temp_max], df[col_hum], alpha=0.5)
                plt.xlabel("Temp. máxima (°C)")
                plt.ylabel("Humedad al momento de la temp. máxima (%)")
                plt.title("Relación entre temp. máxima y humedad en el máximo")
                plt.tight_layout()
                plt.savefig(figs_dir / "b2_scatter_temp_max_vs_humedad.png", bbox_inches="tight")
                plt.close()
            except Exception as e:
                logger.warning(f"Error generando scatter temp vs humedad: {e}")
    else:
        logger.warning("No se encontró columna de temperatura máxima. Omitiendo gráficos de temperatura.")
    
    # BLOQUE 3: Duración
    # 3.1 Boxplot global
    umbral_extremo = 30
    df_normal = df[df["duracion_horas"] <= umbral_extremo]
    
    plt.figure(figsize=(10, 4))
    plt.boxplot(df_normal["duracion_horas"], vert=False, showfliers=True)
    plt.xlabel("Duración (hs)")
    plt.title(f"Distribución de duración (≤ {umbral_extremo} hs)")
    plt.tight_layout()
    plt.savefig(figs_dir / "b3_boxplot_duracion_global_sin_ultras.png", bbox_inches="tight")
    plt.close()
    
    # Ultra outliers
    ultra = df[df["duracion_horas"] > umbral_extremo]
    if not ultra.empty:
        plt.figure(figsize=(10, 4))
        plt.scatter(ultra["fecha_fin"], ultra["duracion_horas"], color=COLOR_PROBLEMA)
        plt.xlabel("Fecha")
        plt.ylabel("Duración (hs)")
        plt.title(f"Tachadas extremadamente largas (> {umbral_extremo} hs)")
        plt.tight_layout()
        plt.savefig(figs_dir / "b3_scatter_ultra_outliers_duracion.png", bbox_inches="tight")
        plt.close()
    
    # 3.2 Boxplot por secadora
    df_focal = df[df["duracion_horas"] <= umbral_extremo]
    plt.figure(figsize=(10, 5))
    df_focal.boxplot(column="duracion_horas", by="sensor_id", vert=False)
    plt.title(f"Duración por secadora (≤ {umbral_extremo} hs)")
    plt.suptitle("")
    plt.xlabel("Duración (hs)")
    plt.tight_layout()
    plt.savefig(figs_dir / "b3_boxplot_duracion_por_secadora.png", bbox_inches="tight")
    plt.close()
    
    # 3.3 Evolución semanal
    df["semana_dom"] = df["fecha_fin"].dt.to_period("W-SUN").apply(lambda r: r.start_time)
    dur_semana = (
        df.groupby("semana_dom", as_index=False)
        .agg(duracion_media=("duracion_horas", "mean"), duracion_mediana=("duracion_horas", "median"))
    )
    
    plt.figure(figsize=(10, 5))
    plt.plot(dur_semana["semana_dom"], dur_semana["duracion_media"], label="Media semanal")
    plt.plot(dur_semana["semana_dom"], dur_semana["duracion_mediana"], label="Mediana semanal")
    plt.xticks(dur_semana["semana_dom"], rotation=45)
    plt.ylabel("Duración (hs)")
    plt.xlabel("Semana")
    plt.title("Evolución semanal de la duración de las tachadas")
    plt.legend()
    plt.tight_layout()
    plt.savefig(figs_dir / "b3_duracion_media_y_mediana_semanal.png", bbox_inches="tight")
    plt.close()
    
    # BLOQUE 4: Laboratorio
    if {"hum_fin_lab", "hum_30fin_prom"}.issubset(df.columns):
        try:
            plt.figure(figsize=(6, 6))
            plt.scatter(df["hum_30fin_prom"], df["hum_fin_lab"], alpha=0.5)
            plt.xlabel("Humedad sensor últimos 30 min (%)")
            plt.ylabel("Humedad final laboratorio (%)")
            plt.title("Sensor vs laboratorio (humedad final)")
            plt.tight_layout()
            plt.savefig(figs_dir / "b4_scatter_humedad_lab_vs_30min.png", bbox_inches="tight")
            plt.close()
        except Exception as e:
            logger.warning(f"Error generando scatter lab vs 30min: {e}")
    
    if "diff_hum_lab_vs_30fin" in df.columns:
        try:
            diff_semana = (
                df.groupby("semana", as_index=False)
                .agg(diff_prom=("diff_hum_lab_vs_30fin", "mean"))
            )
            
            plt.figure(figsize=(10, 4))
            plt.plot(diff_semana["semana"], diff_semana["diff_prom"])
            plt.xticks(rotation=45)
            plt.axhline(0, linestyle="--")
            plt.ylabel("Diferencia promedio (lab - 30min)")
            plt.title("Evolución semanal de la diferencia lab vs sensor")
            plt.tight_layout()
            plt.savefig(figs_dir / "b4_diferencia_lab_vs_30min_semanal.png", bbox_inches="tight")
            plt.close()
        except Exception as e:
            logger.warning(f"Error generando gráfico diferencia lab vs sensor: {e}")


def _preparar_tablas(
    resumen_cant_global_last, resumen_cant_global_total,
    resumen_cant_secadora_last, resumen_cant_secadora_total,
    temp_turno_last, temp_variedad_last,
    duracion_global_last, duracion_global_total,
    duracion_secadora_last,
    lab_global_last, lab_global_total, lab_secadora_last,
    df, df_last_week
) -> Dict[str, pd.DataFrame]:
    """Prepara todas las tablas formateadas del reporte."""
    RENAME_DURACION_COMUN = {
        "duracion_mean": "Media (hs)",
        "duracion_media": "Media (hs)",
        "duracion_mediana": "Mediana (hs)",
        "duracion_median": "Mediana (hs)",
        "duracion_min": "Mínimo (hs)",
        "duracion_max": "Máximo (hs)",
        "duracion_p25": "P25 (hs)",
        "duracion_p75": "P75 (hs)",
    }
    DEC1_DURACION_COLS = ["Media (hs)", "Mediana (hs)", "Mínimo (hs)", "Máximo (hs)", "P25 (hs)", "P75 (hs)"]
    
    tablas = {}
    
    # Tablas de cantidad
    tablas["cant_global_last"] = preparar_tabla(
        resumen_cant_global_last,
        rename={"ambito": "Ámbito", "n_tachadas": "Tachadas", "n_problema": "Con problema", "pct_problema": "% con problema"},
        int_cols=["Tachadas", "Con problema"],
        pct_cols=["% con problema"],
    )
    tablas["cant_global_total"] = preparar_tabla(
        resumen_cant_global_total,
        rename={"ambito": "Ámbito", "n_tachadas": "Tachadas", "n_problema": "Con problema", "pct_problema": "% con problema"},
        int_cols=["Tachadas", "Con problema"],
        pct_cols=["% con problema"],
    )
    tablas["cant_secadora_last"] = preparar_tabla(
        resumen_cant_secadora_last,
        rename={"sensor_id": "Secadora", "n_tachadas": "Tachadas", "n_problema": "Con problema", "pct_problema": "% con problema"},
        int_cols=["Tachadas", "Con problema"],
        pct_cols=["% con problema"],
    )
    tablas["cant_secadora_total"] = preparar_tabla(
        resumen_cant_secadora_total,
        rename={"sensor_id": "Secadora", "n_tachadas": "Tachadas", "n_problema": "Con problema", "pct_problema": "% con problema"},
        int_cols=["Tachadas", "Con problema"],
        pct_cols=["% con problema"],
    )
    
    # Tablas de temperatura
    tablas["temp_turno_last"] = preparar_tabla(
        temp_turno_last,
        rename={"momento_dia": "Turno", "temp_max_grados": "Temp. máx. (°C)", "hum_al_temp_max": "Humedad al máximo (%)"},
        dec1_cols=["Temp. máx. (°C)"],
        dec2_cols=["Humedad al máximo (%)"],
    )
    tablas["temp_variedad_last"] = preparar_tabla(
        temp_variedad_last,
        rename={"variedad": "Variedad", "temp_max_grados": "Temp. máx. (°C)", "hum_al_temp_max": "Humedad al máximo (%)"},
        dec1_cols=["Temp. máx. (°C)"],
        dec2_cols=["Humedad al máximo (%)"],
    )
    
    # Tablas de duración
    tablas["duracion_global_last"] = preparar_tabla(
        duracion_global_last,
        rename={"ambito": "Ámbito", "n_tachadas": "Tachadas", **RENAME_DURACION_COMUN},
        int_cols=["Tachadas"],
        dec1_cols=DEC1_DURACION_COLS,
    )
    tablas["duracion_global_total"] = preparar_tabla(
        duracion_global_total,
        rename={"ambito": "Ámbito", "n_tachadas": "Tachadas", **RENAME_DURACION_COMUN},
        int_cols=["Tachadas"],
        dec1_cols=DEC1_DURACION_COLS,
    )
    
    # Top tachadas largas
    umbral_extremo = 30
    df_ultra = df[df["duracion_horas"] > umbral_extremo]
    # Incluir ID_tachada siempre como primera columna si existe
    if "ID_tachada" in df.columns:
        top_largas = (
            df[["ID_tachada", "duracion_horas", "sensor_id", "fecha_fin", "variedad"]]
            .sort_values("duracion_horas", ascending=False)
            .head(10)
        )
        tablas["top_largas"] = preparar_tabla(
            top_largas,
            rename={
                "ID_tachada": "ID tachada",
                "duracion_horas": "Duración (hs)",
                "sensor_id": "Secadora",
                "fecha_fin": "Fin de secado",
                "variedad": "Variedad",
            },
            int_cols=["Secadora"],
            dec1_cols=["Duración (hs)"],
        )
    else:
        top_largas = (
            df[["duracion_horas", "sensor_id", "fecha_fin", "variedad"]]
            .sort_values("duracion_horas", ascending=False)
            .head(10)
        )
        tablas["top_largas"] = preparar_tabla(
            top_largas,
            rename={
                "duracion_horas": "Duración (hs)",
                "sensor_id": "Secadora",
                "fecha_fin": "Fin de secado",
                "variedad": "Variedad",
            },
            int_cols=["Secadora"],
            dec1_cols=["Duración (hs)"],
        )
    
    # Duración por secadora
    tablas["duracion_secadora_last"] = preparar_tabla(
        duracion_secadora_last,
        rename={"sensor_id": "Secadora", "n_tachadas": "Tachadas", **RENAME_DURACION_COMUN},
        int_cols=["Tachadas"],
        dec1_cols=DEC1_DURACION_COLS,
    )
    
    if not df_ultra.empty:
        # Incluir ID_tachada siempre como primera columna si existe
        if "ID_tachada" in df_ultra.columns:
            tablas["ultra"] = preparar_tabla(
                df_ultra[["ID_tachada", "sensor_id", "duracion_horas", "fecha_fin"]],
                rename={
                    "ID_tachada": "ID tachada",
                    "sensor_id": "Secadora",
                    "duracion_horas": "Duración (hs)",
                    "fecha_fin": "Fin de secado",
                },
                dec1_cols=["Duración (hs)"],
            )
        else:
            tablas["ultra"] = preparar_tabla(
                df_ultra[["sensor_id", "duracion_horas", "fecha_fin"]],
                rename={
                    "sensor_id": "Secadora",
                    "duracion_horas": "Duración (hs)",
                    "fecha_fin": "Fin de secado",
                },
                dec1_cols=["Duración (hs)"],
            )
    else:
        tablas["ultra"] = pd.DataFrame(
            {"ID tachada": [], "Secadora": [], "Duración (hs)": [], "Fin de secado": []}
        )
    
    # Duración semanal
    df["semana_dom"] = df["fecha_fin"].dt.to_period("W-SUN").apply(lambda r: r.start_time)
    dur_semana = (
        df.groupby("semana_dom", as_index=False)
        .agg(duracion_media=("duracion_horas", "mean"), duracion_mediana=("duracion_horas", "median"))
    )
    tablas["dur_semana"] = preparar_tabla(
        dur_semana,
        rename={"semana_dom": "Semana", **RENAME_DURACION_COMUN},
        dec1_cols=DEC1_DURACION_COLS,
    )
    
    # Tablas de laboratorio
    # Eliminar columna "ambito" si existe antes de formatear
    if "ambito" in lab_global_last.columns:
        lab_global_last = lab_global_last.drop(columns=["ambito"])
    if "ambito" in lab_global_total.columns:
        lab_global_total = lab_global_total.drop(columns=["ambito"])
    if "ambito" in lab_secadora_last.columns:
        lab_secadora_last = lab_secadora_last.drop(columns=["ambito"])
    
    # Definir mapeo de columnas para laboratorio
    RENAME_LAB_COMUN = {
        "hum_ini_lab": "Humedad Inicial Lab (%)",
        "hum_fin_lab_prom": "Humedad Final Lab (%)",
        "hum_30fin_prom": "Humedad Ultimos 30 min (%)",
        "diff_lab_30fin_prom": "Diferencia Lab-Ultimos 30min (pp)",
    }
    DEC2_LAB_COLS = [
        "Humedad Inicial Lab (%)",
        "Humedad Final Lab (%)",
        "Humedad Ultimos 30 min (%)",
        "Diferencia Lab-Ultimos 30min (pp)",
    ]
    
    # Preparar tablas de laboratorio globales (sin sensor_id)
    tablas["lab_global_last"] = preparar_tabla(
        lab_global_last,
        rename=RENAME_LAB_COMUN,
        dec2_cols=DEC2_LAB_COLS,
    )
    tablas["lab_global_total"] = preparar_tabla(
        lab_global_total,
        rename=RENAME_LAB_COMUN,
        dec2_cols=DEC2_LAB_COLS,
    )
    
    # Preparar tabla de laboratorio por secadora (con sensor_id)
    RENAME_LAB_SECADORA = {
        "sensor_id": "Secadora",
        **RENAME_LAB_COMUN,
    }
    tablas["lab_secadora_last"] = preparar_tabla(
        lab_secadora_last,
        rename=RENAME_LAB_SECADORA,
        dec2_cols=DEC2_LAB_COLS,
    )
    
    # Eliminar columna 'Ámbito' de tablas globales (no aporta información)
    for key in [
        "cant_global_last",
        "cant_global_total",
        "duracion_global_last",
        "duracion_global_total",
        "lab_global_last",
        "lab_global_total",
    ]:
        if key in tablas and "Ámbito" in tablas[key].columns:
            tablas[key] = tablas[key].drop(columns=["Ámbito"])
    
    return tablas


def _generar_comentarios(
    resumen_cant_global_last, resumen_cant_global_total,
    duracion_global_last, duracion_global_total,
    df_last_week, df,
    lab_global_last, lab_global_total
) -> Dict[str, str]:
    """Genera los comentarios interpretativos del reporte."""
    comentarios = {}
    
    # Bloque 1: Cantidad
    pct_last = float(resumen_cant_global_last["pct_problema"].iloc[0])
    pct_hist = float(resumen_cant_global_total["pct_problema"].iloc[0])
    
    if pct_last < pct_hist:
        tendencia_b1 = "por debajo del promedio histórico"
    elif pct_last > pct_hist:
        tendencia_b1 = "por encima del promedio histórico"
    else:
        tendencia_b1 = "en línea con el promedio histórico"
    
    comentarios["b1"] = (
        f"En la última semana, el <strong>{pct_last:.1f}%</strong> de las tachadas presentó problemas, "
        f"mientras que el promedio histórico es de <strong>{pct_hist:.1f}%</strong>. "
        f"Esto indica que la semana se encuentra <strong>{tendencia_b1}</strong> en términos de calidad del secado."
    )
    
    # Bloque 2: Temperaturas
    col_temp_max = _detectar_columna_temp_max(df_last_week)
    if col_temp_max and col_temp_max in df_last_week.columns and not df_last_week.empty:
        try:
            fila_max = df_last_week.loc[df_last_week[col_temp_max].idxmax()]
            temp_max_semana = float(fila_max[col_temp_max])
            turno_max = str(fila_max.get("momento_dia", "N/D"))
            variedad_max = str(fila_max.get("variedad", "N/D"))
            comentarios["b2"] = (
                f"En la última semana, la temperatura máxima registrada fue de <strong>{temp_max_semana:.1f} °C</strong>, "
                f"alcanzada en el turno <strong>{turno_max}</strong> para la variedad <strong>{variedad_max}</strong>."
            )
        except Exception as e:
            logger.warning(f"Error obteniendo temp_max para comentarios: {e}")
            comentarios["b2"] = (
                "En este período no se dispone de datos de temperatura máxima suficientes como para construir "
                "un resumen interpretativo por turno y variedad."
            )
    else:
        comentarios["b2"] = (
            "En este período no se dispone de datos de temperatura máxima suficientes como para construir "
            "un resumen interpretativo por turno y variedad."
        )
    
    # Bloque 3: Duración
    dur_med_last = float(duracion_global_last["duracion_mean"].iloc[0])
    dur_med_hist = float(duracion_global_total["duracion_mean"].iloc[0])
    umbral_extremo = 30
    df_ultra = df[df["duracion_horas"] > umbral_extremo]
    n_ultra = len(df_ultra)
    
    if dur_med_last < dur_med_hist:
        tendencia_b3 = "ligeramente por debajo del histórico"
    elif dur_med_last > dur_med_hist:
        tendencia_b3 = "por encima del histórico"
    else:
        tendencia_b3 = "muy alineada con el histórico"
    
    comentarios["b3"] = (
        f"La duración promedio de las tachadas en la última semana fue de <strong>{dur_med_last:.1f} horas</strong>, "
        f"frente a un promedio histórico de <strong>{dur_med_hist:.1f} horas</strong>, lo que sugiere que la duración típica "
        f"de las tachadas está <strong>{tendencia_b3}</strong>. "
        f"Además, se identificaron <strong>{n_ultra}</strong> tachadas extremadamente largas (ultra-outliers)."
    )
    
    # Bloque 4: Laboratorio
    cols_needed = ["hum_fin_lab_prom", "diff_lab_30fin_prom"]
    if all(col in lab_global_last.columns for col in cols_needed):
        hum_fin_last = lab_global_last["hum_fin_lab_prom"].iloc[0]
        hum_fin_hist = lab_global_total["hum_fin_lab_prom"].iloc[0]
        diff_last = lab_global_last["diff_lab_30fin_prom"].iloc[0]
        
        if pd.isna(hum_fin_last) or pd.isna(hum_fin_hist) or pd.isna(diff_last):
            comentarios["b4"] = (
                "Si bien se dispone de la estructura de los datos de laboratorio, algunos valores de "
                "humedad final o diferencias con los datos del sensor no están disponibles para el periodo "
                "analizado. Por lo tanto, la comparación con laboratorio es parcial o no concluyente."
            )
        else:
            comentarios["b4"] = (
                f"La humedad final promedio de laboratorio en la última semana fue de <strong>{hum_fin_last:.1f}%</strong>, "
                f"mientras que el promedio histórico es de <strong>{hum_fin_hist:.1f}%</strong>. "
                f"La diferencia media entre la medición del laboratorio y la estimación del sensor en los últimos 30 minutos "
                f"fue de <strong>{diff_last:.2f} puntos porcentuales</strong>. "
                "Esto permite evaluar la alineación entre sensor y laboratorio y detectar posibles descalibraciones."
            )
    else:
        comentarios["b4"] = (
            "En este período <strong>no se dispone de datos de laboratorio suficientes</strong>, "
            "por lo que la comparación sensor–laboratorio es parcial o no concluyente."
        )
    
    return comentarios


def _generar_html(
    planta, figs_dir, tablas, comentarios,
    resumen_cant_global_last, resumen_cant_global_total,
    duracion_global_last, duracion_global_total,
    temp_turno_last, df_last_week, df, dur_semana,
    logo_b64: Optional[str] = None
) -> str:
    """Genera el HTML completo del reporte."""
    html = []
    
    # KPIs
    kpi_tachadas_last = int(resumen_cant_global_last["n_tachadas"].iloc[0])
    kpi_pct_prob_last = float(resumen_cant_global_last["pct_problema"].iloc[0])
    kpi_dur_media_last = float(duracion_global_last["duracion_mean"].iloc[0])
    kpi_temp_max_prom_last = float(temp_turno_last["temp_max_grados"].max()) if not temp_turno_last.empty else 0.0
    
    # HTML head y estilos
    html.append("""<html>
<head>
  <meta charset="utf-8">
  <title>Reporte de tachadas de secado</title>
  <style>
  body { 
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; 
    margin: 20px; 
    background-color: #fafafa;
    color: #222;
  }
  h1 { margin-bottom: 5px; }
  h2 { margin-top: 30px; border-bottom: 2px solid #e0e0e0; padding-bottom: 4px; }
  h3 { margin-top: 20px; }
  h4 { margin-top: 16px; margin-bottom: 6px; }
  table { 
    border-collapse: collapse; 
    margin: 8px 0 16px; 
    font-size: 13px;
    width: auto;
    max-width: 100%;
  }
  th, td { 
    border: 1px solid #ddd; 
    padding: 6px 8px; 
  }
  th { 
    background-color: #f3f4f6; 
    font-weight: 600;
    text-align: left;
  }
  td {
    text-align: right;
  }
  tr:nth-child(even) td { background-color: #fafafa; }
  img { 
    max-width: 100%; 
    height: auto; 
    margin-bottom: 16px; 
    border-radius: 4px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    background: #fff;
  }
  .subsection { 
    margin-bottom: 24px; 
    padding: 10px 12px;
    background: #ffffff;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  .two-col {
    display: flex;
    gap: 16px;
    align-items: flex-start;
  }
  .two-col .col {
    flex: 1;
  }
  .section {
    margin-bottom: 24px;
    background: #f8f8f8;
    padding: 16px 20px;
    border-radius: 10px;
    border: 1px solid #ddd;
  }
  .section h2 {
    margin-top: 0;
  }
  .section p {
    margin: 6px 0;
    font-size: 15px;
  }
  .section ul {
    margin: 8px 0 0 18px;
    padding: 0;
  }
  .section li {
    margin-bottom: 4px;
    font-size: 14px;
  }
  a {
    color: #1f4f7f;
    text-decoration: none;
  }
  a:hover {
    text-decoration: underline;
  }
  </style>
</head>
<body>""")
    
    # Encabezado
    now = datetime.now(ZoneInfo("America/Montevideo"))
    fecha_rep = now.strftime("%d/%m/%Y %H:%M")
    logo_html = f'<div><img src="data:image/png;base64,{logo_b64}" alt="Logo Latitud" style="height:60px;"></div>' if logo_b64 else ''
    html.append(f"""
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
  <div>
    <h1 style="margin:0; padding:0;">Reporte de Tachadas de Secado</h1>
    <div style="font-size:13px; color:#555;">Generado el {fecha_rep}</div>
  </div>
  {logo_html}
</div>""")
    
    # KPIs
    html.append(f"""
<div style="display:flex; flex-wrap:wrap; gap:16px; margin-bottom:30px;">
  <div style="flex:1; min-width:200px; padding:12px 16px; border-radius:8px; background:#f5f7fb;">
    <div style="font-size:12px; color:#555;">Tachadas (última semana)</div>
    <div style="font-size:22px; font-weight:bold; color:#222;">{kpi_tachadas_last}</div>
  </div>
  <div style="flex:1; min-width:200px; padding:12px 16px; border-radius:8px; background:#fff5f5;">
    <div style="font-size:12px; color:#555;">% con problemas (última semana)</div>
    <div style="font-size:22px; font-weight:bold; color:#b22222;">{kpi_pct_prob_last:.1f}%</div>
  </div>
  <div style="flex:1; min-width:200px; padding:12px 16px; border-radius:8px; background:#f5f7fb;">
    <div style="font-size:12px; color:#555;">Duración media (hs, última semana)</div>
    <div style="font-size:22px; font-weight:bold; color:#222;">{kpi_dur_media_last:.1f}</div>
  </div>
  <div style="flex:1; min-width:200px; padding:12px 16px; border-radius:8px; background:#f5f7fb;">
    <div style="font-size:12px; color:#555;">Temp. máxima máx. (°C, última semana)</div>
    <div style="font-size:22px; font-weight:bold; color:#222;">{kpi_temp_max_prom_last:.1f}</div>
  </div>
</div>""")
    
    # Resumen interpretativo
    html.append('<div class="section">')
    html.append("<h2>Resumen interpretativo de la semana</h2>")
    html.append("<ul>")
    html.append(f"<li><strong>Cantidad de tachadas:</strong> {comentarios['b1']}</li>")
    html.append(f"<li><strong>Temperaturas:</strong> {comentarios['b2']}</li>")
    html.append(f"<li><strong>Duración:</strong> {comentarios['b3']}</li>")
    html.append(f"<li><strong>Comparación con laboratorio:</strong> {comentarios['b4']}</li>")
    html.append("</ul>")
    html.append("</div>")
    
    # Índice
    html.append("""
<h2>Índice</h2>
<ul>
  <li><a href="#bloque1">1. Cantidad de tachadas</a></li>
  <li><a href="#bloque2">2. Temperaturas</a></li>
  <li><a href="#bloque3">3. Duración de las tachadas</a></li>
  <li><a href="#bloque4">4. Comparación con laboratorio</a></li>
</ul>""")
    
    # Bloque 1: Cantidad
    html.append('<h2 id="bloque1">1. Cantidad de tachadas</h2>')
    html.append('<div class="subsection two-col">')
    html.append('<div class="col"><h3>1.1 Resumen global</h3>')
    html.append("<p>Última semana:</p>")
    html.append(tablas["cant_global_last"].to_html(index=False))
    html.append("<p>Total histórico:</p>")
    html.append(tablas["cant_global_total"].to_html(index=False))
    html.append("<h4>Distribución de tachadas con problema</h4>")
    html.append(img_inline(figs_dir / "b1_pie_global_last_vs_total.png", alt="Distribución de tachadas con problema"))
    html.append("</div>")
    html.append('<div class="col"><h3>1.2 Por secadora</h3>')
    html.append("<p>Última semana:</p>")
    html.append(tablas["cant_secadora_last"].to_html(index=False))
    html.append("<p>Total histórico:</p>")
    html.append(tablas["cant_secadora_total"].to_html(index=False))
    html.append("</div>")
    html.append("</div>")
    html.append('<div class="subsection"><h3>1.3 Gráficos históricos</h3>')
    html.append("<p>Evolución semanal de tachadas y tachadas con problema:</p>")
    html.append(img_inline(figs_dir / "b1_tachadas_vs_problemas_semanal.png", alt="Tachadas vs problemas por semana"))
    html.append("<p>Evolución semanal del % de tachadas con problema:</p>")
    html.append(img_inline(figs_dir / "b1_pct_problemas_semanal.png", alt="% problemas por semana"))
    html.append("<p>Última semana: % de tachadas con problema por secadora:</p>")
    html.append(img_inline(figs_dir / "b1_pct_problemas_ultima_semana_por_secadora.png", alt="% problemas última semana por secadora"))
    html.append("</div>")
    
    # Bloque 2: Temperaturas
    html.append('<h2 id="bloque2">2. Temperaturas</h2>')
    html.append('<div class="subsection two-col">')
    html.append('<div class="col"><h3>2.1 Máxima por turno (última semana)</h3>')
    html.append(tablas["temp_turno_last"].to_html(index=False))
    html.append("</div>")
    html.append('<div class="col"><h3>2.2 Máxima por variedad (última semana)</h3>')
    html.append(tablas["temp_variedad_last"].to_html(index=False))
    html.append("</div>")
    html.append("</div>")
    html.append('<div class="subsection"><h3>2.3 Distribución por secadora, turno y variedad</h3>')
    html.append("<p>Temp. máxima por secadora:</p>")
    html.append(img_inline(figs_dir / "b2_boxplot_temp_max_por_secadora.png", alt="Boxplot temp máxima por secadora"))
    html.append("<p>Temp. máxima por turno:</p>")
    html.append(img_inline(figs_dir / "b2_boxplot_temp_max_por_turno.png", alt="Boxplot temp máxima por turno"))
    html.append("<p>Temp. máxima por variedad:</p>")
    html.append(img_inline(figs_dir / "b2_boxplot_temp_max_por_variedad.png", alt="Boxplot temp máxima por variedad"))
    html.append("</div>")
    html.append('<div class="subsection"><h3>2.4 Relación temperatura y humedad</h3>')
    html.append(img_inline(figs_dir / "b2_scatter_temp_max_vs_humedad.png", alt="Scatter temp máxima vs humedad al máximo"))
    html.append("</div>")
    
    # Bloque 3: Duración
    html.append('<h2 id="bloque3">3. Duración de las tachadas</h2>')
    html.append('<div class="subsection"><h3>3.1 Resumen global</h3>')
    html.append("<p>Última semana:</p>")
    html.append(tablas["duracion_global_last"].to_html(index=False))
    html.append("<p>Total histórico:</p>")
    html.append(tablas["duracion_global_total"].to_html(index=False))
    html.append("</div>")
    html.append('<div class="subsection"><h3>3.2 Distribución de la duración</h3>')
    html.append("<p>Distribución global de duración (sin ultra-outliers):</p>")
    html.append(img_inline(figs_dir / "b3_boxplot_duracion_global_sin_ultras.png", alt="Boxplot duración global sin ultra-outliers"))
    html.append("<p>Tachadas extremadamente largas (ultra-outliers):</p>")
    html.append(img_inline(figs_dir / "b3_scatter_ultra_outliers_duracion.png", alt="Scatter ultra-outliers de duración"))
    html.append("<h4>Top 10 tachadas por duración</h4>")
    html.append(tablas["top_largas"].to_html(index=False))
    html.append("</div>")
    html.append('<div class="subsection"><h3>3.3 Duración por secadora (última semana)</h3>')
    html.append(tablas["duracion_secadora_last"].to_html(index=False))
    html.append("<p>Distribución de duración por secadora:</p>")
    html.append(img_inline(figs_dir / "b3_boxplot_duracion_por_secadora.png", alt="Boxplot duración por secadora"))
    html.append("<h4>Tachadas extremadamente largas (> 30 hs.)</h4>")
    html.append(tablas["ultra"].to_html(index=False))
    html.append("</div>")
    html.append('<div class="subsection"><h3>3.4 Evolución histórica</h3>')
    html.append(img_inline(figs_dir / "b3_duracion_media_y_mediana_semanal.png", alt="Duración media y mediana semanal"))
    html.append("<h4>Tabla de duración por semana</h4>")
    html.append(tablas["dur_semana"].to_html(index=False))
    html.append("</div>")
    
    # Bloque 4: Laboratorio
    html.append('<h2 id="bloque4">4. Comparación con Laboratorio</h2>')
    html.append('<div class="subsection two-col">')
    html.append('<div class="col"><h3>4.1 Resumen global</h3>')
    html.append("<p>Última semana:</p>")
    html.append(tablas["lab_global_last"].to_html(index=False))
    html.append("<p>Total histórico:</p>")
    html.append(tablas["lab_global_total"].to_html(index=False))
    html.append("</div>")
    html.append('<div class="col"><h3>4.2 Por secadora (última semana)</h3>')
    html.append(tablas["lab_secadora_last"].to_html(index=False))
    html.append("</div>")
    html.append("</div>")
    html.append('<div class="subsection"><h3>4.3 Gráficos de comparación sensor vs laboratorio</h3>')
    if (figs_dir / "b4_scatter_humedad_lab_vs_30min.png").exists():
        html.append("<p>Relación entre humedad final de laboratorio y del sensor (últimos 30 minutos):</p>")
        html.append(img_inline(figs_dir / "b4_scatter_humedad_lab_vs_30min.png", alt="Scatter humedad lab vs 30 min sensor"))
    if (figs_dir / "b4_diferencia_lab_vs_30min_semanal.png").exists():
        html.append("<p>Evolución semanal de la diferencia promedio (lab - 30 min):</p>")
        html.append(img_inline(figs_dir / "b4_diferencia_lab_vs_30min_semanal.png", alt="Diferencia lab vs 30 min semanal"))
    html.append("</div>")
    
    html.append("</body></html>")
    
    return "".join(html)


