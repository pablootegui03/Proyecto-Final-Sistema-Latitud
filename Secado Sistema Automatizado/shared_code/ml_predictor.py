"""
Módulo de predicción ML para tachadas de secado de arroz.

Implementa el pipeline completo de ML basado en el notebook de producción:
- Descarga archivo procesado desde Google Drive
- Limpia y procesa datos
- Genera features usando resumir_tachadas_v3
- Carga modelo CatBoost y predice
- Calcula hum_30fin_prom
- Incluye HumedadInicial y HumedadFinal si están presentes
- Sube CSV de predicciones a carpeta validated
"""

import io
import os
from pathlib import Path
from typing import Dict, Any

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier

from shared_code.config import get_processed_folder_id, get_validated_folder_id
from shared_code.gdrive_client import GoogleDriveClient
from shared_code.minimal_logger import log, log_error, log_debug

# Umbral final del modelo elegido en la tesis
UMBRAL = 0.20

# Features del modelo en orden exacto del entrenamiento
FEATURES_MODELO = [
    "humedad_mean", "humedad_std", "humedad_min", "humedad_max",
    "temp_mean", "temp_std", "temp_min", "temp_max",
    "variedad", "sensor_id",
    "humedad_range", "temp_range",
    "humedad_p25", "humedad_p75",
    "temp_p25", "temp_p75",
    "duracion_horas", "hora_inicio", "momento_dia",
    "slope_temp", "slope_hum",
    "temp_cross_38", "hum_cross_11",
    "temp_shocks_3", "hum_shocks_5",
    "temp_time_above_38", "hum_time_above_11",
    "slope_temp_Q1", "slope_temp_Q4",
    "slope_hum_Q1", "slope_hum_Q4",
    "drying_rate", "drop_ratio_temp", "drop_ratio_hum",
    "hum_final_above_13"
]

# Columnas que el modelo NO usa y deben eliminarse antes de predecir
COLS_BORRAR = ["ID_tachada", "planta", "año", "timestamp_min", "timestamp_max"]

# Columnas categóricas
CAT_COLS = ["variedad", "sensor_id", "momento_dia"]


def _get_model_path() -> str:
    """Obtiene la ruta al archivo del modelo."""
    # Intentar varias ubicaciones posibles
    possible_paths = [
        Path(__file__).parent / "modelo_tachadas.cbm",
        Path("shared_code") / "modelo_tachadas.cbm",
        Path("modelo_tachadas.cbm"),
    ]
    
    for path in possible_paths:
        if path.exists():
            return str(path)
    
    raise FileNotFoundError(
        f"No se encontró modelo_tachadas.cbm en ninguna de estas ubicaciones: {[str(p) for p in possible_paths]}"
    )


def _load_model() -> CatBoostClassifier:
    """Carga el modelo CatBoost desde el archivo .cbm."""
    model_path = _get_model_path()
    log(f"Cargando modelo desde: {model_path}", "INFO", "ML")
    
    modelo = CatBoostClassifier()
    modelo.load_model(model_path)
    log("Modelo cargado exitosamente", "INFO", "ML")
    return modelo


def _limpiar_datos(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y prepara el DataFrame según la lógica del notebook.
    
    Args:
        df_raw: DataFrame crudo cargado desde el CSV
        
    Returns:
        DataFrame limpio y preparado
    """
    df = df_raw.copy()
    
    # Convertir timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    
    # Eliminar filas sin timestamp válido
    df = df.dropna(subset=["timestamp"])
    
    # Eliminar filas sin valores de sensores
    df = df.dropna(subset=["TEMPERATURA", "HUMEDAD"])
    
    # Arreglar variedades
    df["Variedad"] = (
        df["Variedad"]
        .fillna("MERÍN")
        .replace("L5903", "MERÍN")
        .astype(str)
    )
    
    # Eliminar voltajes (no se usan y no deben aparecer en el CSV final)
    df = df.drop(columns=["VOLT_HUM", "VOLT_TEM"], errors="ignore")
    
    log(f"Datos limpiados: {df.shape[0]} filas, {df.shape[1]} columnas", "INFO", "ML")
    return df


def resumir_tachadas_v3(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera resumen de tachadas con todas las features v3.
    
    Esta función es una copia exacta de la función del notebook de producción.
    No modificar la lógica matemática.
    
    Args:
        df: DataFrame limpio con columnas: ID_tachada, timestamp, TEMPERATURA, HUMEDAD, etc.
        
    Returns:
        DataFrame con una fila por tachada y todas las features calculadas
    """
    df = df.sort_values(["ID_tachada", "timestamp"])
    
    # ===============================================================
    # 1) FEATURES ORIGINALES (V1)
    # ===============================================================
    resumen = df.groupby("ID_tachada").agg(
        humedad_mean=("HUMEDAD", "mean"),
        humedad_std=("HUMEDAD", "std"),
        humedad_min=("HUMEDAD", "min"),
        humedad_max=("HUMEDAD", "max"),
        temp_mean=("TEMPERATURA", "mean"),
        temp_std=("TEMPERATURA", "std"),
        temp_min=("TEMPERATURA", "min"),
        temp_max=("TEMPERATURA", "max"),
        timestamp_min=("timestamp", "min"),
        timestamp_max=("timestamp", "max"),
        variedad=("Variedad", "first"),
        sensor_id=("sensor_id", "first"),
        año=("año", "first"),
        planta=("planta", "first")
    ).reset_index()
    
    resumen["humedad_range"] = resumen["humedad_max"] - resumen["humedad_min"]
    resumen["temp_range"] = resumen["temp_max"] - resumen["temp_min"]
    
    resumen["humedad_p25"] = df.groupby("ID_tachada")["HUMEDAD"].quantile(0.25).values
    resumen["humedad_p75"] = df.groupby("ID_tachada")["HUMEDAD"].quantile(0.75).values
    resumen["temp_p25"] = df.groupby("ID_tachada")["TEMPERATURA"].quantile(0.25).values
    resumen["temp_p75"] = df.groupby("ID_tachada")["TEMPERATURA"].quantile(0.75).values
    
    resumen["duracion_horas"] = (
        resumen["timestamp_max"] - resumen["timestamp_min"]
    ).dt.total_seconds() / 3600
    
    resumen["hora_inicio"] = resumen["timestamp_min"].dt.hour
    
    condiciones = [
        resumen["hora_inicio"].between(6, 12),
        resumen["hora_inicio"].between(12, 18),
        resumen["hora_inicio"].between(18, 24),
        resumen["hora_inicio"].between(0, 6)
    ]
    categorias = ["mañana", "tarde", "noche", "madrugada"]
    resumen["momento_dia"] = np.select(condiciones, categorias, default="desconocido")
    
    def slope(grupo, col):
        if len(grupo) > 1:
            return (grupo[col].iloc[-1] - grupo[col].iloc[0]) / (
                (grupo["timestamp"].iloc[-1] - grupo["timestamp"].iloc[0]).total_seconds() + 1e-9
            )
        return 0
    
    resumen["slope_temp"] = df.groupby("ID_tachada").apply(slope, "TEMPERATURA").values
    resumen["slope_hum"] = df.groupby("ID_tachada").apply(slope, "HUMEDAD").values
    
    # ===============================================================
    # 2) FEATURES NUEVAS (V3)
    # ===============================================================
    UMBRAL_TEMP = 38
    UMBRAL_HUM = 11
    HUM_FINAL_BUENA = 13
    
    def count_crossings(series, thr):
        return np.sum((series.shift(1) < thr) & (series >= thr))
    
    def count_shocks(series, delta):
        return np.sum(series.diff().abs() > delta)
    
    def time_in_zone(df_t, col, thr):
        return np.sum(df_t[col] > thr)
    
    def segmented_slope(series):
        n = len(series)
        if n < 4:
            return (0, 0)
        q1 = series.iloc[:n//4]
        q4 = series.iloc[-n//4:]
        slope_q1 = (q1.iloc[-1] - q1.iloc[0]) / (len(q1) + 1e-6)
        slope_q4 = (q4.iloc[-1] - q4.iloc[0]) / (len(q4) + 1e-6)
        return slope_q1, slope_q4
    
    nuevas_cols = [
        "temp_cross_38", "hum_cross_11",
        "temp_shocks_3", "hum_shocks_5",
        "temp_time_above_38", "hum_time_above_11",
        "slope_temp_Q1", "slope_temp_Q4",
        "slope_hum_Q1", "slope_hum_Q4",
        "drying_rate", "drop_ratio_temp",
        "drop_ratio_hum", "hum_final_above_13"
    ]
    
    for c in nuevas_cols:
        resumen[c] = np.nan
    
    for idx, row in resumen.iterrows():
        id_tach = row["ID_tachada"]
        df_tach = df[df["ID_tachada"] == id_tach].sort_values("timestamp")
        
        resumen.at[idx, "temp_cross_38"] = count_crossings(df_tach["TEMPERATURA"], UMBRAL_TEMP)
        resumen.at[idx, "hum_cross_11"] = count_crossings(df_tach["HUMEDAD"], UMBRAL_HUM)
        
        resumen.at[idx, "temp_shocks_3"] = count_shocks(df_tach["TEMPERATURA"], 3)
        resumen.at[idx, "hum_shocks_5"] = count_shocks(df_tach["HUMEDAD"], 5)
        
        resumen.at[idx, "temp_time_above_38"] = time_in_zone(df_tach, "TEMPERATURA", UMBRAL_TEMP)
        resumen.at[idx, "hum_time_above_11"] = time_in_zone(df_tach, "HUMEDAD", UMBRAL_HUM)
        
        st1, st4 = segmented_slope(df_tach["TEMPERATURA"])
        sh1, sh4 = segmented_slope(df_tach["HUMEDAD"])
        resumen.at[idx, "slope_temp_Q1"] = st1
        resumen.at[idx, "slope_temp_Q4"] = st4
        resumen.at[idx, "slope_hum_Q1"] = sh1
        resumen.at[idx, "slope_hum_Q4"] = sh4
        
        resumen.at[idx, "drying_rate"] = (
            row["humedad_mean"] - row["humedad_min"]
        ) / (row["duracion_horas"] + 1e-6)
        
        resumen.at[idx, "drop_ratio_temp"] = row["temp_min"] / (row["temp_max"] + 1e-6)
        resumen.at[idx, "drop_ratio_hum"] = row["humedad_min"] / (row["humedad_max"] + 1e-6)
        
        resumen.at[idx, "hum_final_above_13"] = int(row["humedad_min"] > HUM_FINAL_BUENA)
    
    return resumen


def _calcular_hum_30fin_prom(resumen: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula la humedad promedio de los últimos 30 minutos por tachada.
    
    Args:
        resumen: DataFrame de resumen con ID_tachada
        df: DataFrame limpio con todas las mediciones
        
    Returns:
        DataFrame resumen con columna hum_30fin_prom agregada
    """
    resumen["hum_30fin_prom"] = np.nan
    
    for idx, row in resumen.iterrows():
        id_tach = row["ID_tachada"]
        
        # USAR df (limpiado), NO df_raw
        df_tach = df[df["ID_tachada"] == id_tach].sort_values("timestamp")
        
        # Último timestamp
        fin = df_tach["timestamp"].max()
        
        # Marca 30 minutos antes
        inicio_30 = fin - pd.Timedelta(minutes=30)
        
        # Filtrar últimas mediciones
        df_ultimos30 = df_tach[df_tach["timestamp"] >= inicio_30]
        
        # Asignar promedio
        if len(df_ultimos30) > 0:
            resumen.at[idx, "hum_30fin_prom"] = df_ultimos30["HUMEDAD"].mean()
        else:
            resumen.at[idx, "hum_30fin_prom"] = np.nan
    
    return resumen


def ejecutar_modelo_ml(gdrive_client: GoogleDriveClient, planta: str, archivo: str) -> Dict[str, Any]:
    """
    Ejecuta el pipeline completo de ML para predecir tachadas defectuosas.
    
    Args:
        gdrive_client: Cliente de Google Drive
        planta: Código de planta (JPV o RB)
        archivo: Nombre del archivo procesado (ej: "SENSOR20_processed_20251126T194042Z.csv")
        
    Returns:
        dict con:
            success: bool
            filas: int (número de tachadas procesadas)
            nombre_output: str (nombre del archivo CSV generado)
            mensaje: str (mensaje descriptivo)
    """
    try:
        log(f"Iniciando ML para planta={planta}, archivo={archivo}", "INFO", "ML")
        
        # 1) Obtener folder IDs
        processed_folder_id = get_processed_folder_id(planta)
        validated_folder_id = get_validated_folder_id(planta)
        
        log(f"Folder processed: {processed_folder_id}, validated: {validated_folder_id}", "DEBUG", "ML")
        
        # 2) Buscar archivo en carpeta processed
        archivos = gdrive_client.list_files_by_folder_id(processed_folder_id)
        archivo_info = None
        for f in archivos:
            if f["name"] == archivo:
                archivo_info = f
                break
        
        if not archivo_info:
            error_msg = f"Archivo '{archivo}' no encontrado en carpeta processed de {planta}"
            log(error_msg, "ERROR", "ML")
            return {
                "success": False,
                "filas": 0,
                "nombre_output": "",
                "mensaje": error_msg
            }
        
        log(f"Archivo encontrado: {archivo_info['id']}", "INFO", "ML")
        
        # 3) Descargar archivo
        log("Descargando archivo desde Google Drive...", "INFO", "ML")
        file_content = gdrive_client.download_file(file_path=archivo, file_id=archivo_info["id"])
        
        # 4) Convertir a DataFrame
        log("Convirtiendo CSV a DataFrame...", "INFO", "ML")
        df_raw = pd.read_csv(io.BytesIO(file_content))
        log(f"Archivo crudo cargado: {df_raw.shape}", "INFO", "ML")
        
        # 5) Limpiar datos
        log("Limpiando datos...", "INFO", "ML")
        df = _limpiar_datos(df_raw)
        
        if len(df) == 0:
            error_msg = "No quedaron datos válidos después de la limpieza"
            log(error_msg, "ERROR", "ML")
            return {
                "success": False,
                "filas": 0,
                "nombre_output": "",
                "mensaje": error_msg
            }
        
        # 6) Generar resumen con features
        log("Generando resumen de tachadas con features v3...", "INFO", "ML")
        resumen = resumir_tachadas_v3(df)
        log(f"Resumen generado: {resumen.shape[0]} tachadas", "INFO", "ML")
        
        if len(resumen) == 0:
            error_msg = "No se generaron tachadas en el resumen"
            log(error_msg, "ERROR", "ML")
            return {
                "success": False,
                "filas": 0,
                "nombre_output": "",
                "mensaje": error_msg
            }
        
        # 7) Preparar datos para predicción
        log("Preparando datos para predicción...", "INFO", "ML")
        resumen_pred = resumen.drop(columns=COLS_BORRAR, errors="ignore").copy()
        
        # Reordenar EXACTAMENTE en el orden del entrenamiento
        resumen_pred = resumen_pred[FEATURES_MODELO]
        
        # Asegurar columnas categóricas correctas
        for c in CAT_COLS:
            if c in resumen_pred.columns:
                resumen_pred[c] = resumen_pred[c].astype(str).fillna("DESCONOCIDO")
        
        log(f"Dataset final para predecir: {resumen_pred.shape}", "INFO", "ML")
        
        # 8) Cargar modelo y predecir
        log("Cargando modelo CatBoost...", "INFO", "ML")
        modelo = _load_model()
        
        log("Generando predicciones...", "INFO", "ML")
        y_prob = modelo.predict_proba(resumen_pred)[:, 1]
        resumen["probabilidad"] = y_prob
        resumen["prediccion"] = (y_prob >= UMBRAL).astype(int)
        
        log(f"Predicciones generadas. Defectuosas: {resumen['prediccion'].sum()}/{len(resumen)}", "INFO", "ML")
        
        # 9) Calcular hum_30fin_prom
        log("Calculando hum_30fin_prom...", "INFO", "ML")
        resumen = _calcular_hum_30fin_prom(resumen, df)
        
        # 10) Agregar HumedadInicial y HumedadFinal si están presentes
        log("Agregando HumedadInicial y HumedadFinal si están presentes...", "INFO", "ML")
        extra_cols = ["HumedadInicial", "HumedadFinal"]
        cols_presentes = [c for c in extra_cols if c in df_raw.columns]
        
        if cols_presentes:
            resumen = resumen.merge(
                df_raw[["ID_tachada"] + cols_presentes].drop_duplicates("ID_tachada"),
                on="ID_tachada",
                how="left"
            )
            log(f"Columnas agregadas: {cols_presentes}", "INFO", "ML")
            
            # Renombrar columnas para el output
            if "HumedadInicial" in resumen.columns:
                resumen = resumen.rename(columns={"HumedadInicial": "hum_ini_lab"})
            if "HumedadFinal" in resumen.columns:
                resumen = resumen.rename(columns={"HumedadFinal": "hum_fin_lab"})
        
        # 11) Generar nombre del archivo de salida
        nombre_output = f"predicciones_{archivo}"
        
        # 12) Convertir a CSV en memoria
        log("Generando CSV en memoria...", "INFO", "ML")
        csv_buffer = io.StringIO()
        resumen.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue().encode("utf-8")
        
        # 13) Subir a carpeta validated
        log(f"Subiendo CSV a carpeta validated: {nombre_output}", "INFO", "ML")
        gdrive_client.upload_file_to_folder(
            folder_id=validated_folder_id,
            file_name=nombre_output,
            content=csv_content,
            mime_type="text/csv"
        )
        
        log(f"✓ Pipeline ML completado exitosamente. {len(resumen)} tachadas procesadas", "INFO", "ML")
        
        return {
            "success": True,
            "filas": len(resumen),
            "nombre_output": nombre_output,
            "mensaje": f"Predicciones generadas exitosamente. {len(resumen)} tachadas procesadas, {resumen['prediccion'].sum()} defectuosas."
        }
        
    except Exception as e:
        log_error("ML", e, {"planta": planta, "archivo": archivo})
        return {
            "success": False,
            "filas": 0,
            "nombre_output": "",
            "mensaje": f"Error en pipeline ML: {str(e)}"
        }

