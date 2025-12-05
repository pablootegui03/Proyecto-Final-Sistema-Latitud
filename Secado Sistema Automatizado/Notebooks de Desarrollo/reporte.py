# Importaciones y datos
import io
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import base64
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import tempfile
import os

# === CONFIGURACIÓN DE PLANTA ===
# Cambiar este valor según la planta: "JPV" o "RB"
planta = "JPV"  # Cambiar a "RB" para la otra planta

# IDs de las carpetas "validated" en Google Drive (donde está df_historico.csv)
carpetas_validated = {
    "JPV": "1JbzvdmUiK_qAEHvfFK7g4dyVU2j7JwB9",
    "RB":  "11q2vW9Fk8qYz5MIcpmmxmNhc0PiWYlaY",
}

NOMBRE_HISTORICO = "df_historico.csv"

# IDs de las carpetas de Google Drive (donde se suben los reportes)
GOOGLE_DRIVE_FOLDER_IDS = {
    "JPV": "1CP6KsGkIHq5l0WrN7KMx-RK4ip_AXz4k",
    "RB": "181dqjsFvdu6pls_LLMcRD3J5PU-5eBR1"
}

# URLs de las carpetas compartidas (para referencia)
GOOGLE_DRIVE_URLS = {
    "JPV": "https://drive.google.com/drive/folders/1CP6KsGkIHq5l0WrN7KMx-RK4ip_AXz4k?usp=sharing",
    "RB": "https://drive.google.com/drive/folders/181dqjsFvdu6pls_LLMcRD3J5PU-5eBR1?usp=sharing"
}

# Alcance para la API de Drive (lectura/escritura)
SCOPES = ["https://www.googleapis.com/auth/drive"]

# ==========================
# AUTENTICACIÓN CON OAUTH (para descargar df_historico.csv)
# ==========================

def obtener_credenciales():
    """
    Usa OAuth (usuario final) en lugar de service account.
    - client_secrets.json: descargado de Google Cloud Console
      (OAuth Client ID, tipo "Desktop").
    - token.json: se genera solo la primera vez que se autoriza la app
      y se reutiliza después.
    """
    creds = None

    # Si ya existe token.json, lo usamos
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    # Si no hay credenciales válidas, pedimos login/refresco
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Renovar token
            creds.refresh(Request())
        else:
            # Primera vez: abrir navegador para login
            flow = InstalledAppFlow.from_client_secrets_file(
                "client_secrets.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Guardar token para próximas ejecuciones
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds

print("Iniciando autenticación OAuth para descargar datos históricos...")
credentials = obtener_credenciales()
service = build("drive", "v3", credentials=credentials)
print("Autenticación OAuth OK.")

# ==========================
# FUNCIONES AUXILIARES PARA DESCARGAR DESDE DRIVE
# ==========================

def buscar_archivo_por_nombre(service, folder_id, nombre_archivo):
    """Devuelve el ID del archivo si existe en la carpeta, si no None."""
    query = (
        f"'{folder_id}' in parents and "
        f"name = '{nombre_archivo}' and "
        f"trashed = false"
    )
    resp = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id, name)",
    ).execute()

    files = resp.get("files", [])
    if files:
        return files[0]["id"]
    return None


def descargar_csv_a_dataframe(service, file_id, file_name):
    """Descarga un CSV de Drive como DataFrame y agrega columna archivo_origen."""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    fh.seek(0)
    df = pd.read_csv(fh)
    df["archivo_origen"] = file_name
    return df

# Autenticación con Google Drive (pydrive para subir archivos)
gauth = GoogleAuth()
gauth.LocalWebserverAuth()  # Abre navegador para autenticación
drive = GoogleDrive(gauth)

# ID de la carpeta según la planta
folder_id = GOOGLE_DRIVE_FOLDER_IDS[planta]

# Carpeta temporal local para guardar archivos antes de subirlos
temp_dir = Path(tempfile.gettempdir()) / "reporte_temp"
temp_dir.mkdir(parents=True, exist_ok=True)
figs_dir_temp = temp_dir / "figs"
figs_dir_temp.mkdir(parents=True, exist_ok=True)

print(f"Planta configurada: {planta}")
print(f"ID de carpeta Google Drive: {folder_id}")
print(f"Carpeta compartida: {GOOGLE_DRIVE_URLS[planta]}")
print(f"Carpeta temporal local: {temp_dir}")

# === FUNCIONES HELPER PARA SUBIR ARCHIVOS A GOOGLE DRIVE ===

def subir_archivo_a_drive(ruta_local, nombre_archivo, folder_id_destino=None):
    """
    Sube un archivo a Google Drive en la carpeta especificada.
    
    Args:
        ruta_local: Path del archivo local a subir
        nombre_archivo: Nombre que tendrá el archivo en Google Drive
        folder_id_destino: ID de la carpeta destino (por defecto usa la de la planta configurada)
    
    Returns:
        ID del archivo subido en Google Drive
    """
    if folder_id_destino is None:
        folder_id_destino = folder_id
    
    # Verificar si el archivo ya existe en la carpeta
    file_list = drive.ListFile({'q': f"'{folder_id_destino}' in parents and trashed=false"}).GetList()
    for file in file_list:
        if file['title'] == nombre_archivo:
            # Si existe, eliminarlo primero
            file.Delete()
            break
    
    # Crear y subir el archivo
    file_drive = drive.CreateFile({
        'title': nombre_archivo,
        'parents': [{'id': folder_id_destino}]
    })
    file_drive.SetContentFile(str(ruta_local))
    file_drive.Upload()
    
    print(f"✓ Archivo subido: {nombre_archivo} -> {GOOGLE_DRIVE_URLS[planta]}")
    return file_drive['id']

def guardar_y_subir_grafico(fig, nombre_archivo, folder_id_destino=None):
    """
    Guarda un gráfico temporalmente y lo sube a Google Drive.
    
    Args:
        fig: Figura de matplotlib
        nombre_archivo: Nombre del archivo (ej: "b1_pie_global_last_vs_total.png")
        folder_id_destino: ID de la carpeta destino (opcional)
    """
    ruta_temp = figs_dir_temp / nombre_archivo
    fig.savefig(ruta_temp, bbox_inches="tight")
    subir_archivo_a_drive(ruta_temp, nombre_archivo, folder_id_destino)
    # Opcional: eliminar archivo temporal después de subir
    # ruta_temp.unlink()

def guardar_y_subir_html(contenido_html, nombre_archivo, folder_id_destino=None):
    """
    Guarda un archivo HTML temporalmente y lo sube a Google Drive.
    
    Args:
        contenido_html: Contenido HTML como string
        nombre_archivo: Nombre del archivo (ej: "reporte_tachadas_JPV.html")
        folder_id_destino: ID de la carpeta destino (opcional)
    """
    ruta_temp = temp_dir / nombre_archivo
    ruta_temp.write_text(contenido_html, encoding="utf-8")
    subir_archivo_a_drive(ruta_temp, nombre_archivo, folder_id_destino)
    # Opcional: eliminar archivo temporal después de subir
    # ruta_temp.unlink()

print("\n✓ Funciones helper para Google Drive configuradas")

# === DESCARGA DE DATOS HISTÓRICOS DESDE GOOGLE DRIVE ===

# Validar que la planta esté en carpetas_validated
if planta not in carpetas_validated:
    raise ValueError("Planta inválida. Debe ser 'JPV' o 'RB'.")

folder_id_validated = carpetas_validated[planta]
print(f"\nDescargando datos históricos para planta: {planta}")
print(f"Folder validated (Drive ID): {folder_id_validated}")

# Buscar el archivo df_historico.csv en la carpeta validated
historico_id = buscar_archivo_por_nombre(service, folder_id_validated, NOMBRE_HISTORICO)

if historico_id is None:
    raise ValueError(
        f"No se encontró el archivo '{NOMBRE_HISTORICO}' en la carpeta 'validated' "
        f"de la planta '{planta}'. Asegúrate de que el archivo existe en Google Drive."
    )

# Descargar el archivo histórico como DataFrame
print(f"Descargando {NOMBRE_HISTORICO}...")
df = descargar_csv_a_dataframe(service, historico_id, NOMBRE_HISTORICO)
print(f"✓ Datos históricos descargados: {len(df)} filas")

# Verificar si el DataFrame tiene la columna fecha_fin
# Si no la tiene, la generamos (compatibilidad con datos antiguos)
if "fecha_fin" not in df.columns:
    print("⚠ Advertencia: El DataFrame no tiene columna 'fecha_fin'. Generándola...")
    hoy = pd.to_datetime(datetime.now())  # o pd.Timestamp("2025-11-23") para pruebas
    df = df.copy()
    df["fecha_fin"] = hoy - pd.to_timedelta(range(len(df))[::-1], unit="h")
    print("✓ Columna 'fecha_fin' generada")
else:
    # Asegurar que fecha_fin sea datetime
    df["fecha_fin"] = pd.to_datetime(df["fecha_fin"])

df.head()

# HELPER: definimos "última semana"
# En producción, el ref_date puede ser: today() (si se corre una vez al día), o fecha pasada por parámetro desde el trigger de Azure.

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

hoy = df["fecha_fin"].max()  # o pd.Timestamp("2025-11-23"), lo que quieras
df_last_week, df_total = split_periods(df, date_col="fecha_fin", ref_date=hoy)

# BLOQUE 1 - CANTIDAD DE TACHADAS

# Función de resumen de cantidad

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


# Aplicar a "última semana" y "total"

# --- Global ---
resumen_cant_global_last = resumen_cantidad_tachadas(
    df_last_week,
    col_problema="prediccion",   # en producción, la columna del modelo
    group_cols=None              # global
)

resumen_cant_global_total = resumen_cantidad_tachadas(
    df_total,
    col_problema="prediccion",
    group_cols=None
)

print(resumen_cant_global_last)
print(resumen_cant_global_total)

# --- Por secadora ---
resumen_cant_secadora_last = resumen_cantidad_tachadas(
    df_last_week,
    col_problema="prediccion",
    group_cols=["sensor_id"]
)

resumen_cant_secadora_total = resumen_cantidad_tachadas(
    df_total,
    col_problema="prediccion",
    group_cols=["sensor_id"]
)

print(resumen_cant_secadora_last)
print(resumen_cant_secadora_total)

# BLOQUE 2 - TEMPERATURA

# Función: resumen de temperaturas y humedad al máximo

def resumen_temp_y_hum(
    df,
    group_cols,
    temp_col="temp_max",
    hum_tempmax_col="humedad_en_temp_max",
    hum_proxy_col="humedad_mean",
):
    """
    Devuelve, por cada grupo:
      - temp_max_grados: máximo de temp_max (°C) en las tachadas del grupo
      - hum_al_temp_max: promedio de humedad en el momento de la temp máxima
        (en producción: columna 'humedad_en_temp_max';
         si no existe, usamos 'humedad_mean' como proxy para poder probar).
    """
    df = df.copy()

    # Elegimos la columna de humedad a usar
    if hum_tempmax_col in df.columns:
        hum_col = hum_tempmax_col
    else:
        hum_col = hum_proxy_col  # PROXY para pruebas
        # Si querés, podés dejar este print para recordatorio:
        print(f"⚠ Usando '{hum_proxy_col}' como proxy de humedad en temp máxima.")

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

# Maxima por turno (global y por secadora)

# Máxima por turno (momento_dia) - última semana, global
temp_turno_last = resumen_temp_y_hum(
    df_last_week,
    group_cols=["momento_dia"]   # turno
)

temp_turno_last

# Máxima por turno (momento_dia) - total histórico, global
temp_turno_total = resumen_temp_y_hum(
    df_total,
    group_cols=["momento_dia"]
)

temp_turno_total

# Máxima por turno y secadora - última semana
temp_turno_secadora_last = resumen_temp_y_hum(
    df_last_week,
    group_cols=["sensor_id", "momento_dia"]   # secadora = sensor_id en tu caso
)

temp_turno_secadora_last

# Máxima por turno y secadora - total histórico
temp_turno_secadora_total = resumen_temp_y_hum(
    df_total,
    group_cols=["sensor_id", "momento_dia"]
)

temp_turno_secadora_total


# Maxima por variedad (global y por secadora)

# Máxima por variedad - última semana, global
temp_variedad_last = resumen_temp_y_hum(
    df_last_week,
    group_cols=["variedad"]
)

temp_variedad_last


# Máxima por variedad - total histórico, global
temp_variedad_total = resumen_temp_y_hum(
    df_total,
    group_cols=["variedad"]
)

temp_variedad_total


# Máxima por variedad y secadora - última semana
temp_variedad_secadora_last = resumen_temp_y_hum(
    df_last_week,
    group_cols=["sensor_id", "variedad"]
)

temp_variedad_secadora_last


# Máxima por variedad y secadora - total histórico
temp_variedad_secadora_total = resumen_temp_y_hum(
    df_total,
    group_cols=["sensor_id", "variedad"]
)

temp_variedad_secadora_total

# BLOQUE 3 - DURACION

# Función: resumen de duración

def resumen_duracion(
    df,
    col_dur="duracion_horas",
    group_cols=None
):
    """
    Resumen de duración de tachadas.

    Si group_cols es None o [] -> resumen global (1 fila).
    Si group_cols es lista -> resumen por grupo.

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
            # Evitar crashear si no hay datos
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

# Aplicar a "última semana" y "total"

# Global - última semana
duracion_global_last = resumen_duracion(
    df_last_week,
    col_dur="duracion_horas",
    group_cols=None
)

# Global - total histórico
duracion_global_total = resumen_duracion(
    df_total,
    col_dur="duracion_horas",
    group_cols=None
)

print(duracion_global_last)
print(duracion_global_total)


# Por secadora - última semana
duracion_secadora_last = resumen_duracion(
    df_last_week,
    col_dur="duracion_horas",
    group_cols=["sensor_id"]   # o ["sensor_id"] si usás eso directo
)

# Por secadora - total histórico
duracion_secadora_total = resumen_duracion(
    df_total,
    col_dur="duracion_horas",
    group_cols=["sensor_id"]
)

print(duracion_secadora_last)
print(duracion_secadora_total)

# BLOQUE 4 - COMPARACIONES CON DATOS DE LABORATORIO

# Función: resumen de comparación con datos de laboratorio
def resumen_laboratorio(
    df,
    hum_ini_col="hum_lab_ini",
    hum_fin_col="hum_lab_fin",
    hum_30fin_col="humedad_mean_30fin",
    diff_col="diff_hum_lab_vs_30fin",
    group_cols=None
):
    """
    Devuelve, por grupo:
      - humedad_inicial_lab_prom
      - humedad_final_lab_prom
      - humedad_30fin_prom
      - diferencia_lab_30fin_prom

    Si alguna columna no existe → devuelve None.
    """

    df = df.copy()

    columnas_existentes = df.columns

    def col_or_none(col):
        return col if col in columnas_existentes else None

    hum_ini = col_or_none(hum_ini_col)
    hum_fin = col_or_none(hum_fin_col)
    hum_30fin = col_or_none(hum_30fin_col)
    diff = col_or_none(diff_col)

    # --- Si group_cols es None → resumen global ---
    if not group_cols:

        resumen = {
            "ambito": ["global"],
            "hum_ini_lab_prom": [df[hum_ini].mean() if hum_ini else None],
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
        "hum_ini_lab_prom": (hum_ini, agg_mean(hum_ini)),
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


# Aplicar a "última semana" y "total"

# Global - ultima semana
lab_global_last = resumen_laboratorio(
    df_last_week,
    group_cols=None
)

lab_global_last

# Global - total histórico
lab_global_total = resumen_laboratorio(
    df_total,
    group_cols=None
)

lab_global_total

# Por secadora - ultima semana
lab_secadora_last = resumen_laboratorio(
    df_last_week,
    group_cols=["sensor_id"]
)

lab_secadora_last


# Por secadora - total histórico
lab_secadora_total = resumen_laboratorio(
    df_total,
    group_cols=["sensor_id"]
)

lab_secadora_total

# GRAFICOS

# Aseguramos tipo datetime
df["fecha_fin"] = pd.to_datetime(df["fecha_fin"])

# Columna de semana (lunes como inicio, por ejemplo)
df["semana"] = df["fecha_fin"].dt.to_period("W").apply(lambda r: r.start_time)

df.head()[["fecha_fin", "semana"]]

# Guardado de graficos:
# Usar carpeta temporal para guardar gráficos antes de subirlos a Google Drive
# Los gráficos se guardan temporalmente y luego se suben automáticamente
figs_dir = figs_dir_temp  # Ya definido en la celda de configuración
COLOR_PROBLEMA = "#d62728"  # rojo sobrio
COLOR_OK = "#1f77b4"        # azul para "total" o "sin problema"

# Función helper para guardar gráfico y subirlo a Google Drive
def savefig_y_subir(nombre_archivo):
    """
    Guarda el gráfico actual de matplotlib y lo sube a Google Drive.
    Reemplaza plt.savefig() seguido de subir_archivo_a_drive()
    
    Uso: savefig_y_subir("b1_pie_global_last_vs_total.png")
    """
    ruta_temp = figs_dir / nombre_archivo
    plt.savefig(ruta_temp, bbox_inches="tight")
    subir_archivo_a_drive(ruta_temp, nombre_archivo)

## BLOQUE 1 - CANTIDAD DE TACHADAS

### 1.1. Gráficos resumen global

# Extraer números
row_last = resumen_cant_global_last.iloc[0]
row_total = resumen_cant_global_total.iloc[0]

n_tach_last = int(row_last["n_tachadas"])
n_prob_last = int(row_last["n_problema"])
n_ok_last = n_tach_last - n_prob_last

n_tach_tot = int(row_total["n_tachadas"])
n_prob_tot = int(row_total["n_problema"])
n_ok_tot = n_tach_tot - n_prob_tot

labels = ["Con problema", "Sin problema"]
colors = [COLOR_PROBLEMA, "#D9D9D9"]   # rojo + gris claro

fig, axes = plt.subplots(1, 2, figsize=(8, 4))

# Última semana
axes[0].pie(
    [n_prob_last, n_ok_last],
    labels=labels,
    autopct="%1.1f%%",
    startangle=90,
    colors=colors,
    textprops={"fontsize": 10}
)
axes[0].set_title("Última semana")

# Total histórico
axes[1].pie(
    [n_prob_tot, n_ok_tot],
    labels=labels,
    autopct="%1.1f%%",
    startangle=90,
    colors=colors,
    textprops={"fontsize": 10}
)
axes[1].set_title("Total histórico")

plt.tight_layout()
savefig_y_subir("b1_pie_global_last_vs_total.png")

### 1.2. Gráfico histórico global: tachadas totales y con problemas por semana

# Agregamos por semana
cant_semana = (
    df
    .assign(es_problema=df["prediccion"].astype(int))
    .groupby("semana", as_index=False)
    .agg(
        n_tachadas=("es_problema", "count"),
        n_problema=("es_problema", "sum"),
    )
)

cant_semana["pct_problema"] = cant_semana["n_problema"] / cant_semana["n_tachadas"] * 100

# --- Gráfico 1: líneas de n_tachadas y n_problema ---
plt.figure(figsize=(10, 5))
plt.plot(cant_semana["semana"], cant_semana["n_tachadas"], label="N° tachadas", color= COLOR_OK)
plt.plot(cant_semana["semana"], cant_semana["n_problema"], label="N° tachadas con problema", color= COLOR_PROBLEMA)
plt.xticks(rotation=45)
plt.ylabel("Cantidad")
plt.title("Evolución semanal de tachadas y tachadas con problema")
plt.legend()
plt.tight_layout()
savefig_y_subir("b1_tachadas_vs_problemas_semanal.png")

# --- Gráfico 2: línea de % con problemas ---
plt.figure(figsize=(10, 4))
plt.plot(cant_semana["semana"], cant_semana["pct_problema"], color= COLOR_PROBLEMA)
plt.xticks(rotation=45)
plt.ylabel("% con problemas")
plt.title("Evolución semanal del % de tachadas con problema")
plt.tight_layout()
savefig_y_subir("b1_pct_problemas_semanal.png")

### 1.3. Gráfico "foto" de la última semana: barras por secadora

df_last_week = df[df["fecha_fin"] >= df["fecha_fin"].max() - pd.Timedelta(days=7)]

resumen_last_sec = (
    df_last_week
    .assign(es_problema=df_last_week["prediccion"].astype(int))
    .groupby("sensor_id", as_index=False)
    .agg(
        n_tachadas=("es_problema", "count"),
        n_problema=("es_problema", "sum"),
    )
)

resumen_last_sec["pct_problema"] = resumen_last_sec["n_problema"] / resumen_last_sec["n_tachadas"] * 100

plt.figure(figsize=(8, 4))
plt.bar(resumen_last_sec["sensor_id"], resumen_last_sec["pct_problema"], color= COLOR_PROBLEMA)
plt.ylabel("% con problemas")
plt.title("Última semana: % de tachadas con problema por secadora")
plt.tight_layout()
savefig_y_subir("b1_pct_problemas_ultima_semana_por_secadora.png")

## BLOQUE 2 - TEMPERATURAS

### 2.1. Boxplot de temp_max por secadora (total histórico)

plt.rcParams["figure.dpi"] = 120  # prueba valores tipo 120, 150, 200
plt.figure(figsize=(18, 10))  # más ancho y más alto
df.boxplot(column="temp_max", by="sensor_id")

plt.title("Distribución de temperatura máxima por secadora", fontsize=16)
plt.suptitle("")  # quita el título automático de pandas
plt.ylabel("Temp. máxima (°C)", fontsize=14)
plt.xlabel("Secadora", fontsize=14)

plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

plt.tight_layout()
savefig_y_subir("b2_boxplot_temp_max_por_secadora.png")

### 2.2. Boxplot de temp_max por turno (total histórico)

plt.rcParams["figure.dpi"] = 120  # prueba valores tipo 120, 150, 200
plt.figure(figsize=(18, 10))  # más ancho y más alto
df.boxplot(column="temp_max", by="momento_dia")

plt.title("Distribución de temperatura máxima por turno", fontsize=16)
plt.suptitle("")  # quita el título automático de pandas
plt.ylabel("Temp. máxima (°C)", fontsize=14)
plt.xlabel("Turno", fontsize=14)

plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

plt.tight_layout()
savefig_y_subir("b2_boxplot_temp_max_por_turno.png")

### 2.3. Boxplot de temp_max por variedad (total histórico)

plt.rcParams["figure.dpi"] = 100  # prueba valores tipo 120, 150, 200
plt.figure(figsize=(40, 10))  # más ancho y más alto
df.boxplot(column="temp_max", by="variedad")

plt.title("Distribución de temperatura máxima por variedad", fontsize=16)
plt.suptitle("")  # quita el título automático de pandas
plt.ylabel("Temp. máxima (°C)", fontsize=12)
plt.xlabel("Variedad", fontsize=12)

plt.xticks(rotation=45, ha='right', fontsize=12)  # rotar etiquetas 45 grados
plt.yticks(fontsize=12)

plt.tight_layout()
savefig_y_subir("b2_boxplot_temp_max_por_variedad.png")

### 2.4. Relacion temperatura maxima - humedad al maximo

col_hum = "humedad_en_temp_max" if "humedad_en_temp_max" in df.columns else "humedad_mean"

plt.figure(figsize=(6, 5))
plt.scatter(df["temp_max"], df[col_hum], alpha=0.5)
plt.xlabel("Temp. máxima (°C)")
plt.ylabel("Humedad al momento de la temp. máxima (%)")
plt.title("Relación entre temp. máxima y humedad en el máximo")
plt.tight_layout()
savefig_y_subir("b2_scatter_temp_max_vs_humedad.png")

## BLOQUE 3 - DURACION

### 3.1. Duración global de tachadas

umbral_extremo = 30  # ajustar a gusto

df_normal_y_exceso = df[df["duracion_horas"] <= umbral_extremo]

plt.figure(figsize=(10, 4))
plt.boxplot(df_normal_y_exceso["duracion_horas"], vert=False, showfliers=True)
plt.xlabel("Duración (hs)")
plt.title(f"Distribución de duración (≤ {umbral_extremo} hs)")
plt.tight_layout()
savefig_y_subir("b3_boxplot_duracion_global_sin_ultras.png")

# ultra outliers

ultra = df[df["duracion_horas"] > umbral_extremo]

ultra.sort_values("duracion_horas", ascending=False)[
    ["sensor_id", "fecha_fin", "duracion_horas", "variedad"] #agregar ID_tachada, fecha_inicio cuando las tenga
].head(20)

plt.figure(figsize=(10, 4))
plt.scatter(ultra["fecha_fin"], ultra["duracion_horas"], color = COLOR_PROBLEMA)
plt.xlabel("Fecha")
plt.ylabel("Duración (hs)")
plt.title(f"Tachadas extremadamente largas (> {umbral_extremo} hs)")
plt.tight_layout()
savefig_y_subir("b3_scatter_ultra_outliers_duracion.png")

print("Top 10 tachadas por duración:")
top_largas = (
    df[["duracion_horas", "sensor_id", "fecha_fin", "variedad"]]
    .sort_values("duracion_horas", ascending=False)
    .head(10)
)
top_largas

### 3.2. Boxplot de duración por secadora

umbral_extremo = 30
df_focal = df[df["duracion_horas"] <= umbral_extremo]

plt.figure(figsize=(10,5))
df_focal.boxplot(column="duracion_horas", by="sensor_id", vert=False)
plt.title(f"Duración por secadora (≤ {umbral_extremo} hs)")
plt.suptitle("")
plt.xlabel("Duración (hs)")
plt.tight_layout()
savefig_y_subir("b3_boxplot_duracion_por_secadora.png")

df_ultra = df[df["duracion_horas"] > umbral_extremo]
df_ultra[["sensor_id","duracion_horas","fecha_fin"]]

### 3.3. Evolución semanal de la duración media

df["fecha_fin"] = pd.to_datetime(df["fecha_fin"])
df["semana"] = df["fecha_fin"].dt.to_period("W-SUN").apply(lambda r: r.start_time) #anclado a domingo


# Agregar por semana: media y mediana de duración
dur_semana = (
    df
    .groupby("semana", as_index=False)
    .agg(
        duracion_media=("duracion_horas", "mean"),
        duracion_mediana=("duracion_horas", "median"),
    )
)

plt.figure(figsize=(10, 5))

plt.plot(
    dur_semana["semana"],
    dur_semana["duracion_media"],
    label="Media semanal"
)

plt.plot(
    dur_semana["semana"],
    dur_semana["duracion_mediana"],
    label="Mediana semanal"
)

plt.xticks(dur_semana["semana"], rotation=45)
plt.ylabel("Duración (hs)")
plt.xlabel("Semana")
plt.title("Evolución semanal de la duración de las tachadas")
plt.legend()
plt.tight_layout()
savefig_y_subir("b3_duracion_media_y_mediana_semanal.png")

print(f"Ejemplo explicativo: La duración promedio de la semana que empieza el 6/10 es ≈ {dur_semana.loc[dur_semana['semana'] == pd.Timestamp('2025-10-06')]['duracion_media'].values[0]:.2f} horas")

dur_semana = (
    df
    .groupby("semana", as_index=False)
    .agg(
        duracion_media=("duracion_horas", "mean"),
        duracion_mediana=("duracion_horas", "median"),
    )
)

dur_semana

## BLOQUE 4 - LABORATORIO

### 4.1. Scatter hum. final lab vs hum. 30 min finales

if {"hum_lab_fin", "humedad_mean_30fin"}.issubset(df.columns):
    plt.figure(figsize=(6, 6))
    plt.scatter(df["humedad_mean_30fin"], df["hum_lab_fin"], alpha=0.5)
    plt.xlabel("Humedad sensor últimos 30 min (%)")
    plt.ylabel("Humedad final laboratorio (%)")
    plt.title("Sensor vs laboratorio (humedad final)")
    plt.tight_layout()
    savefig_y_subir("b4_scatter_humedad_lab_vs_30min.png")

### 4.2. Evolución de la diferencia promedio (lab – 30 min) por semana

if "diff_hum_lab_vs_30fin" in df.columns:
    diff_semana = (
        df
        .groupby("semana", as_index=False)
        .agg(diff_prom=("diff_hum_lab_vs_30fin", "mean"))
    )

    plt.figure(figsize=(10, 4))
    plt.plot(diff_semana["semana"], diff_semana["diff_prom"])
    plt.xticks(rotation=45)
    plt.axhline(0, linestyle="--")
    plt.ylabel("Diferencia promedio (lab - 30min)")
    plt.title("Evolución semanal de la diferencia lab vs sensor")
    plt.tight_layout()
    savefig_y_subir("b4_diferencia_lab_vs_30min_semanal.png")

## 5. KPIs rapidos

kpi_tachadas_last = int(resumen_cant_global_last["n_tachadas"].iloc[0])
kpi_pct_prob_last = float(resumen_cant_global_last["pct_problema"].iloc[0])

kpi_dur_media_last = float(duracion_global_last["duracion_mean"].iloc[0])
kpi_temp_max_prom_last = float(temp_turno_last["temp_max_grados"].max())  # ej: máx por turno

# REPORTE
# Se usan las funciones de Google Drive.El HTML se subirá directamente a Google Drive con el nombre de la planta
nombre_html = f"reporte_tachadas_{planta}.html"

# ---- Helper genérico para renombrar + formatear tablas ----


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

# ---- Helper incrustar imágenes ----


def img_inline(path, alt=""):
    """
    Lee la imagen en 'path' y devuelve un tag <img> con el contenido embebido en base64.
    Si el archivo no existe, devuelve un pequeño placeholder HTML en vez de romper.
    """
    path = Path(path)

    if not path.exists():
        # Placeholder cuando todavía no hay imagen (p. ej. no hay datos de laboratorio)
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



# ==== Bloque 1: tablas formateadas ====

# Mapeo común para columnas de duración
RENAME_DURACION_COMUN = {
    "duracion_mean":   "Media (hs)",
    "duracion_media": "Media (hs)",
    "duracion_mediana": "Mediana (hs)",
    "duracion_median": "Mediana (hs)",
    "duracion_min":    "Mínimo (hs)",
    "duracion_max":    "Máximo (hs)",
    "duracion_p25":    "P25 (hs)",
    "duracion_p75":    "P75 (hs)",
}

# Columnas de duración que van con 1 decimal (por ejemplo)
DEC1_DURACION_COLS = [
    "Media (hs)", "Mediana (hs)", "Mínimo (hs)",
    "Máximo (hs)", "P25 (hs)", "P75 (hs)"
]


tabla_cant_global_last = preparar_tabla(
    resumen_cant_global_last,
    rename={
        "ambito": "Ámbito",
        "n_tachadas": "Tachadas",
        "n_problema": "Con problema",
        "pct_problema": "% con problema",
    },
    int_cols=["Tachadas", "Con problema"],
    pct_cols=["% con problema"],
)

tabla_cant_global_total = preparar_tabla(
    resumen_cant_global_total,
    rename={
        "ambito": "Ámbito",
        "n_tachadas": "Tachadas",
        "n_problema": "Con problema",
        "pct_problema": "% con problema",
    },
    int_cols=["Tachadas", "Con problema"],
    pct_cols=["% con problema"],
)

tabla_cant_secadora_last = preparar_tabla(
    resumen_cant_secadora_last,
    rename={
        "sensor_id": "Secadora",
        "n_tachadas": "Tachadas",
        "n_problema": "Con problema",
        "pct_problema": "% con problema",
    },
    int_cols=["Tachadas", "Con problema"],
    pct_cols=["% con problema"],
)

tabla_cant_secadora_total = preparar_tabla(
    resumen_cant_secadora_total,
    rename={
        "sensor_id": "Secadora",
        "n_tachadas": "Tachadas",
        "n_problema": "Con problema",
        "pct_problema": "% con problema",
    },
    int_cols=["Tachadas", "Con problema"],
    pct_cols=["% con problema"],
)

tabla_temp_turno_last = preparar_tabla(
    temp_turno_last,
    rename={
        "momento_dia": "Turno",
        "temp_max_grados": "Temp. máx. (°C)",
        "hum_al_temp_max": "Humedad al máximo (%)",
    },
    dec1_cols=["Temp. máx. (°C)"],
    dec2_cols=["Humedad al máximo (%)"],
)

tabla_temp_variedad_last = preparar_tabla(
    temp_variedad_last,
    rename={
        "variedad": "Variedad",
        "temp_max_grados": "Temp. máx. (°C)",
        "hum_al_temp_max": "Humedad al máximo (%)",
    },
    dec1_cols=["Temp. máx. (°C)"],
    dec2_cols=["Humedad al máximo (%)"],
)

tabla_duracion_global_last = preparar_tabla(
    duracion_global_last,
    rename={
        "ambito": "Ámbito",
        "n_tachadas": "Tachadas",
        **RENAME_DURACION_COMUN,
    },
    int_cols=["Tachadas"],
    dec1_cols= DEC1_DURACION_COLS,
)

tabla_duracion_global_total = preparar_tabla(
    duracion_global_total,
    rename={
        "ambito": "Ámbito",
        "n_tachadas": "Tachadas",
        **RENAME_DURACION_COMUN,
    },
    int_cols=["Tachadas"],
    dec1_cols= DEC1_DURACION_COLS,
)

tabla_top_largas = preparar_tabla(
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

tabla_dur_semana = preparar_tabla(
    dur_semana,
    rename={
        "semana": "Semana",
        **RENAME_DURACION_COMUN,
    },
    dec1_cols=DEC1_DURACION_COLS,
)

tabla_duracion_secadora_last = preparar_tabla(
    duracion_secadora_last,
    rename={
        "secadora": "Secadora",
        "n_tachadas": "Tachadas",
        **RENAME_DURACION_COMUN,
    },
    int_cols=["Tachadas"],
    dec1_cols=DEC1_DURACION_COLS,
)

tabla_ultra = preparar_tabla(
    df_ultra[["sensor_id", "duracion_horas", "fecha_fin"]],
    rename={
        "sensor_id": "Secadora",
        "duracion_horas": "Duración (hs)",
        "fecha_fin": "Fin de secado",
    },
    dec1_cols=["Duración (hs)"],
)

# --- COMENTARIOS AUTOMÁTICOS ---

# --- Comentario Bloque 1 ---

pct_last = float(resumen_cant_global_last["pct_problema"].iloc[0])
pct_hist = float(resumen_cant_global_total["pct_problema"].iloc[0])

if pct_last < pct_hist:
    tendencia_b1 = "por debajo del promedio histórico"
elif pct_last > pct_hist:
    tendencia_b1 = "por encima del promedio histórico"
else:
    tendencia_b1 = "en línea con el promedio histórico"

comentario_b1 = (
    f"En la última semana, el <strong>{pct_last:.1f}%</strong> de las tachadas presentó problemas, "
    f"mientras que el promedio histórico es de <strong>{pct_hist:.1f}%</strong>. "
    f"Esto indica que la semana se encuentra <strong>{tendencia_b1}</strong> en términos de calidad del secado."
)

# --- Comentario Bloque 2 ---

# Detectamos qué columna usar para temperatura máxima
if "temp_max_grados" in df_last_week.columns:
    col_temp_max = "temp_max_grados"
elif "temp_max" in df_last_week.columns:
    col_temp_max = "temp_max"
else:
    col_temp_max = None

if col_temp_max is not None and not df_last_week.empty:
    # Fila con la temperatura máxima de la última semana
    fila_max = df_last_week.loc[df_last_week[col_temp_max].idxmax()]

    temp_max_semana = float(fila_max[col_temp_max])
    turno_max = str(fila_max.get("momento_dia", "N/D"))
    variedad_max = str(fila_max.get("variedad", "N/D"))

    comentario_b2 = (
    f"En la última semana, la temperatura máxima registrada fue de <strong>{temp_max_semana:.1f} °C</strong>, "
    f"alcanzada en el turno <strong>{turno_max}</strong> para la variedad <strong>{variedad_max}</strong>. "
    )
else:
    comentario_b2 = (
        "En este período no se dispone de datos de temperatura máxima suficientes como para construir "
        "un resumen interpretativo por turno y variedad."
    )



# --- Comentario Bloque 3 ---

dur_med_last = float(duracion_global_last["duracion_mean"].iloc[0])
dur_med_hist = float(duracion_global_total["duracion_mean"].iloc[0])
n_ultra = len(df_ultra)

if dur_med_last < dur_med_hist:
    tendencia_b3 = "ligeramente por debajo del histórico"
elif dur_med_last > dur_med_hist:
    tendencia_b3 = "por encima del histórico"
else:
    tendencia_b3 = "muy alineada con el histórico"

comentario_b3 = (
    f"La duración promedio de las tachadas en la última semana fue de <strong>{dur_med_last:.1f} horas</strong>, "
    f"frente a un promedio histórico de <strong>{dur_med_hist:.1f} horas</strong>, lo que sugiere que la duración típica "
    f"de las tachadas está <strong>{tendencia_b3}</strong>. "
    f"Además, se identificaron <strong>{n_ultra}</strong> tachadas extremadamente largas (ultra-outliers)."
)

# --- Comentario Bloque 4 ---

# Primero verificamos que existan las columnas necesarias
cols_needed = ["hum_fin_lab_prom", "diff_lab_30fin_prom"]
if all(col in lab_global_last.columns for col in cols_needed):

    hum_fin_last = lab_global_last["hum_fin_lab_prom"].iloc[0]
    hum_fin_hist = lab_global_total["hum_fin_lab_prom"].iloc[0]
    diff_last = lab_global_last["diff_lab_30fin_prom"].iloc[0]

    # Si alguno es None o NaN → comentario alternativo
    if pd.isna(hum_fin_last) or pd.isna(hum_fin_hist) or pd.isna(diff_last):
        comentario_b4 = (
            "Si bien se dispone de la estructura de los datos de laboratorio, algunos valores de "
            "humedad final o diferencias con los datos del sensor no están disponibles para el periodo "
            "analizado. Por lo tanto, la comparación con laboratorio es parcial o no concluyente."
        )
    else:
        comentario_b4 = (
          f"La humedad final promedio de laboratorio en la última semana fue de <strong>{hum_fin_last:.1f}%</strong>, "
          f"mientras que el promedio histórico es de <strong>{hum_fin_hist:.1f}%</strong>. "
          f"La diferencia media entre la medición del laboratorio y la estimación del sensor en los últimos 30 minutos "
          f"fue de <strong>{diff_last:.2f} puntos porcentuales</strong>. "
          "Esto permite evaluar la alineación entre sensor y laboratorio y detectar posibles descalibraciones."
        )

else:
    comentario_b4 = (
        "En este período <strong>no se dispone de datos de laboratorio suficientes</strong>, "
        "por lo que la comparación sensor–laboratorio es parcial o no concluyente."
    )


html = []

html.append("""
<html>
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

  /* ---- TABLAS ---- */
  table { 
    border-collapse: collapse; 
    margin: 8px 0 16px; 
    font-size: 13px;              /* un poquito más grande */
    width: auto;                  /* que no se estiren a lo ancho siempre */
    max-width: 100%;
  }
  th, td { 
    border: 1px solid #ddd; 
    padding: 6px 8px; 
  }
  th { 
    background-color: #f3f4f6; 
    font-weight: 600;
    text-align: left;             /* encabezados alineados a la izquierda */
  }
  td {
    text-align: right;            /* números a la derecha en general */
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

  /* ---- layout 2 columnas ---- */
  .two-col {
  display: flex;
  gap: 16px;
  align-items: flex-start;
}

.two-col .col {
  flex: 1;              /* ocupa mitad y mitad */
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
<body>
""")

# ---- Encabezado con logo y fecha ----
fecha_rep = datetime.now().strftime("%d/%m/%Y %H:%M")

# Buscar el logo en el directorio actual o en figs_dir
logo_path = None
logo_nombre = "logo_latitud_2.png"
# Intentar primero en el directorio actual
if Path(logo_nombre).exists():
    logo_path = Path(logo_nombre)
# Si no está, intentar en figs_dir
elif (figs_dir / logo_nombre).exists():
    logo_path = figs_dir / logo_nombre
# Si no está, intentar en el directorio del script
else:
    script_dir = Path(__file__).parent
    if (script_dir / logo_nombre).exists():
        logo_path = script_dir / logo_nombre
    elif (script_dir / "figs" / logo_nombre).exists():
        logo_path = script_dir / "figs" / logo_nombre

# Generar el HTML del logo usando img_inline
if logo_path:
    logo_html = img_inline(logo_path, alt="Logo Latitud")
    # Agregar estilo inline al logo (insertar después de <img)
    if logo_html.startswith('<img'):
        # Insertar el estilo después de <img y antes del primer espacio o >
        if ' style=' in logo_html:
            # Si ya tiene estilo, reemplazarlo
            import re
            logo_html = re.sub(r' style="[^"]*"', ' style="max-height:60px; width:auto;"', logo_html)
        else:
            # Si no tiene estilo, agregarlo después de <img
            logo_html = logo_html.replace('<img', '<img style="max-height:60px; width:auto;"', 1)
else:
    # Si no se encuentra el logo, usar un placeholder
    logo_html = '<div style="max-height:60px; padding:10px; background:#f0f0f0; border-radius:4px; font-size:12px; color:#666;">Logo Latitud</div>'
    print("⚠ Advertencia: No se encontró el logo 'logo_latitud_2.png'. Se usará un placeholder.")

html.append(f"""
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
  
  <div>
    <h1 style="margin:0; padding:0;">Reporte de Tachadas de Secado</h1>
    <div style="font-size:13px; color:#555;">Generado el {fecha_rep}</div>
  </div>

  <div>
    {logo_html}
  </div>

</div>
""")


# ===== Resumen ejecutivo (kpis) =====

html.append("""
<div style="display:flex; flex-wrap:wrap; gap:16px; margin-bottom:30px;">
  <div style="flex:1; min-width:200px; padding:12px 16px; border-radius:8px; background:#f5f7fb;">
    <div style="font-size:12px; color:#555;">Tachadas (última semana)</div>
    <div style="font-size:22px; font-weight:bold; color:#222;">""" + str(kpi_tachadas_last) + """</div>
  </div>
  <div style="flex:1; min-width:200px; padding:12px 16px; border-radius:8px; background:#fff5f5;">
    <div style="font-size:12px; color:#555;">% con problemas (última semana)</div>
    <div style="font-size:22px; font-weight:bold; color:#b22222;">""" + f"{kpi_pct_prob_last:.1f}%" + """</div>
  </div>
  <div style="flex:1; min-width:200px; padding:12px 16px; border-radius:8px; background:#f5f7fb;">
    <div style="font-size:12px; color:#555;">Duración media (hs, última semana)</div>
    <div style="font-size:22px; font-weight:bold; color:#222;">""" + f"{kpi_dur_media_last:.1f}" + """</div>
  </div>
  <div style="flex:1; min-width:200px; padding:12px 16px; border-radius:8px; background:#f5f7fb;">
    <div style="font-size:12px; color:#555;">Temp. máxima máx. (°C, última semana)</div>
    <div style="font-size:22px; font-weight:bold; color:#222;">""" + f"{kpi_temp_max_prom_last:.1f}" + """</div>
  </div>
</div>
""")

# ===== RESUMEN INTERPRETATIVO =====

html.append('<div class="section">')
html.append("<h2>Resumen interpretativo de la semana</h2>")
html.append("<ul>")
html.append(f"<li><strong>Cantidad de tachadas:</strong> {comentario_b1}</li>")
html.append(f"<li><strong>Temperaturas:</strong> {comentario_b2}</li>")
html.append(f"<li><strong>Duración:</strong> {comentario_b3}</li>")
html.append(f"<li><strong>Comparación con laboratorio:</strong> {comentario_b4}</li>")
html.append("</ul>")
html.append("</div>")




# ===== ÍNDICE =====

html.append("""
<h2>Índice</h2>
<ul>
  <li><a href="#bloque1">1. Cantidad de tachadas</a></li>
  <li><a href="#bloque2">2. Temperaturas</a></li>
  <li><a href="#bloque3">3. Duración de las tachadas</a></li>
  <li><a href="#bloque4">4. Comparación con laboratorio</a></li>
</ul>
""")



# ===== Bloque 1 =====
html.append('<h2 id="bloque1">1. Cantidad de tachadas</h2>')

html.append('<div class="subsection two-col">')

# ---- Columna izquierda (1.1) ----
html.append('<div class="col">')
html.append("<h3>1.1 Resumen global</h3>")
html.append("<p>Última semana:</p>")
html.append(tabla_cant_global_last.to_html(index=False))
html.append("<p>Total histórico:</p>")
html.append(tabla_cant_global_total.to_html(index=False))

# tortas abajo de las tablas
html.append("<h4>Distribución de tachadas con problema</h4>")
html.append(
    img_inline(
        figs_dir / "b1_pie_global_last_vs_total.png",
        alt="Distribución de tachadas con problema"
    )
)

html.append("</div>")


# ---- Columna derecha (1.2) ----
html.append('<div class="col">')
html.append("<h3>1.2 Por secadora</h3>")
html.append("<p>Última semana:</p>")
html.append(tabla_cant_secadora_last.to_html(index=False))
html.append("<p>Total histórico:</p>")
html.append(tabla_cant_secadora_total.to_html(index=False))
html.append("</div>")

html.append("</div>")


html.append('<div class="subsection"><h3>1.3 Gráficos históricos</h3>')
html.append("<p>Evolución semanal de tachadas y tachadas con problema:</p>")
html.append(
    img_inline(
        figs_dir / "b1_tachadas_vs_problemas_semanal.png",
        alt="Tachadas vs problemas por semana"
    )
)
html.append("<p>Evolución semanal del % de tachadas con problema:</p>")
html.append(
    img_inline(
        figs_dir / "b1_pct_problemas_semanal.png",
        alt="% problemas por semana"
    )
)
html.append("<p>Última semana: % de tachadas con problema por secadora:</p>")
html.append(
    img_inline(
        figs_dir / "b1_pct_problemas_ultima_semana_por_secadora.png",
        alt="% problemas última semana por secadora"
    )
)
html.append("</div>")



# ===== Bloque 2 =====
html.append('<h2 id="bloque2">2. Temperaturas</h2>')

html.append('<div class="subsection two-col">')

# ---- Columna izquierda (2.1) ----
html.append('<div class="col">')
html.append("<h3>2.1 Máxima por turno (última semana)</h3>")
html.append(tabla_temp_turno_last.to_html(index=False))
html.append("</div>")

# ---- Columna derecha (2.2) ----
html.append('<div class="col">')
html.append("<h3>2.2 Máxima por variedad (última semana)</h3>")
html.append(tabla_temp_variedad_last.to_html(index=False))
html.append("</div>")

html.append("</div>")


html.append('<div class="subsection"><h3>2.3 Distribución por secadora, turno y variedad</h3>')
html.append("<p>Temp. máxima por secadora:</p>")
html.append(
    img_inline(
        figs_dir / "b2_boxplot_temp_max_por_secadora.png",
        alt="Boxplot temp máxima por secadora"
    )
)
html.append("<p>Temp. máxima por turno:</p>")
html.append(
    img_inline(
        figs_dir / "b2_boxplot_temp_max_por_turno.png",
        alt="Boxplot temp máxima por turno"
    )
)
html.append("<p>Temp. máxima por variedad:</p>")
html.append(
    img_inline(
        figs_dir / "b2_boxplot_temp_max_por_variedad.png",
        alt="Boxplot temp máxima por variedad"
    )
)
html.append("</div>")

html.append('<div class="subsection"><h3>2.4 Relación temperatura y humedad</h3>')
html.append(
    img_inline(
        figs_dir / "b2_scatter_temp_max_vs_humedad.png",
        alt="Scatter temp máxima vs humedad al máximo"
    )
)
html.append("</div>")

# ===== Bloque 3 =====
html.append('<h2 id="bloque3">3. Duración de las tachadas</h2>')

html.append('<div class="subsection"><h3>3.1 Resumen global</h3>')
html.append("<p>Última semana:</p>")
html.append(tabla_duracion_global_last.to_html(index=False))
html.append("<p>Total histórico:</p>")
html.append(tabla_duracion_global_total.to_html(index=False))
html.append("</div>")

html.append('<div class="subsection"><h3>3.2 Distribución de la duración</h3>')
html.append("<p>Distribución global de duración (sin ultra-outliers):</p>")
html.append(
    img_inline(
        figs_dir / "b3_boxplot_duracion_global_sin_ultras.png",
        alt="Boxplot duración global sin ultra-outliers"
    )
)
html.append("<p>Tachadas extremadamente largas (ultra-outliers):</p>")
html.append(
    img_inline(
        figs_dir / "b3_scatter_ultra_outliers_duracion.png",
        alt="Scatter ultra-outliers de duración"
    )
)
html.append("<h4>Top 10 tachadas por duración</h4>")
html.append(tabla_top_largas.to_html(index=False))
html.append("</div>")


html.append('<div class="subsection"><h3>3.3 Duración por secadora (última semana)</h3>')
html.append(tabla_duracion_secadora_last.to_html(index=False))
html.append("<p>Distribución de duración por secadora:</p>")
html.append(
    img_inline(
        figs_dir / "b3_boxplot_duracion_por_secadora.png",
        alt="Boxplot duración por secadora"
    )
)
html.append("<h4>Tachadas extremadamente largas (> 30 hs.)</h4>")
html.append(tabla_ultra.to_html(index=False))
html.append("</div>")


html.append('<div class="subsection"><h3>3.4 Evolución histórica</h3>')
html.append(
    img_inline(
        figs_dir / "b3_duracion_media_y_mediana_semanal.png",
        alt="Duración media y mediana semanal"
    )
)
html.append("<h4>Ejemplo interpretativo</h4>")
html.append(
    f"<p>La duración promedio de la semana que empieza el 6/10 es ≈ "
    f"{dur_semana.loc[dur_semana['semana'] == pd.Timestamp('2025-10-06'), 'duracion_media'].values[0]:.2f} horas.</p>"
)

html.append("<h4>Tabla de duración por semana</h4>")
html.append(tabla_dur_semana.to_html(index=False))
html.append("</div>")


# ===== Bloque 4 =====
html.append('<h2 id="bloque4">4. Comparación con Laboratorio </h2>')

# 4.1 + 4.2 juntos como dos columnas
html.append('<div class="subsection two-col">')

html.append('<div class="col">')
html.append("<h3>4.1 Resumen global</h3>")
html.append("<p>Última semana:</p>")
html.append(lab_global_last.to_html(index=False))
html.append("<p>Total histórico:</p>")
html.append(lab_global_total.to_html(index=False))
html.append("</div>")  # fin col izquierda

html.append('<div class="col">')
html.append("<h3>4.2 Por secadora (última semana)</h3>")
html.append(lab_secadora_last.to_html(index=False))
html.append("</div>")  # fin col derecha

html.append("</div>")  # fin subsection two-col

html.append('<div class="subsection"><h3>4.3 Gráficos de comparación sensor vs laboratorio</h3>')
html.append("<p>Relación entre humedad final de laboratorio y del sensor (últimos 30 minutos):</p>")
html.append(
    img_inline(
        figs_dir / "b4_scatter_humedad_lab_vs_30min.png",
        alt="Scatter humedad lab vs 30 min sensor"
    )
)
html.append("<p>Evolución semanal de la diferencia promedio (lab - 30 min):</p>")
html.append(
    img_inline(
        figs_dir / "b4_diferencia_lab_vs_30min_semanal.png",
        alt="Diferencia lab vs 30 min semanal"
    )
)
html.append("</div>")

html.append("</body></html>")

# Subir el HTML a Google Drive
guardar_y_subir_html("".join(html), nombre_html)
print(f"✓ Reporte generado y subido a Google Drive: {GOOGLE_DRIVE_URLS[planta]}")

