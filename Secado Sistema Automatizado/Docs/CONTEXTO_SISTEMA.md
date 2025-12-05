# 📚 Documentación Completa del Sistema de Secado de Arroz

## 📋 Tabla de Contenidos

1. [Visión General](#visión-general)
2. [Arquitectura del Sistema](#arquitectura-del-sistema)
3. [Componentes Principales](#componentes-principales)
4. [Flujo de Datos](#flujo-de-datos)
5. [Sistema de Timestamps Incremental](#sistema-de-timestamps-incremental)
6. [Autenticación y Seguridad](#autenticación-y-seguridad)
7. [Configuración](#configuración)
8. [Procesamiento ETL](#procesamiento-etl)
9. [Integración con Google Drive](#integración-con-google-drive)
10. [Interfaz Google Apps Script](#interfaz-google-apps-script)
11. [Azure Functions](#azure-functions)
12. [Estructura de Datos](#estructura-de-datos)

---

## 🎯 Visión General

El **Sistema de Secado de Arroz** es una solución automatizada end-to-end para consolidar, procesar, validar y analizar datos de sensores de secado de arroz desde múltiples plantas (JPV y RB). El sistema está diseñado para:

- **Interfaz de Carga**: Google Apps Script para cargar archivos crudos de sensores a Google Drive
- **Procesar** archivos de sensores mediante ETL (Extract, Transform, Load)
- **Cruzar** datos de sensores con controles de laboratorio y aplicar calibración
- **Validar** datos mediante modelos de Machine Learning
- **Consolidar** archivos validados en un dataset histórico único (`df_historico.csv`)
- **Reportar** métricas y gráficos en formato HTML
- **Evitar reprocesamiento** mediante sistema de timestamps incremental
- **Escalar** automáticamente usando Azure Functions (serverless)

### Características Principales

✅ **Procesamiento Incremental**: Solo procesa archivos nuevos desde la última ejecución  
✅ **Multi-planta**: Soporta múltiples plantas (JPV, Río Branco)  
✅ **Multi-sensor**: Procesa datos de múltiples sensores por planta  
✅ **Cruce con Laboratorio**: Identifica automáticamente tachadas y variedades  
✅ **Calibración**: Aplica curvas de calibración para temperatura y humedad  
✅ **Machine Learning**: Clasificación y validación automática de datos  
✅ **Consolidación Histórica**: Compilación automática de archivos validados en dataset único  
✅ **Reportes Automáticos**: Generación de HTML con métricas y gráficos  
✅ **Serverless**: Ejecuta en Azure Functions sin infraestructura propia  
✅ **Autenticación Service Account**: Sin interacción humana requerida  
✅ **Costo Cero**: Uso dentro de niveles gratuitos de Azure y Google Drive

---

## 🏗️ Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────┐
│         INTERFAZ APPS SCRIPT - CARGA (Operarios)                  │
│  • Carga archivos crudos extraídos de sensores                  │
│  • Sube archivos a carpeta correspondiente en Google Drive       │
│  • Hace POST a etl_trigger para ejecutar ETL                    │
│  • Al completarse, dispara ml_trigger automáticamente            │
└──────────────────────┬──────────────────────────────────────────┘
                       │ HTTP POST (JSON metadata)
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GOOGLE DRIVE                                  │
│  • Archivos RAW (sensores) en carpetas por planta/sensor         │
│  • Archivos de Laboratorio (Control Tachadas)                    │
│  • Archivos de Calibración (Curvas)                              │
│  • Archivos Procesados (CSV) en processed/                      │
│  • Archivos Validados (CSV) en validated/                       │
│  • Dataset Histórico (df_historico.csv) en validated/           │
│  • Reportes HTML en reportes/                                   │
│  • Timestamps (JSON) en etl_timestamps/                         │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                   AZURE FUNCTION APP                            │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  1. ETL TRIGGER (etl_trigger)                            │  │
│  │     • Recibe metadata del archivo                        │  │
│  │     • Lee timestamp de última ejecución                  │  │
│  │     • Lista archivos nuevos en carpeta                    │  │
│  │     • Descarga y procesa cada archivo                     │  │
│  │     • Cruza con datos de laboratorio                     │  │
│  │     • Aplica curvas de calibración                       │  │
│  │     • Genera CSV procesado                               │  │
│  │     • Guarda en processed/                               │  │
│  └───────────────┬──────────────────────────────────────────┘  │
│                  │                                               │
│                  ▼                                               │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  2. ML TRIGGER (ml_trigger)                              │  │
│  │     • Recibe solicitud para procesar archivo procesado    │  │
│  │     • Lee archivo CSV desde processed/                    │  │
│  │     • Aplica modelos de Machine Learning                  │  │
│  │     • Clasifica y valida datos                           │  │
│  │     • Genera archivo validado con predicciones            │  │
│  │     • Guarda en validated/                               │  │
│  └───────────────┬──────────────────────────────────────────┘  │
│                  │                                               │
│                  ▼                                               │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  3. COMPILADOR TRIGGER (compilador_trigger)              │  │
│  │     • Lee todos los CSV validados desde validated/        │  │
│  │     • Concatena y elimina duplicados por ID_tachada       │  │
│  │     • Genera/actualiza df_historico.csv                   │  │
│  │     • Guarda en validated/ (misma carpeta)                │  │
│  └───────────────┬──────────────────────────────────────────┘  │
│                  │                                               │
│                  ▼                                               │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  4. REPORTE TRIGGER (reporte_trigger)                    │  │
│  │     • Lee df_historico.csv desde validated/              │  │
│  │     • Calcula métricas estadísticas                       │  │
│  │     • Genera gráficos embebidos (base64)                 │  │
│  │     • Crea reporte HTML completo                         │  │
│  │     • Guarda en reportes/                                │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              SHARED CODE (shared_code/)                   │  │
│  │  • GoogleDriveClient: Acceso a Google Drive              │  │
│  │  • TimestampManager: Gestión de timestamps                │  │
│  │  • ETL Core: Procesamiento de archivos                   │  │
│  │  • Lab Crosser: Cruce con laboratorio                    │  │
│  │  • Calibración: Aplicación de curvas                     │  │
│  │  • ML Predictor: Modelos de Machine Learning             │  │
│  │  • Compilador Histórico: Consolidación de archivos       │  │
│  │  • Reporte Builder: Generación de reportes HTML          │  │
│  │  • Config: Gestión de configuración                     │  │
│  └──────────────────────────────────────────────────────────┘  │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│         INTERFAZ APPS SCRIPT - REPORTE (Latitud)                 │
│  • Selector de planta                                            │
│  • Al hacer clic en "Generar Reporte":                          │
│    ├─> Llama a compilador_trigger (actualiza df_historico.csv)  │
│    └─> Llama a reporte_trigger (genera HTML)                   │
│  • Descarga reporte y envía correo con PDF adjunto              │
└─────────────────────────────────────────────────────────────────┘
```

### Tecnologías Utilizadas

- **Python 3.11+**: Lenguaje principal
- **Azure Functions**: Plataforma serverless (Consumption Plan)
- **Azure Storage Account**: Blob Storage para artefactos y dependencias
- **Google Drive API**: Almacenamiento y acceso a archivos
- **Pandas**: Procesamiento de datos
- **Google Service Account**: Autenticación sin interacción
- **Matplotlib/Seaborn**: Generación de gráficos para reportes

---

## 🔧 Componentes Principales

### 1. **GoogleDriveClient** (`shared_code/gdrive_client.py`)

Cliente para interactuar con Google Drive usando **Service Account**.

**Funcionalidades:**
- ✅ Autenticación automática con Service Account
- ✅ Descarga de archivos por `fileId` o `file_path`
- ✅ Listado de archivos por carpeta o `folderId`
- ✅ Subida de archivos procesados
- ✅ Creación de carpetas automática
- ✅ Actualización de archivos existentes

**Métodos principales:**
```python
client = GoogleDriveClient()
content = client.download_file("path/to/file.txt", file_id="1abc123...")
files = client.list_files_by_folder_id("folder_id_123")
client.upload_file_to_folder(folder_id, "output.csv", csv_bytes, mime_type="text/csv")
```

**Autenticación:**
- Busca credenciales en:
  1. Variable de entorno `GOOGLE_SERVICE_ACCOUNT_JSON` (recomendado para Azure)
  2. Archivo `service-account-key.json` en la raíz del proyecto

### 2. **TimestampManager** (`shared_code/timestamp_manager.py`)

Gestiona timestamps de última ejecución para procesamiento incremental.

**Funcionalidades:**
- ✅ Lee timestamp de última ejecución por planta-secadora
- ✅ Actualiza timestamp después de procesar
- ✅ Guarda metadata de archivos procesados
- ✅ Almacena timestamps en Google Drive (carpeta `etl_timestamps/`)

**Estructura del archivo de timestamp:**
```json
{
  "planta": "JPV",
  "secadora": "Secadora 1",
  "last_run": "2025-11-19T14:30:00.000Z",
  "last_processed_files": [
    {
      "fileId": "1abc123...",
      "fileName": "datos_001.xlsx",
      "processedAt": "2025-11-19T14:30:00.000Z",
      "status": "success",
      "records_processed": 750
    }
  ],
  "total_files_processed": 5,
  "last_updated": "2025-11-19T14:30:00.000Z"
}
```

**Ubicación:** `etl_timestamps/last_run_timestamp_{PLANTA}_{SECADORA}.json`

### 3. **ETL Core** (`shared_code/etl_core.py`)

Procesamiento de archivos de sensores con formato unificado para JPV y RB.

**Funcionalidades:**
- ✅ Lectura de archivos JPV (TXT, UTF-16)
- ✅ Lectura de archivos RB (CSV, UTF-8) con detección robusta de formatos
- ✅ Normalización de timestamps
- ✅ Conversión a formato largo (long format)
- ✅ Detección automática de formato y separadores
- ✅ Estandarización de variables: ambas plantas producen `VOLT_HUM` y `VOLT_TEM`

**Formatos soportados:**
- **JPV**: Archivos TXT con encoding UTF-16, columnas: `Time`, `VarName`, `VarValue`
- **RB**: Archivos CSV con encoding UTF-8
  - Separadores: `;` o `,` (detección automática)
  - Columnas de fecha: `Date`, `Fecha`
  - Columnas de hora: `Time`, `Hora`, `LOC_time`, `LOCTime`
  - Columnas de voltaje: `V_Hum`, `V_HUM`, `V_Tem`, `V_TEM`, `V_Temp`, etc.

**Salida unificada (JPV y RB):**
DataFrame con columnas:
- `timestamp`: Fecha/hora normalizada (UTC)
- `variable`: `VOLT_HUM` o `VOLT_TEM` (estandarizado para ambas plantas)
- `valor`: Valor numérico del voltaje (RB dividido por 100 automáticamente)
- `Date_raw`: Fecha cruda (para RB) o `None` (para JPV)
- `LOC_time_raw`: Hora cruda (para RB) o `None` (para JPV)
- `planta`: Planta de origen (`JPV` o `RB`)
- `sensor_id`: ID del sensor (inferido del nombre del archivo)
- `source_file`: Nombre del archivo de origen

**Nota importante:** La calibración (conversión de `VOLT_HUM`/`VOLT_TEM` a `HUMEDAD`/`TEMPERATURA`) se aplica **después** en el pipeline cuando hay información del laboratorio, no en esta etapa.

### 4. **Lab Crosser** (`shared_code/lab_crosser.py`)

Cruce de datos de sensores con controles de laboratorio.

**Funcionalidades:**
- ✅ Carga archivos Excel de "Control Tachadas"
- ✅ Normalización de IDs de tachadas
- ✅ Cruce por timestamp y sensor_id
- ✅ Identificación de variedades y tachadas

**Columnas agregadas:**
- `Variedad`: Variedad de arroz
- `ID_tachada`: ID de la tachada
- `HumedadInicial`: Humedad inicial de laboratorio
- `HumedadFinal`: Humedad final de laboratorio
- `En_duda`: Indicador de datos en duda

### 5. **Calibración** (`shared_code/calibracion.py`)

Aplicación de curvas de calibración para convertir voltajes a valores reales.

**Funcionalidades:**
- ✅ Búsqueda de archivos de curvas de calibración por planta y año
- ✅ Conversión de VOLT_HUM → HUMEDAD (por variedad)
- ✅ Conversión de VOLT_TEM → TEMPERATURA (global)
- ✅ Aplicación de correcciones fijas (C_fix) y variables (C_var) por sensor
- ✅ Soporte para múltiples variedades con curvas específicas

**Fórmulas aplicadas:**
- **TEMPERATURA**: `VT * AT + BT + C_fix_T[sensor] - C_var_T[sensor, timestamp]`
- **HUMEDAD**: `(VH²) * AH + VH * BH + CH + C_fix_H[sensor] - C_var_H[sensor, timestamp]`

**Estructura de archivos de calibración:**
- Hoja "TEMPERATURA": Constantes AT, BT y correcciones por sensor
- Hojas por variedad: Constantes AH, BH, CH y correcciones por variedad y sensor

### 6. **ML Predictor** (`shared_code/ml_predictor.py`)

Aplicación de modelos de Machine Learning para clasificación y validación.

**Funcionalidades:**
- ✅ Lee archivos procesados desde `processed/`
- ✅ Aplica modelos de ML para clasificar datos
- ✅ Valida datos y detecta anomalías
- ✅ Categoriza datos correctamente
- ✅ Genera archivos validados con predicciones

**Columnas agregadas en archivos validados:**
- `categoria`: Categoría asignada por el modelo ML
- `confianza`: Nivel de confianza de la clasificación
- `valido`: Indicador de validación (True/False)
- `prediccion`: Predicción binaria (0/1)
- `anomalia`: Indicador de detección de anomalías

### 7. **Compilador Histórico** (`shared_code/compilador_historico.py`)

Consolida todos los archivos CSV validados en un dataset histórico único.

**Funcionalidades:**
- ✅ Lee todos los CSV validados desde `validated/`
- ✅ Concatena archivos en un DataFrame único
- ✅ Elimina duplicados por `ID_tachada` (mantiene la primera ocurrencia)
- ✅ Agrega columna `archivo_origen` para trazabilidad
- ✅ Genera/actualiza `df_historico.csv` en `validated/`

**Resultado:**
- Dataset histórico consolidado con una fila por tachada
- Datos agregados por tachada (duración, estadísticas, etc.)
- Ubicación: `validated/df_historico.csv` (misma carpeta que los archivos validados)

### 8. **Reporte Builder** (`shared_code/reporte_builder.py`)

Módulo de generación de reportes HTML con métricas y gráficos a partir del dataset histórico consolidado.

**Funcionalidades:**
- ✅ Lee `df_historico.csv` desde `validated/`
- ✅ Calcula métricas estadísticas por planta, sensor, variedad, tachada
- ✅ Genera gráficos y visualizaciones embebidos en HTML (base64)
- ✅ Detección dinámica de columnas con fallbacks para compatibilidad
- ✅ Genera reporte HTML completo con diseño profesional
- ✅ Integra logo de la empresa desde Google Drive
- ✅ Manejo robusto de columnas faltantes (no rompe si faltan datos)

**Estructura del reporte:**
- **BLOQUE 1**: Cantidad de tachadas (global, por secadora, evolución semanal)
- **BLOQUE 2**: Temperaturas (por turno, variedad, distribución)
- **BLOQUE 3**: Duración de las tachadas (estadísticas, distribución, evolución)
- **BLOQUE 4**: Comparación con laboratorio (humedad inicial/final, diferencias)

**Salida:**
- Reporte HTML con gráficos embebidos (base64)
- Ubicación: Google Drive (carpeta `reportes/`)

### 9. **Config** (`shared_code/config.py`)

Gestión centralizada de configuración de folder IDs de Google Drive.

**Funcionalidades:**
- ✅ Obtiene folder IDs desde variables de entorno
- ✅ Validación de configuración por planta
- ✅ Funciones helper: `get_lab_folder_id()`, `get_processed_folder_id()`, `get_validated_folder_id()`, `get_reports_folder_id()`

**Variables de entorno requeridas:**
- `LAB_FOLDER_JPV`, `LAB_FOLDER_RB`: Carpetas de archivos de laboratorio
- `PROCESSED_FOLDER_JPV`, `PROCESSED_FOLDER_RB`: Carpetas de archivos procesados
- `VALIDATED_FOLDER_JPV`, `VALIDATED_FOLDER_RB`: Carpetas de archivos validados
- `REPORTS_FOLDER_JPV`, `REPORTS_FOLDER_RB`: Carpetas de reportes

---

## 🔄 Flujo de Datos

### Flujo Completo del Sistema

```
┌─────────────────────────────────────────────────────────────────┐
│  FASE 1: CARGA DE DATOS (Google Apps Script - Interfaz Carga)   │
└─────────────────────────────────────────────────────────────────┘

1. INTERFAZ GOOGLE APPS SCRIPT (Operarios)
   ├─> Usuario carga archivos crudos extraídos de sensores
   ├─> Selecciona planta (JPV o RB) y secadora
   ├─> Archivos se suben a carpeta correspondiente en Google Drive
   │   └─> Ubicación: Secado_Arroz/{PLANTA}/raw/{SENSOR}/
   └─> Hace POST a Azure Function (etl_trigger) con metadata del archivo
       └─> Metadata: fileId, fileName, secadora, planta, folderId, uploadDate, etc.

┌─────────────────────────────────────────────────────────────────┐
│  FASE 2: PROCESAMIENTO ETL (etl_trigger)                       │
└─────────────────────────────────────────────────────────────────┘

2. AZURE FUNCTION (etl_trigger)
   ├─> Valida metadata recibida
   ├─> Inicializa GoogleDriveClient
   ├─> Inicializa TimestampManager
   │
   ├─> MODO INCREMENTAL (si tiene folderId y secadora):
   │   ├─> Lee timestamp de última ejecución desde etl_timestamps/
   │   ├─> Lista TODOS los archivos en la carpeta de esa secadora
   │   ├─> Filtra archivos modificados después del timestamp
   │   └─> Procesa cada archivo nuevo
   │
   └─> MODO LEGACY (si no tiene folderId o secadora):
       └─> Procesa solo el archivo recibido

3. PROCESAMIENTO POR ARCHIVO:
   ├─> Descarga archivo RAW desde Google Drive
   ├─> Detecta formato (JPV TXT o RB CSV)
   ├─> Lee y normaliza datos de sensores
   ├─> Convierte a formato largo (long format)
   │   └─> Estandariza variables: JPV y RB producen VOLT_HUM/VOLT_TEM
   ├─> Consolida datos de múltiples sensores (si existen)
   ├─> Busca archivo de laboratorio correspondiente
   ├─> Cruza datos con laboratorio (Lab Crosser)
   │   └─> Agrega: Variedad, ID_tachada, HumedadInicial, HumedadFinal, En_duda
   ├─> Busca archivos de curvas de calibración
   ├─> Aplica calibración (calibracion.py)
   │   ├─> Convierte formato largo a ancho (to_wide)
   │   └─> Convierte VOLT_HUM → HUMEDAD, VOLT_TEM → TEMPERATURA
   └─> Genera CSV procesado (formato unificado para JPV y RB)

4. GUARDADO ETL:
   ├─> Sube CSV procesado a Google Drive
   │   └─> Ubicación: Secado_Arroz/{PLANTA}/processed/
   └─> Actualiza timestamp (solo en modo incremental)

5. ORQUESTACIÓN AUTOMÁTICA:
   └─> Al completarse etl_trigger exitosamente, la lógica de orquestación
       (en Apps Script o en el mismo etl_trigger) dispara ml_trigger
       automáticamente con la información del nuevo archivo procesado

┌─────────────────────────────────────────────────────────────────┐
│  FASE 3: VALIDACIÓN ML (ml_trigger)                            │
└─────────────────────────────────────────────────────────────────┘

6. AZURE FUNCTION (ml_trigger)
   ├─> Recibe solicitud para procesar archivo procesado
   ├─> Lee archivo CSV desde carpeta processed/
   ├─> Aplica modelos de Machine Learning:
   │   ├─> Clasifica datos según categorías predefinidas
   │   ├─> Valida calidad de datos
   │   ├─> Detecta anomalías y outliers
   │   └─> Categoriza datos correctamente
   └─> Genera archivo validado con columnas adicionales

7. GUARDADO ML:
   └─> Sube CSV validado a Google Drive
       └─> Ubicación: Secado_Arroz/{PLANTA}/validated/

┌─────────────────────────────────────────────────────────────────┐
│  FASE 4: CONSOLIDACIÓN HISTÓRICA (compilador_trigger)          │
└─────────────────────────────────────────────────────────────────┘

8. AZURE FUNCTION (compilador_trigger)
   ├─> Recibe solicitud desde interfaz de reporte (Apps Script)
   ├─> Lee todos los CSV validados desde validated/
   ├─> Concatena archivos en un DataFrame único
   ├─> Elimina duplicados por ID_tachada (mantiene primera ocurrencia)
   ├─> Agrega columna archivo_origen para trazabilidad
   └─> Genera/actualiza df_historico.csv en validated/

9. GUARDADO HISTÓRICO:
   └─> Sube/actualiza df_historico.csv en Google Drive
       └─> Ubicación: Secado_Arroz/{PLANTA}/validated/df_historico.csv

┌─────────────────────────────────────────────────────────────────┐
│  FASE 5: GENERACIÓN DE REPORTES (reporte_trigger)               │
└─────────────────────────────────────────────────────────────────┘

10. INTERFAZ GOOGLE APPS SCRIPT (Reporte - Latitud)
    ├─> Usuario selecciona planta (JPV o RB)
    ├─> Hace clic en "Generar Reporte"
    ├─> Llama primero a compilador_trigger (actualiza df_historico.csv)
    └─> Al completarse exitosamente, llama a reporte_trigger

11. AZURE FUNCTION (reporte_trigger)
    ├─> Descarga df_historico.csv desde validated/
    ├─> Usa reporte_builder.py para generar reporte HTML:
    │   ├─> Detección dinámica de columnas (con fallbacks)
    │   ├─> Calcula métricas estadísticas por tachada, planta, variedad
    │   ├─> Genera gráficos embebidos (base64)
    │   ├─> Incluye logo de la empresa
    │   └─> Manejo robusto de columnas faltantes
    └─> Crea reporte HTML completo con diseño profesional

12. GUARDADO REPORTE:
    └─> Sube reporte HTML a Google Drive
        └─> Ubicación: Secado_Arroz/{PLANTA}/reportes/reporte_tachadas_{PLANTA}.html

13. DISTRIBUCIÓN:
    └─> Apps Script descarga el reporte generado
        └─> Envía correo con PDF adjunto y enlace/botón para acceder al HTML
```

### Ejemplo de Metadata Recibida por etl_trigger

```json
{
  "fileId": "1abc123xyz789",
  "fileName": "sensor_1_datos_2025_11_19.xlsx",
  "secadora": "Secadora 1",
  "planta": "JPV",
  "folderId": "1xyz789abc123",
  "uploadDate": "2025-11-19T14:30:00.000Z",
  "size": 245760,
  "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  "fileUrl": "https://drive.google.com/file/d/1abc123xyz789/view",
  "driveUrl": "https://drive.google.com/file/d/1abc123xyz789/view"
}
```

### Ejemplo de Respuesta de etl_trigger (Modo Incremental)

```json
{
  "success": true,
  "message": "ETL incremental completado - 2 archivos procesados",
  "timestamp": "2025-11-19T15:00:00.000Z",
  "metadata": {
    "planta": "JPV",
    "secadora": "Secadora 1",
    "year": 2025,
    "total_files_processed": 2,
    "total_records_processed": 1500,
    "total_records_matched_lab": 1200,
    "total_records_unmatched": 300,
    "last_run_timestamp": "2025-11-19T10:00:00.000Z"
  },
  "processed_files": [
    {
      "fileId": "1abc123...",
      "fileName": "archivo_B.xlsx",
      "processedAt": "2025-11-19T15:00:00.000Z",
      "status": "success",
      "records_processed": 750,
      "records_matched_lab": 600,
      "records_unmatched": 150,
      "processed_file": "archivo_B_processed_20251119T150000Z.csv",
      "processed_path": "Secado_Arroz/JPV/processed/archivo_B_processed_20251119T150000Z.csv"
    }
  ]
}
```

---

## ⏱️ Sistema de Timestamps Incremental

### Objetivo

Evitar reprocesar archivos ya procesados, procesando solo archivos nuevos desde la última ejecución.

### Funcionamiento

1. **Primera Ejecución:**
   - No existe timestamp → procesa TODOS los archivos en la carpeta
   - Guarda timestamp con fecha/hora actual

2. **Ejecuciones Subsecuentes:**
   - Lee timestamp de última ejecución desde `etl_timestamps/`
   - Lista todos los archivos en la carpeta
   - Filtra solo archivos con `modifiedTime > last_run`
   - Procesa solo archivos nuevos
   - Actualiza timestamp con el `modifiedTime` más reciente

### Ventajas

✅ **Eficiencia**: No reprocesa archivos ya procesados  
✅ **Procesamiento en lote**: Procesa múltiples archivos nuevos en una ejecución  
✅ **Resiliencia**: Si falla un archivo, continúa con los demás  
✅ **Trazabilidad**: Guarda metadata de archivos procesados  

### Ejemplo de Caso de Uso

**Escenario:**
- Última ejecución: `2025-11-19 10:00:00`
- Carpeta tiene 3 archivos:
  - `archivo_A.xlsx` (modificado: `09:00:00`) → ❌ NO procesar
  - `archivo_B.xlsx` (modificado: `11:00:00`) → ✅ SÍ procesar
  - `archivo_C.xlsx` (modificado: `12:00:00`) → ✅ SÍ procesar

**Resultado:**
- Procesa 2 archivos (B y C)
- Guarda timestamp: `2025-11-19 12:00:00`
- Próxima ejecución solo procesará archivos posteriores a `12:00:00`

### Archivos de Timestamp

**Ubicación:** `etl_timestamps/last_run_timestamp_{PLANTA}_{SECADORA}.json`

**Ejemplos:**
- `etl_timestamps/last_run_timestamp_JPV_Secadora_1.json`
- `etl_timestamps/last_run_timestamp_JPV_Secadora_2.json`
- `etl_timestamps/last_run_timestamp_RB_Secadora_1.json`

---

## 🔐 Autenticación y Seguridad

### Service Account (Google Cloud)

El sistema usa **Service Account** para autenticación automática sin interacción humana.

**Ventajas:**
- ✅ No requiere navegador/consola
- ✅ Ideal para entornos serverless (Azure Functions)
- ✅ Autenticación automática sin tokens expirables
- ✅ No depende de sesiones de usuario

**Configuración:**

1. **Crear Service Account en Google Cloud Console:**
   - IAM & Admin → Service Accounts
   - Crear nueva Service Account
   - Descargar archivo JSON de credenciales

2. **Compartir carpetas en Google Drive:**
   - Compartir carpetas necesarias con el email de la Service Account
   - Formato: `nombre@proyecto.iam.gserviceaccount.com`
   - Permisos: Editor para carpetas de salida, Lector para carpetas de entrada

3. **Configurar en Azure Function App:**
   - Settings → Configuration → Application settings
   - Agregar variable: `GOOGLE_SERVICE_ACCOUNT_JSON`
   - Valor: Contenido completo del JSON (como string)

**Scopes requeridos:**
- `https://www.googleapis.com/auth/drive` (lectura y escritura)

### Seguridad

- ✅ Credenciales almacenadas como variables de entorno (no en código)
- ✅ Service Account con permisos mínimos necesarios
- ✅ Autenticación HTTP requerida en Azure Functions (`authLevel: "function"`)

---

## ⚙️ Configuración

### Variables de Entorno (Azure Functions)

```json
{
  "GOOGLE_SERVICE_ACCOUNT_JSON": "{...contenido completo del JSON...}",
  "LAB_FOLDER_JPV": "folder_id_lab_jpv",
  "LAB_FOLDER_RB": "folder_id_lab_rb",
  "PROCESSED_FOLDER_JPV": "folder_id_processed_jpv",
  "PROCESSED_FOLDER_RB": "folder_id_processed_rb",
  "VALIDATED_FOLDER_JPV": "folder_id_validated_jpv",
  "VALIDATED_FOLDER_RB": "folder_id_validated_rb",
  "REPORTS_FOLDER_JPV": "folder_id_reports_jpv",
  "REPORTS_FOLDER_RB": "folder_id_reports_rb"
}
```

---

## 🔄 Procesamiento ETL

### Pasos del Procesamiento

1. **Descarga de Archivo**
   - Usa `fileId` directamente (más eficiente)
   - Fallback a búsqueda por `file_path` si no hay `fileId`

2. **Detección de Formato**
   - **JPV**: Archivos TXT, encoding UTF-16
   - **RB**: Archivos CSV, encoding UTF-8

3. **Lectura y Normalización**
   - **JPV**: Parsea timestamps desde `TimeString` o `Time`
   - **RB**: Construye timestamp desde `Date` + `LOC time` (detección robusta de columnas)
   - Normaliza nombres de variables a formato estándar
   - **RB**: Divide valores de voltaje por 100 (x0.01) para equiparar con JPV
   - Convierte valores a numérico
   - Filtra variables de metadata (ej: `$RT_*` para JPV)

4. **Conversión a Formato Largo**
   - Transforma de formato ancho a largo mediante `melt()`
   - **Estandarización de variables:**
     - JPV: Normaliza variables a `VOLT_HUM`, `VOLT_TEM`
     - RB: Mapea `V_HUM`/`V_HUM` → `VOLT_HUM`, `V_TEM`/`V_TEM` → `VOLT_TEM`
   - Columnas resultantes: `timestamp`, `variable` (`VOLT_HUM`/`VOLT_TEM`), `valor`
   - Agrega metadata: `planta`, `sensor_id`, `source_file`, `Date_raw`, `LOC_time_raw`

5. **Cruce con Laboratorio**
   - Busca archivo Excel de "Control Tachadas"
   - Cruza por `timestamp` y `sensor_id`
   - Agrega: `Variedad`, `ID_tachada`, `HumedadInicial`, `HumedadFinal`, `En_duda`

6. **Consolidación de Sensores**
   - Agrupa datos de múltiples sensores si existen
   - Elimina duplicados por `timestamp` y `variable`
   - Unifica formato para preparar la calibración

7. **Aplicación de Calibración**
   - Busca archivos de curvas de calibración por planta y año
   - Patrón: `*{AÑO}*Curvas*{PLANTA}*.xlsx`
   - **Importante**: Solo se aplica cuando hay información del laboratorio (variedad)
   - Convierte formato largo a ancho (`to_wide()`) para aplicar calibración
   - Lee hoja "TEMPERATURA" para constantes AT, BT
   - Lee hoja de variedad para constantes AH, BH, CH
   - Aplica fórmulas de calibración:
     - **TEMPERATURA**: `VT * AT + BT + C_fix_T[sensor] - C_var_T[sensor, timestamp]`
     - **HUMEDAD**: `(VH²) * AH + VH * BH + CH + C_fix_H[sensor] - C_var_H[sensor, timestamp]`
   - Agrega columnas: `TEMPERATURA`, `HUMEDAD`
   - Convierte de vuelta a formato largo si es necesario

8. **Generación de CSV Procesado**
   - Ordena columnas consistentemente
   - Incluye columnas: `timestamp`, `variable`, `valor`, `planta`, `sensor_id`, `source_file`, `Variedad`, `ID_tachada`, `VOLT_HUM`, `VOLT_TEM`, `TEMPERATURA`, `HUMEDAD`
   - Guarda en formato CSV UTF-8
   - Nombre: `{archivo_original}_processed_{timestamp}.csv`
   - **Resultado**: Formato idéntico para JPV y RB

### Manejo de Errores

- ✅ Si un archivo falla, continúa con los demás
- ✅ Logging detallado de cada error
- ✅ Respuesta JSON con status de cada archivo
- ✅ Timestamp se actualiza solo si hay archivos procesados exitosamente

---

## 📁 Integración con Google Drive

### Estructura de Carpetas

```
Secado_Arroz/
├── JPV/
│   ├── raw/
│   │   ├── sensor_1/
│   │   │   ├── archivo_001.txt
│   │   │   └── archivo_002.txt
│   │   ├── sensor_2/
│   │   ├── laboratorio/
│   │   │   └── Control_Tachadas_2025.xlsx
│   │   └── calibracion/
│   │       └── 2025 Curvas JPV.xlsx
│   ├── processed/
│   │   ├── archivo_001_processed_20251119T150000Z.csv
│   │   └── archivo_002_processed_20251119T160100Z.csv
│   ├── validated/
│   │   ├── archivo_001_validated_20251119T160000Z.csv
│   │   ├── archivo_002_validated_20251119T160100Z.csv
│   │   └── df_historico.csv  ← Dataset histórico consolidado
│   └── reportes/
│       ├── reporte_tachadas_JPV.html
│       └── (imágenes PNG de gráficos)
├── RB/
│   ├── raw/
│   │   ├── sensor_1/
│   │   └── laboratorio/
│   ├── processed/
│   ├── validated/
│   │   └── df_historico.csv  ← Dataset histórico consolidado
│   └── reportes/
│       └── reporte_tachadas_RB.html
└── etl_timestamps/
    ├── last_run_timestamp_JPV_Secadora_1.json
    ├── last_run_timestamp_JPV_Secadora_2.json
    └── last_run_timestamp_RB_Secadora_1.json
```

### Operaciones en Google Drive

**Lectura:**
- Listar archivos por carpeta
- Descargar archivos por `fileId` o `file_path`
- Leer archivos de timestamps

**Escritura:**
- Subir archivos procesados (CSV)
- Subir archivos validados (CSV)
- Crear/actualizar `df_historico.csv`
- Subir reportes HTML e imágenes
- Crear carpetas automáticamente
- Actualizar archivos de timestamps

---

## 📱 Interfaz Google Apps Script

### Dos Interfaces Diferenciadas

#### 1. Interfaz de Carga (Operarios)

Interfaz independiente para cada planta (JPV y RB) que permite cargar archivos de sensores.

**Funcionalidades:**
- ✅ Interfaz de usuario para carga de archivos
- ✅ Detección automática de planta y sensor desde nombre de archivo o selección manual
- ✅ Subida automática a carpeta correspondiente en Google Drive
- ✅ Trigger automático del ETL mediante POST a `etl_trigger`
- ✅ Orquestación automática: al completarse `etl_trigger`, dispara `ml_trigger` con el nuevo archivo procesado
- ✅ Validación de formato y estructura de archivos

**Flujo de Operación:**

1. **Usuario carga archivo:**
   - Selecciona archivo desde dispositivo local
   - La interfaz detecta o permite seleccionar planta y sensor

2. **Subida a Google Drive:**
   - Archivo se sube a: `Secado_Arroz/{PLANTA}/raw/{SENSOR}/`
   - Se preserva nombre original o se renombra con timestamp

3. **Trigger de ETL:**
   - Hace POST a Azure Function `/api/etl_trigger`
   - Envía metadata: `fileId`, `fileName`, `planta`, `secadora`, `folderId`, etc.
   - Azure Function procesa el archivo automáticamente

4. **Orquestación automática:**
   - Al completarse `etl_trigger` exitosamente, la lógica de orquestación (en Apps Script o en el mismo `etl_trigger`) dispara `ml_trigger` automáticamente con la información del nuevo archivo procesado

**Ventajas:**
- ✅ Interfaz familiar (Google Apps Script)
- ✅ Sin necesidad de acceso directo a Google Drive API
- ✅ Procesamiento automático sin intervención manual
- ✅ Trazabilidad completa del proceso

#### 2. Interfaz de Solicitud de Reporte (Latitud)

Interfaz para solicitar y distribuir reportes HTML.

**Funcionalidades:**
- ✅ Selector de planta (JPV o RB)
- ✅ Botón "Generar Reporte"
- ✅ Orquestación de dos pasos:
  1. Llama a `compilador_trigger` (actualiza `df_historico.csv`)
  2. Al completarse exitosamente, llama a `reporte_trigger` (genera HTML)
- ✅ Descarga del reporte generado
- ✅ Envío de correo con PDF adjunto y enlace/botón para acceder al HTML

**Flujo de Operación:**

1. **Usuario solicita reporte:**
   - Selecciona planta (JPV o RB)
   - Hace clic en "Generar Reporte"

2. **Consolidación histórica:**
   - Llama a `compilador_trigger` con parámetro `planta`
   - `compilador_trigger` lee todos los CSV validados desde `validated/`
   - Concatena y elimina duplicados por `ID_tachada`
   - Genera/actualiza `df_historico.csv` en `validated/`

3. **Generación de reporte:**
   - Al completarse `compilador_trigger` exitosamente, llama a `reporte_trigger` con parámetro `planta`
   - `reporte_trigger` descarga `df_historico.csv` desde `validated/`
   - Genera reporte HTML con métricas y gráficos
   - Sube reporte a `reportes/`

4. **Distribución:**
   - Apps Script descarga el reporte generado
   - Envía correo con PDF adjunto y enlace/botón para acceder al HTML

---

## ☁️ Azure Functions

### Infraestructura en Azure

El sistema corre sobre **Azure Functions** con la siguiente configuración:

1. **Storage Account (Blob Storage)**
   - Almacena artefactos internos y dependencias
   - Requerido por Azure Functions para operación

2. **Function App**
   - Entorno Python 3.11
   - Plan de consumo (Consumption Plan)
   - Variables de entorno configuradas
   - Código compartido en carpeta `shared_code/`

3. **Azure Functions Individuales**
   - `etl_trigger`: Procesamiento ETL
   - `ml_trigger`: Validación con Machine Learning
   - `compilador_trigger`: Consolidación histórica
   - `reporte_trigger`: Generación de reportes HTML

### Funciones Disponibles

#### 1. **`etl_trigger`** (HTTP Trigger)

- **Ruta:** `/api/etl_trigger`
- **Método:** POST
- **Autenticación:** Function key
- **Función:** Procesa archivos RAW de sensores
- **Procesos:**
  - Descarga archivos desde Google Drive
  - Normaliza y transforma datos
  - Cruza con datos de laboratorio
  - Aplica calibración
  - Genera archivos procesados
- **Salida:** CSV procesado en `processed/`

#### 2. **`ml_trigger`** (HTTP Trigger)

- **Ruta:** `/api/ml_trigger` o `/api/ml`
- **Método:** POST
- **Autenticación:** Function key
- **Función:** Clasifica y valida datos procesados
- **Procesos:**
  - Lee archivos desde `processed/`
  - Aplica modelos de Machine Learning
  - Clasifica y categoriza datos
  - Valida calidad y detecta anomalías
  - Genera archivos validados
- **Salida:** CSV validado en `validated/`

#### 3. **`compilador_trigger`** (HTTP Trigger)

- **Ruta:** `/api/compilador_trigger` o `/api/compilador`
- **Método:** POST
- **Autenticación:** Function key
- **Función:** Consolida archivos validados en dataset histórico único
- **Procesos:**
  - Lee todos los CSV validados desde `validated/`
  - Concatena archivos en un DataFrame único
  - Elimina duplicados por `ID_tachada` (mantiene primera ocurrencia)
  - Agrega columna `archivo_origen` para trazabilidad
  - Genera/actualiza `df_historico.csv`
- **Salida:** `df_historico.csv` en `validated/`

#### 4. **`reporte_trigger`** (HTTP Trigger)

- **Ruta:** `/api/reporte_trigger` o `/api/reporte`
- **Método:** POST
- **Autenticación:** Function key
- **Función:** Genera reportes HTML con métricas y gráficos
- **Procesos:**
  - Descarga `df_historico.csv` desde `validated/`
  - Calcula métricas estadísticas
  - Genera gráficos y visualizaciones embebidos (base64)
  - Crea reporte HTML completo
- **Salida:** Reporte HTML en carpeta `reportes/`

### Configuración de Azure Function App

**Requirements:**
- Python 3.11
- Dependencias en `requirements.txt`
- Variables de entorno configuradas

**Deployment:**
- Código en carpeta `shared_code/` se incluye automáticamente
- Configuración en `host.json`
- Bindings en `{function}/function.json`

### Costo Operativo

El sistema corre sobre un **Consumption Plan** de Azure Functions, con funciones ligeras y de corta duración. El uso combinado de Azure (Functions + Storage) y Google Drive se mantiene dentro de los niveles gratuitos, por lo que el **costo operativo es 0** siempre que se respeten los volúmenes actuales de procesamiento.

---

## 📊 Estructura de Datos

### Formato de Entrada (JPV)

```txt
Time	VarName	VarValue
2025-11-19 10:00:00	V_HUM	45.2
2025-11-19 10:00:00	TEMP	25.3
2025-11-19 10:01:00	V_HUM	45.5
2025-11-19 10:01:00	TEMP	25.4
```

### Formato de Entrada (RB)

**Formato 1 (separador `,`):**
```csv
Date,LOC_time,Record,V_HUM,TEMP,PRES
2025-11-19,10:00:00,1,4520,2380,1013.2
2025-11-19,10:01:00,2,4550,2390,1013.3
```

**Formato 2 (separador `;`):**
```csv
Date;Time;V_Hum;V_Tem
2025-11-19;10:00:00;4520;2380
2025-11-19;10:01:00;4550;2390
```

**Nota:** RB maneja múltiples variantes de nombres de columnas y separadores automáticamente.

### Formato de Salida (CSV Procesado)

**Formato unificado para JPV y RB:**

```csv
timestamp,variable,valor,planta,sensor_id,source_file,Date_raw,LOC_time_raw,Variedad,ID_tachada,HumedadInicial,HumedadFinal,VOLT_HUM,VOLT_TEM,TEMPERATURA,HUMEDAD
2025-11-19T10:00:00Z,VOLT_HUM,2.45,JPV,1,archivo_001.txt,,,Variedad_A,123,25.5,13.2,2.45,3.12,25.3,15.8
2025-11-19T10:00:00Z,VOLT_TEM,3.12,JPV,1,archivo_001.txt,,,Variedad_A,123,25.5,13.2,2.45,3.12,25.3,15.8
2025-11-19T10:00:00Z,VOLT_HUM,45.20,RB,1,SENSOR1_RB.csv,2025-11-19,10:00:00,Variedad_A,123,25.5,13.2,45.20,31.20,25.3,15.8
2025-11-19T10:00:00Z,VOLT_TEM,31.20,RB,1,SENSOR1_RB.csv,2025-11-19,10:00:00,Variedad_A,123,25.5,13.2,45.20,31.20,25.3,15.8
```

**Nota:** Los valores de `variable` son siempre `VOLT_HUM` o `VOLT_TEM` para ambas plantas, estandarizado para el pipeline ML.

### Columnas del CSV Procesado

| Columna | Descripción | Ejemplo |
|---------|-------------|---------|
| `timestamp` | Fecha/hora en UTC (ISO 8601) | `2025-11-19T10:00:00Z` |
| `variable` | Nombre de la variable normalizado | `VOLT_HUM`, `VOLT_TEM` (estandarizado para ambas plantas) |
| `valor` | Valor numérico del voltaje | `2.45`, `3.12` (RB ya dividido por 100) |
| `Date_raw` | Fecha cruda (RB) o `None` (JPV) | `2025-11-19` o vacío |
| `LOC_time_raw` | Hora cruda (RB) o `None` (JPV) | `10:00:00` o vacío |
| `planta` | Planta de origen | `JPV`, `RB` |
| `sensor_id` | ID del sensor | `1`, `2`, `3` |
| `source_file` | Archivo de origen | `archivo_001.txt` |
| `Variedad` | Variedad de arroz (del laboratorio) | `Variedad_A` |
| `ID_tachada` | ID de la tachada (del laboratorio) | `123` |
| `HumedadInicial` | Humedad inicial de laboratorio | `25.5` |
| `HumedadFinal` | Humedad final de laboratorio | `13.2` |
| `VOLT_HUM` | Voltaje de humedad (raw) | `2.45` |
| `VOLT_TEM` | Voltaje de temperatura (raw) | `3.12` |
| `TEMPERATURA` | Temperatura calibrada (°C) | `25.3` |
| `HUMEDAD` | Humedad calibrada (%) | `15.8` |

### Formato de Salida (CSV Validado)

```csv
timestamp,variable,valor,planta,sensor_id,Variedad,ID_tachada,TEMPERATURA,HUMEDAD,categoria,confianza,valido,prediccion,anomalia
2025-11-19T10:00:00Z,TEMPERATURA,25.3,JPV,1,Variedad_A,123,25.3,15.8,normal,0.95,True,0,False
2025-11-19T10:01:00Z,HUMEDAD,15.9,JPV,1,Variedad_B,124,25.4,15.9,normal,0.92,True,0,False
```

### Columnas Adicionales en CSV Validado

| Columna | Descripción | Ejemplo |
|---------|-------------|---------|
| `categoria` | Categoría asignada por modelo ML | `normal`, `anomalia`, `outlier` |
| `confianza` | Nivel de confianza de clasificación (0-1) | `0.95` |
| `valido` | Indicador de validación | `True`, `False` |
| `prediccion` | Predicción binaria (0/1) | `0`, `1` |
| `anomalia` | Indicador de detección de anomalías | `True`, `False` |

### Formato de Salida (Dataset Histórico - df_historico.csv)

El archivo `df_historico.csv` es el resultado de la consolidación de todos los archivos validados. Contiene datos agregados por tachada.

**Estructura principal:**
```csv
ID_tachada,planta,secadora,Variedad,fecha_inicio,fecha_fin,duracion_hs,TEMPERATURA_max,TEMPERATURA_mean,HUMEDAD_max,HUMEDAD_mean,HumedadInicial,HumedadFinal,...
123,JPV,Secadora 1,Variedad_A,2025-11-19 10:00:00,2025-11-20 14:00:00,28.5,45.2,42.1,18.5,15.8,25.5,13.2,...
124,RB,Secadora 1,Variedad_B,2025-11-19 11:00:00,2025-11-20 16:00:00,29.0,46.1,43.2,19.1,16.2,26.0,13.8,...
```

**Columnas principales:**
- `ID_tachada`: Identificador único de la tachada
- `planta`: Planta de origen (JPV o RB)
- `secadora`: Nombre de la secadora (sensor_id)
- `Variedad`: Variedad de arroz
- `fecha_inicio`, `fecha_fin`: Rango temporal de la tachada
- `duracion_hs`: Duración en horas
- `TEMPERATURA_max`, `TEMPERATURA_mean`: Estadísticas de temperatura
- `HUMEDAD_max`, `HUMEDAD_mean`: Estadísticas de humedad
- `HumedadInicial`, `HumedadFinal`: Valores de laboratorio
- Otras columnas agregadas por el proceso ML

**Ubicación:** `validated/df_historico.csv` (misma carpeta que los archivos validados)

**Nota:** Este formato es el que consume `reporte_builder.py` para generar los reportes HTML. El sistema es robusto y detecta dinámicamente las columnas disponibles, usando fallbacks cuando algunas columnas no existen.

---

## 🔍 Casos de Uso

### Caso 1: Procesamiento Incremental

**Escenario:** Múltiples archivos se suben a Google Drive en un corto período.

**Comportamiento:**
1. Google Apps Script detecta cada archivo y dispara la función
2. Primera ejecución procesa el archivo y guarda timestamp
3. Ejecuciones subsecuentes leen timestamp y procesan solo archivos nuevos
4. Si múltiples archivos están en la misma carpeta, se procesan todos en una ejecución

### Caso 2: Primera Ejecución

**Escenario:** Primera vez que se ejecuta para una planta-secadora.

**Comportamiento:**
1. No existe timestamp → procesa TODOS los archivos en la carpeta
2. Guarda timestamp con fecha/hora actual
3. Próximas ejecuciones solo procesarán archivos nuevos

### Caso 3: Error en un Archivo

**Escenario:** Un archivo tiene formato incorrecto o está corrupto.

**Comportamiento:**
1. Detecta error al procesar el archivo
2. Registra error en logs y respuesta JSON
3. Continúa procesando los demás archivos
4. Timestamp se actualiza con archivos procesados exitosamente

### Caso 4: Generación de Reporte

**Escenario:** Usuario solicita reporte desde interfaz de Apps Script.

**Comportamiento:**
1. Apps Script llama a `compilador_trigger` con parámetro `planta`
2. `compilador_trigger` consolida todos los CSV validados en `df_historico.csv`
3. Al completarse exitosamente, Apps Script llama a `reporte_trigger` con parámetro `planta`
4. `reporte_trigger` genera reporte HTML con métricas y gráficos
5. Apps Script descarga el reporte y envía correo con PDF adjunto

---

## 📝 Logging y Monitoreo

### Niveles de Logging

- **INFO**: Operaciones normales (descarga, procesamiento, subida)
- **WARNING**: Situaciones no críticas (archivo de laboratorio no encontrado)
- **ERROR**: Errores que no detienen el procesamiento
- **EXCEPTION**: Errores críticos con stack trace

### Información Registrada

- Metadata de archivos recibidos
- Timestamps de última ejecución
- Archivos procesados y sus resultados
- Errores y excepciones
- Estadísticas de procesamiento (registros procesados, cruzados, etc.)

---

## 🚀 Próximos Pasos y Mejoras

### Funcionalidades Implementadas

- [x] ETL Trigger: Procesamiento completo con calibración y cruce con laboratorio
- [x] ML Trigger: Clasificación y validación de datos con Machine Learning
- [x] Compilador Trigger: Consolidación de archivos validados en dataset histórico
- [x] Reporte Trigger: Generación de reportes HTML con métricas y gráficos
- [x] Sistema de calibración: Conversión de voltajes a valores reales
- [x] Interfaz Google Apps Script: Carga de archivos y trigger de ETL
- [x] Interfaz Google Apps Script: Solicitud y distribución de reportes
- [x] Sistema incremental de timestamps: Evita reprocesamiento

### Funcionalidades en Desarrollo

- [ ] Notificaciones por email en caso de errores
- [ ] Integración con Power BI
- [ ] Dashboard de monitoreo en tiempo real
- [ ] API REST para consulta de datos consolidados

### Mejoras Potenciales

- [ ] Procesamiento paralelo de archivos
- [ ] Compresión de archivos procesados
- [ ] Cache de archivos de laboratorio y calibración
- [ ] Retry automático en caso de errores temporales
- [ ] Métricas y telemetría detallada
- [ ] Versionado de modelos ML
- [ ] Reportes programados automáticos

---

## 📚 Referencias

- [Guía de Instalación](GUIA_INSTALACION.md)
- [Configuración de Google Cloud](CONFIGURACION_GOOGLE_CLOUD.md)
- [Configuración de Azure](CONFIGURACION_AZURE.md)
- [Troubleshooting](TROUBLESHOOTING_AUTENTICACION.md)
- [Automatización de Deployment](AUTOMATIZACION_DEPLOYMENT.md)

---

**Última actualización:** Diciembre 2025  
**Versión del sistema:** 4.0 (ETL + ML + Compilador Histórico + Reportes - Arquitectura completa automatizada)

