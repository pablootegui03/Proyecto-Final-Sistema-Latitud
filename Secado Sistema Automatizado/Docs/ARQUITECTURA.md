# Arquitectura del Sistema

## Visión General

Sistema automatizado end-to-end que procesa datos de sensores de secado de arroz desde múltiples plantas, los valida con Machine Learning, consolida en un dataset histórico y genera reportes HTML.

## Diagrama de Flujo

```
Apps Script (Carga) → etl_trigger → ml_trigger → validated/
                                              ↓
Apps Script (Reporte) → compilador_trigger → df_historico.csv
                                              ↓
                                    reporte_trigger → reporte HTML
```

## Componentes Principales

| Componente | Función | Ubicación |
|------------|---------|-----------|
| **etl_trigger** | Procesamiento ETL de archivos RAW | `etl_trigger/` |
| **ml_trigger** | Validación con modelos ML | `ml_trigger/` |
| **compilador_trigger** | Consolidación histórica | `compilador_trigger/` |
| **reporte_trigger** | Generación de reportes HTML | `reporte_trigger/` |
| **GoogleDriveClient** | Cliente de Google Drive API | `shared_code/gdrive_client.py` |
| **TimestampManager** | Gestión de timestamps incrementales | `shared_code/timestamp_manager.py` |
| **ETL Core** | Procesamiento de archivos de sensores | `shared_code/etl_core.py` |
| **Lab Crosser** | Cruce con datos de laboratorio | `shared_code/lab_crosser.py` |
| **Calibración** | Aplicación de curvas de calibración | `shared_code/calibracion.py` |
| **ML Predictor** | Modelos de Machine Learning | `shared_code/ml_predictor.py` |
| **Compilador Histórico** | Consolidación de archivos validados | `shared_code/compilador_historico.py` |
| **Reporte Builder** | Generación de reportes HTML | `shared_code/reporte_builder.py` |

## Flujo de Datos de Alto Nivel

### 1. Carga de Datos
- Operarios cargan archivos de sensores via Google Apps Script
- Archivos se suben a `raw/{PLANTA}/{SENSOR}/`
- Apps Script hace POST a `etl_trigger` con metadata

### 2. Procesamiento ETL
- `etl_trigger` descarga archivos RAW
- Normaliza y transforma datos (JPV y RB unificados)
- Cruza con datos de laboratorio
- Aplica curvas de calibración
- Genera CSV procesado en `processed/`

### 3. Validación ML
- `ml_trigger` recibe solicitud para procesar archivo procesado
- Aplica modelos de Machine Learning
- Clasifica y valida datos
- Genera CSV validado en `validated/`

### 4. Consolidación Histórica
- `compilador_trigger` lee todos los CSV validados
- Concatena y elimina duplicados por `ID_tachada`
- Genera/actualiza `df_historico.csv` en `validated/`

### 5. Generación de Reportes
- `reporte_trigger` descarga `df_historico.csv`
- Calcula métricas estadísticas
- Genera gráficos embebidos (base64)
- Crea reporte HTML en `reportes/`

## Tecnologías Utilizadas

- **Python 3.11+**: Lenguaje principal
- **Azure Functions**: Plataforma serverless (Consumption Plan)
- **Azure Storage Account**: Blob Storage para artefactos
- **Google Drive API**: Almacenamiento y acceso a archivos
- **Pandas**: Procesamiento de datos
- **Google Service Account**: Autenticación sin interacción
- **Matplotlib/Seaborn**: Generación de gráficos
- **CatBoost**: Modelos de Machine Learning

## Estructura de Carpetas en Google Drive

```
Secado_Arroz/
├── JPV/
│   ├── raw/              # Archivos RAW de sensores
│   ├── processed/        # Archivos procesados (ETL)
│   ├── validated/        # Archivos validados (ML) + df_historico.csv
│   └── reportes/         # Reportes HTML
├── RB/
│   └── (misma estructura)
└── etl_timestamps/       # Timestamps de última ejecución
```

## Infraestructura Azure

- **Function App**: Entorno Python 3.11, Consumption Plan
- **Storage Account**: Blob Storage para artefactos
- **4 Functions**: etl_trigger, ml_trigger, compilador_trigger, reporte_trigger
- **Costo**: $0 (dentro de niveles gratuitos)

