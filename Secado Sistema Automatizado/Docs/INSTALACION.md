# Guía de Instalación

## Requisitos Previos

- Python 3.11 o superior
- Azure Functions Core Tools v4
- Cuenta de Google Cloud con Service Account creada
- Acceso a Azure Portal

## Paso 1: Clonar Repositorio

```bash
git clone https://github.com/tu-usuario/secado-arroz.git
cd secado-arroz
```

## Paso 2: Crear Entorno Virtual

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o
venv\Scripts\activate  # Windows
```

## Paso 3: Instalar Dependencias

```bash
pip install -r requirements.txt
```

## Paso 4: Configurar Google Service Account

1. Ir a [Google Cloud Console](https://console.cloud.google.com/)
2. IAM & Admin → Service Accounts
3. Crear nueva Service Account
4. Descargar archivo JSON de credenciales
5. Compartir carpetas de Google Drive con el email de la Service Account

## Paso 5: Crear local.settings.json

Crear archivo `local.settings.json` en la raíz del proyecto:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
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
}
```

**⚠️ IMPORTANTE**: Este archivo está en `.gitignore` y NO debe subirse a GitHub.

## Paso 6: Probar Localmente

```bash
func start
```

Las funciones estarán disponibles en:
- `http://localhost:7071/api/etl_trigger`
- `http://localhost:7071/api/modelo`
- `http://localhost:7071/api/compilador`
- `http://localhost:7071/api/reporte`

## Paso 7: Desplegar en Azure

Ver [DEPLOYMENT.md](DEPLOYMENT.md) para instrucciones detalladas.

## Troubleshooting

### Error: "No se encontraron credenciales válidas"

- Verificar que `GOOGLE_SERVICE_ACCOUNT_JSON` esté configurado correctamente
- Verificar que el JSON sea válido (sin saltos de línea adicionales)

### Error: "No existe configuración de carpeta"

- Verificar que todas las variables de entorno de folder IDs estén configuradas
- Verificar que las carpetas estén compartidas con la Service Account

### Error: "Module not found"

- Verificar que todas las dependencias estén instaladas: `pip install -r requirements.txt`
- Verificar que el entorno virtual esté activado

