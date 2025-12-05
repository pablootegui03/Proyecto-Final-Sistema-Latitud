# Configuración

## Variables de Entorno Requeridas

Todas las variables deben configurarse en Azure Function App Settings.

### Autenticación Google

**`GOOGLE_SERVICE_ACCOUNT_JSON`** (requerido)
- Contenido completo del archivo JSON de Service Account
- Formato: String JSON (sin saltos de línea adicionales)
- Ejemplo: `{"type": "service_account", "project_id": "...", ...}`

### Folder IDs de Google Drive

**Laboratorio:**
- `LAB_FOLDER_JPV`: ID de carpeta de archivos de laboratorio para JPV
- `LAB_FOLDER_RB`: ID de carpeta de archivos de laboratorio para RB

**Procesados:**
- `PROCESSED_FOLDER_JPV`: ID de carpeta de archivos procesados para JPV
- `PROCESSED_FOLDER_RB`: ID de carpeta de archivos procesados para RB

**Validados:**
- `VALIDATED_FOLDER_JPV`: ID de carpeta de archivos validados para JPV
- `VALIDATED_FOLDER_RB`: ID de carpeta de archivos validados para RB

**Reportes:**
- `REPORTS_FOLDER_JPV`: ID de carpeta de reportes para JPV
- `REPORTS_FOLDER_RB`: ID de carpeta de reportes para RB

## Cómo Obtener Folder IDs

1. Abrir carpeta en Google Drive
2. URL será: `https://drive.google.com/drive/folders/FOLDER_ID`
3. Copiar `FOLDER_ID` de la URL

## Configurar en Azure Portal

1. Ir a Azure Portal → Function App → Configuration
2. Application settings → New application setting
3. Agregar cada variable con su valor
4. Guardar cambios

## Verificar Configuración

Las funciones validan automáticamente que todas las variables estén configuradas. Si falta alguna, se mostrará un error descriptivo indicando cuál falta.

## Seguridad

- ✅ Nunca subir `local.settings.json` a GitHub
- ✅ Usar Azure Key Vault para secretos en producción (opcional)
- ✅ Service Account con permisos mínimos necesarios
- ✅ Compartir solo las carpetas necesarias con la Service Account

