# Guía de Despliegue en Azure

## Prerrequisitos

- Azure CLI instalado
- Azure Functions Core Tools v4
- Suscripción de Azure activa
- Permisos para crear recursos en Azure

## Paso 1: Crear Azure Function App

```bash
# Login en Azure
az login

# Crear Resource Group
az group create --name rg-secado-arroz --location eastus

# Crear Storage Account
az storage account create \
  --name stsecadoarroz \
  --resource-group rg-secado-arroz \
  --location eastus \
  --sku Standard_LRS

# Crear Function App
az functionapp create \
  --name func-secado-arroz \
  --resource-group rg-secado-arroz \
  --storage-account stsecadoarroz \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4 \
  --consumption-plan-location eastus
```

## Paso 2: Configurar Variables de Entorno

```bash
# Configurar variables de entorno
az functionapp config appsettings set \
  --name func-secado-arroz \
  --resource-group rg-secado-arroz \
  --settings \
    GOOGLE_SERVICE_ACCOUNT_JSON="$(cat service-account-key.json)" \
    LAB_FOLDER_JPV="folder_id_lab_jpv" \
    LAB_FOLDER_RB="folder_id_lab_rb" \
    PROCESSED_FOLDER_JPV="folder_id_processed_jpv" \
    PROCESSED_FOLDER_RB="folder_id_processed_rb" \
    VALIDATED_FOLDER_JPV="folder_id_validated_jpv" \
    VALIDATED_FOLDER_RB="folder_id_validated_rb" \
    REPORTS_FOLDER_JPV="folder_id_reports_jpv" \
    REPORTS_FOLDER_RB="folder_id_reports_rb"
```

O configurar manualmente en Azure Portal:
1. Function App → Configuration → Application settings
2. Agregar cada variable con su valor
3. Guardar

## Paso 3: Desplegar Código

```bash
# Desde la raíz del proyecto
func azure functionapp publish func-secado-arroz
```

## Paso 4: Verificar Despliegue

1. Ir a Azure Portal → Function App → Functions
2. Verificar que las 4 funciones estén listadas:
   - `etl_trigger`
   - `ml_trigger` (ruta: `modelo`)
   - `compilador_trigger` (ruta: `compilador`)
   - `reporte_trigger` (ruta: `reporte`)

## Paso 5: Obtener Function Keys

```bash
# Obtener keys de cada función
az functionapp function keys list \
  --name func-secado-arroz \
  --resource-group rg-secado-arroz \
  --function-name etl_trigger
```

O desde Azure Portal:
1. Function App → Functions → [nombre función] → Function Keys
2. Copiar `default` key

## Paso 6: Probar Endpoints

```bash
# Probar etl_trigger
curl -X POST "https://func-secado-arroz.azurewebsites.net/api/etl_trigger?code=FUNCTION_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "planta": "JPV",
    "secadora": "Secadora 1",
    "fileId": "test_file_id",
    "fileName": "test.txt",
    "folderId": "test_folder_id"
  }'
```

## Ver Logs

```bash
# Ver logs en tiempo real
func azure functionapp logstream func-secado-arroz
```

O desde Azure Portal:
1. Function App → Log stream

## Troubleshooting

### Error: "Module not found"

- Verificar que `requirements.txt` esté actualizado
- Forzar reinstalación de dependencias:
  ```bash
  func azure functionapp publish func-secado-arroz --force
  ```

### Error: "Function timeout"

- Verificar `host.json` tiene `functionTimeout` configurado
- Aumentar timeout si es necesario (máx 10 minutos en Consumption Plan)

### Error: "Authentication failed"

- Verificar que `GOOGLE_SERVICE_ACCOUNT_JSON` esté configurado correctamente
- Verificar que las carpetas estén compartidas con la Service Account

## Actualizar Código

```bash
# Desplegar cambios
func azure functionapp publish func-secado-arroz
```

## Rollback

Si necesitas revertir a una versión anterior:

1. Azure Portal → Function App → Deployment Center
2. Ver historial de deployments
3. Seleccionar versión anterior y hacer rollback

