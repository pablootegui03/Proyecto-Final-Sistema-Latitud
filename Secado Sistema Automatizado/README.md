# Sistema de Secado de Arroz

Sistema automatizado end-to-end para procesamiento, validación y reporte de datos de secado de arroz desde múltiples plantas.

## Características

- ✅ Procesamiento ETL completo de datos de sensores
- ✅ Validación automática con Machine Learning
- ✅ Consolidación de dataset histórico
- ✅ Generación automática de reportes HTML
- ✅ Procesamiento incremental (evita reprocesamiento)
- ✅ Soporte multi-planta (JPV, Río Branco)
- ✅ Serverless en Azure Functions (costo $0)

## Requisitos

- Python 3.11+
- Azure Functions CLI (para desarrollo local)
- Google Cloud Service Account con acceso a Google Drive
- Dependencias en `requirements.txt`

## Instalación Rápida

1. **Clonar repositorio:**

```bash
git clone https://github.com/tu-usuario/secado-arroz.git
cd secado-arroz
```

2. **Instalar dependencias:**

```bash
pip install -r requirements.txt
```

3. **Configurar credenciales:**

   - Crear `local.settings.json` (ver [INSTALACION.md](INSTALACION.md))
   - Configurar Google Service Account (ver [CONFIGURACION.md](CONFIGURACION.md))

4. **Desplegar en Azure:**

```bash
func azure functionapp publish <nombre-function-app>
```

## Estructura del Proyecto

```
Secado Sistema Base/
├── shared_code/          # Módulos compartidos
├── etl_trigger/          # Procesamiento ETL
├── ml_trigger/           # Validación ML
├── compilador_trigger/   # Consolidación histórica
├── reporte_trigger/      # Generación reportes
├── requirements.txt      # Dependencias Python
└── README.md            # Este archivo
```

## Flujo de Datos

1. **Carga**: Operarios cargan archivos de sensores via Google Apps Script
2. **ETL**: `etl_trigger` procesa y cruza datos con laboratorio
3. **ML**: `ml_trigger` valida y clasifica datos
4. **Consolidación**: `compilador_trigger` crea dataset histórico
5. **Reporte**: `reporte_trigger` genera HTML con métricas

Para más detalles: ver [ARQUITECTURA.md](ARQUITECTURA.md)

## Documentación

- [ARQUITECTURA.md](ARQUITECTURA.md) - Diseño completo del sistema
- [INSTALACION.md](INSTALACION.md) - Pasos de instalación
- [CONFIGURACION.md](CONFIGURACION.md) - Variables de entorno y secretos
- [DEPLOYMENT.md](DEPLOYMENT.md) - Despliegue en Azure

## Soporte

Para problemas o preguntas, abrir un issue en GitHub.

## Licencia

Propietario - Latitud (2025)

