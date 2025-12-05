"""
Gestor de timestamps para ETL incremental.

Guarda y lee timestamps de última ejecución en Google Drive.
Permite procesar solo archivos nuevos desde la última ejecución.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class TimestampManager:
    """Gestiona timestamps de última ejecución del ETL."""

    def __init__(self, drive_client):
        """
        Args:
            drive_client: Instancia de GoogleDriveClient
        """
        self.drive_client = drive_client
        self.timestamp_folder = "etl_timestamps"  # Carpeta en Drive para timestamps

    def _get_timestamp_filename(self, planta: str, secadora: str) -> str:
        """Genera nombre de archivo de timestamp."""
        # Sanitizar nombre (remover espacios y caracteres especiales)
        planta_clean = planta.replace(" ", "_").replace("/", "_")
        secadora_clean = secadora.replace(" ", "_").replace("/", "_")
        return f"last_run_timestamp_{planta_clean}_{secadora_clean}.json"

    def _get_timestamp_path(self, planta: str, secadora: str) -> str:
        """Genera path completo del archivo de timestamp."""
        filename = self._get_timestamp_filename(planta, secadora)
        return f"{self.timestamp_folder}/{filename}"

    def get_last_run_timestamp(self, planta: str, secadora: str) -> Optional[datetime]:
        """
        Obtiene el timestamp de última ejecución.

        Returns:
            datetime de última ejecución, o None si es la primera vez
        """
        try:
            timestamp_path = self._get_timestamp_path(planta, secadora)

            # Intentar leer el archivo de timestamp
            content = self.drive_client.download_file(timestamp_path)
            data = json.loads(content.decode("utf-8"))

            last_run = data.get("last_run")
            if last_run:
                # Parsear timestamp ISO 8601
                # Manejar formato con 'Z' o con timezone
                if last_run.endswith("Z"):
                    last_run = last_run[:-1] + "+00:00"
                elif "+" not in last_run and "-" in last_run[-6:]:
                    # Ya tiene timezone
                    pass
                else:
                    # Asumir UTC si no tiene timezone
                    last_run = last_run + "+00:00"

                dt = datetime.fromisoformat(last_run)
                logger.info(
                    "Timestamp encontrado para %s - %s: %s",
                    planta,
                    secadora,
                    dt.isoformat(),
                )
                return dt

        except FileNotFoundError:
            logger.info(
                "No existe timestamp para %s - %s (primera ejecución)",
                planta,
                secadora,
            )
        except Exception as e:
            logger.warning(
                "Error leyendo timestamp para %s - %s: %s",
                planta,
                secadora,
                e,
            )

        return None

    def update_timestamp(
        self,
        planta: str,
        secadora: str,
        processed_files: List[Dict],
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Actualiza el timestamp de última ejecución.

        Args:
            planta: Nombre de la planta
            secadora: Nombre de la secadora
            processed_files: Lista de archivos procesados con su info
            timestamp: Timestamp a guardar (default: ahora UTC)
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        timestamp_data = {
            "planta": planta,
            "secadora": secadora,
            "last_run": timestamp.isoformat().replace("+00:00", "Z"),
            "last_processed_files": processed_files[-10:],  # Últimos 10 archivos
            "total_files_processed": len(processed_files),
            "last_updated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

        try:
            # Asegurar que existe la carpeta de timestamps
            self.drive_client.ensure_folder(self.timestamp_folder)

            # Guardar archivo JSON
            timestamp_path = self._get_timestamp_path(planta, secadora)
            content = json.dumps(timestamp_data, indent=2).encode("utf-8")

            self.drive_client.upload_file(
                timestamp_path, content, mime_type="application/json"
            )

            logger.info(
                "Timestamp actualizado para %s - %s: %s",
                planta,
                secadora,
                timestamp.isoformat(),
            )

        except Exception as e:
            logger.error(
                "Error guardando timestamp para %s - %s: %s",
                planta,
                secadora,
                e,
            )
            raise


__all__ = ["TimestampManager"]

