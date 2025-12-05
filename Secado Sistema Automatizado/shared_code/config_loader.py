"""Gestor de configuración del sistema."""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional

import yaml

logger = logging.getLogger(__name__)


class Config:
    """Gestor de configuración del sistema."""

    def __init__(self, config_path: Optional[str] = None) -> None:
        if config_path is None:
            possible_paths = [
                Path("config/config.yaml"),
                Path("../config/config.yaml"),
                Path.home() / ".secado-arroz" / "config.yaml",
            ]
            for p in possible_paths:
                if p.exists():
                    config_path = str(p)
                    break

        if config_path is None or not Path(config_path).exists():
            raise FileNotFoundError(
                "No se encontró config.yaml. Crea uno basándote en config.sample.yaml"
            )

        self._load_config(config_path)
        self._validate()

    def _load_config(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as f:
            self._config = yaml.safe_load(f) or {}

    def _validate(self) -> None:
        """Valida que estén todos los campos requeridos."""
        required = [
            ("google", "client_id"),
            ("google", "client_secret"),
            ("gdrive", "base_path"),
        ]

        missing = []
        for section, key in required:
            val = self.get(f"{section}.{key}")
            if not val or (isinstance(val, str) and val.startswith("YOUR_")):
                missing.append(f"{section}.{key}")

        if missing:
            raise ValueError(
                f"Faltan configurar estos campos en config.yaml: {missing}"
            )

    def get(self, key: str, default: Any = None) -> Any:
        """Obtiene valor usando notación de puntos."""
        keys = key.split(".")
        value: Any = self._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

    def get_planta_config(self, planta: str) -> Dict[str, Any]:
        """Obtiene configuración completa de una planta."""
        return self.get(f"plantas.{planta}", {})

    def get_all_plantas(self) -> list:
        """Obtiene lista de plantas habilitadas."""
        plantas = self.get("plantas", {}) or {}
        return [p for p, cfg in plantas.items() if cfg and cfg.get("enabled", False)]

    def get_sensor_path(self, planta: str, sensor_id: int, tipo: str = "raw") -> str:
        """Genera path para un sensor específico."""
        base = self.get(f"plantas.{planta}.paths.{tipo}")
        if not base:
            raise ValueError(f"No se encontró path para {planta}.{tipo}")
        if tipo == "raw":
            return f"{base}/sensor_{sensor_id}"
        return base

    def get_lab_path(self, planta: str) -> str:
        """Path del archivo de laboratorio (directorio)."""
        path = self.get(f"plantas.{planta}.paths.laboratorio")
        if not path:
            raise ValueError(f"No se encontró path de laboratorio para {planta}")
        return path

    def to_env_dict(self) -> Dict[str, str]:
        """Convierte la configuración a variables de entorno."""
        return {
            "GOOGLE_CLIENT_ID": str(self.get("google.client_id")),
            "GOOGLE_CLIENT_SECRET": str(self.get("google.client_secret")),
            "GDRIVE_BASE_PATH": str(self.get("gdrive.base_path")),
            "GDRIVE_ROOT_FOLDER_ID": str(self.get("gdrive.root_folder_id", "")),
            "POWERBI_DATASET_ID": str(self.get("powerbi.dataset_id", "")),
            "NOTIFICATION_EMAIL": str(self.get("notifications.email", "")),
        }


_config_instance: Optional[Config] = None


def get_config(reload: bool = False) -> Config:
    """Obtiene instancia global de configuración."""
    global _config_instance
    if _config_instance is None or reload:
        _config_instance = Config()
    return _config_instance
