"""Cliente de Google Drive con autenticación OAuth 2.0 o Service Account."""

from __future__ import annotations

import io
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from google.auth.transport.requests import Request
from google.oauth2 import credentials as user_credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

logger = logging.getLogger(__name__)


class GoogleDriveClient:
    """Cliente de Google Drive enfocado en operaciones simples por path."""

    SCOPES = ["https://www.googleapis.com/auth/drive"]

    def __init__(self, config: Optional[Any] = None) -> None:
        if config is None:
            from shared_code.config_loader import get_config

            config = get_config()

        self.config = config
        self.base_path = (config.get("gdrive.base_path") or "").strip("/")
        self.root_folder_id = config.get("gdrive.root_folder_id") or "root"

        self._credentials = None
        self._drive_service = None

        self._initialize_credentials()

    def _initialize_credentials(self) -> None:
        """Inicializa autenticación priorizando OAuth 2.0, fallback a Service Account."""
        user_token_json = os.environ.get("GOOGLE_USER_TOKEN_JSON")
        if user_token_json:
            try:
                token_info = json.loads(user_token_json)
                logger.info("[Auth] Intentando autenticación con OAuth 2.0 (Token de Usuario)")
                creds = user_credentials.Credentials.from_authorized_user_info(
                    token_info, scopes=self.SCOPES
                )
                if creds.expired and creds.refresh_token:
                    logger.info("[Auth] Token expirado, refrescando...")
                    try:
                        creds.refresh(Request())
                        logger.info("[Auth] Token refrescado exitosamente")
                    except Exception as refresh_error:
                        logger.error(
                            "[Auth] Error al refrescar token: %s. "
                            "Verifica que el refresh_token sea válido.",
                            refresh_error
                        )
                        raise ValueError(
                            f"Error al refrescar token OAuth 2.0: {refresh_error}. "
                            "Verifica que GOOGLE_USER_TOKEN_JSON contenga un refresh_token válido."
                        ) from refresh_error
                self._credentials = creds
                logger.info("[Auth] ✓ Autenticación con Token de Usuario (OAuth 2.0) exitosa")
                return
            except json.JSONDecodeError as e:
                logger.warning(
                    "[Auth] GOOGLE_USER_TOKEN_JSON contiene JSON inválido: %s. "
                    "Intentando Service Account como fallback...",
                    e
                )
            except Exception as e:
                logger.warning(
                    "[Auth] Error inicializando OAuth 2.0: %s. "
                    "Intentando Service Account como fallback...",
                    e
                )
        
        logger.info("[Auth] Usando Service Account como método de autenticación")
        service_account_info = None
        source = None

        env_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if env_json:
            try:
                service_account_info = json.loads(env_json)
                source = "variable de entorno GOOGLE_SERVICE_ACCOUNT_JSON"
            except json.JSONDecodeError as e:
                logger.warning(
                    "[Auth] GOOGLE_SERVICE_ACCOUNT_JSON contiene JSON inválido: %s. "
                    "Intentando archivo...",
                    e
                )

        if service_account_info is None:
            project_root = Path(__file__).parent.parent
            json_file_paths = [
                project_root / "service-account-key.json",
                Path("service-account-key.json"),
                Path.cwd() / "service-account-key.json",
            ]
            
            for json_path in json_file_paths:
                if json_path.exists():
                    try:
                        with open(json_path, "r", encoding="utf-8") as f:
                            service_account_info = json.load(f)
                        source = f"archivo {json_path}"
                        break
                    except (json.JSONDecodeError, IOError) as e:
                        logger.warning(
                            "[Auth] No se pudo leer %s: %s. Intentando siguiente ubicación...",
                            json_path,
                            e,
                        )
                        continue

        if service_account_info is None:
            raise ValueError(
                "No se encontraron credenciales válidas. "
                "Configura GOOGLE_SERVICE_ACCOUNT_JSON o service-account-key.json"
            )

        try:
            self._credentials = service_account.Credentials.from_service_account_info(
                service_account_info, scopes=self.SCOPES
            )
            logger.info(
                "[Auth] ✓ Autenticación con Service Account exitosa (cargada desde %s)",
                source,
            )
        except Exception as e:
            raise ValueError(
                f"Error al inicializar credenciales de Service Account desde {source}: {e}\n"
                "Verifica que el JSON sea válido y tenga el formato correcto."
            ) from e

    def _get_service(self):
        if self._drive_service is None:
            if self._credentials is None:
                raise RuntimeError(
                    "Credenciales no inicializadas. Llama a _initialize_credentials() primero."
                )
            self._drive_service = build(
                "drive", "v3", credentials=self._credentials, cache_discovery=False
            )
        return self._drive_service

    @staticmethod
    def _split_path(path: str) -> List[str]:
        return [segment for segment in path.strip("/").split("/") if segment]

    @staticmethod
    def _folder_mime() -> str:
        return "application/vnd.google-apps.folder"

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("'", "\\'")

    def _find_item(
        self,
        name: str,
        parent_id: str,
        mime_type: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        service = self._get_service()
        conditions = [
            "trashed = false",
            f"name = '{self._escape(name)}'",
            f"'{parent_id}' in parents",
        ]
        if mime_type:
            conditions.append(f"mimeType = '{mime_type}'")
        query = " and ".join(conditions)
        result = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id, name, mimeType, parents, modifiedTime, size, webViewLink)",
                pageSize=1,
            )
            .execute()
        )
        files = result.get("files", [])
        return files[0] if files else None

    def _create_folder(self, name: str, parent_id: str) -> Dict[str, Any]:
        service = self._get_service()
        body = {
            "name": name,
            "mimeType": self._folder_mime(),
            "parents": [parent_id],
        }
        return service.files().create(body=body, fields="id, name, parents").execute()

    def _resolve_folder_id(self, path: str, create: bool = False) -> str:
        segments = self._split_path(path)
        parent_id = self.root_folder_id

        for segment in segments:
            existing = self._find_item(segment, parent_id, mime_type=self._folder_mime())
            if existing:
                parent_id = existing["id"]
                continue

            if not create:
                raise FileNotFoundError(f"No existe la carpeta '{segment}' dentro de '{path}'")

            created = self._create_folder(segment, parent_id)
            parent_id = created["id"]
        return parent_id

    def _resolve_file(self, path: str) -> Tuple[str, str, Dict[str, Any]]:
        segments = self._split_path(path)
        if not segments:
            raise ValueError("El path de archivo no puede ser vacío")

        *folder_segments, filename = segments
        folder_path = "/".join(folder_segments)
        parent_id = self._resolve_folder_id(folder_path, create=False) if folder_segments else self.root_folder_id

        existing = self._find_item(filename, parent_id)
        if not existing:
            raise FileNotFoundError(f"No se encontró el archivo '{path}' en Google Drive")

        return existing["id"], parent_id, existing

    def _ensure_parent(self, path: str) -> Tuple[str, str]:
        segments = self._split_path(path)
        if not segments:
            raise ValueError("El path no puede ser vacío")
        *folder_segments, filename = segments
        folder_path = "/".join(folder_segments)
        parent_id = (
            self._resolve_folder_id(folder_path, create=True)
            if folder_segments
            else self.root_folder_id
        )
        return filename, parent_id

    def list_files(self, folder_path: str) -> List[Dict[str, Any]]:
        """Lista los archivos dentro de una carpeta."""
        folder_path = folder_path.strip("/") or self.base_path
        folder_id = self._resolve_folder_id(folder_path, create=False)

        service = self._get_service()
        items: List[Dict[str, Any]] = []
        page_token: Optional[str] = None

        while True:
            result = (
                service.files()
                .list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    spaces="drive",
                    fields="nextPageToken, files(id, name, mimeType, size, modifiedTime, webViewLink)",
                    pageToken=page_token,
                    pageSize=1000,
                )
                .execute()
            )
            items.extend(result.get("files", []))
            page_token = result.get("nextPageToken")
            if not page_token:
                break
        return items

    def list_files_by_folder_id(self, folder_id: str, mime_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lista archivos directamente por folder ID (más eficiente).

        Args:
            folder_id: ID de la carpeta en Google Drive
            mime_type: Filtrar por tipo MIME (opcional, ej: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        Returns:
            Lista de archivos con metadata (id, name, mimeType, size, modifiedTime, webViewLink)
        """
        service = self._get_service()
        items: List[Dict[str, Any]] = []
        page_token: Optional[str] = None

        query = f"'{folder_id}' in parents and trashed=false"
        if mime_type:
            query += f" and mimeType='{mime_type}'"

        while True:
            result = (
                service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="nextPageToken, files(id, name, mimeType, size, modifiedTime, webViewLink)",
                    pageToken=page_token,
                    pageSize=1000,
                    orderBy="modifiedTime desc",
                )
                .execute()
            )
            items.extend(result.get("files", []))
            page_token = result.get("nextPageToken")
            if not page_token:
                break

        logger.info(f"[Drive] Encontrados {len(items)} archivos en folder {folder_id}")
        return items

    def download_file(self, file_path: str, file_id: Optional[str] = None) -> bytes:
        """
        Descarga un archivo y devuelve su contenido en bytes.
        
        Args:
            file_path: Path completo desde la raíz (ej: "Secado_Arroz/JPV/raw/sensor_1/archivo.txt")
            file_id: ID del archivo en Google Drive (opcional, más eficiente si está disponible)
        """
        if file_id:
            fid = file_id
        else:
            fid, _, _ = self._resolve_file(file_path)
        
        service = self._get_service()
        request = service.files().get_media(fileId=fid)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logger.debug("Descarga %.2f%%", status.progress() * 100)

        return buffer.getvalue()

    def upload_file(
        self,
        file_path: str,
        content: bytes,
        mime_type: str = "text/csv",
    ) -> Dict[str, Any]:
        """Sube (o actualiza) un archivo a Google Drive."""
        filename, parent_id = self._ensure_parent(file_path)
        service = self._get_service()

        media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime_type, resumable=False)
        file_metadata = {"name": filename, "parents": [parent_id]}

        existing = self._find_item(filename, parent_id)
        if existing:
            result = (
                service.files()
                .update(
                    fileId=existing["id"],
                    media_body=media,
                    fields="id, name, mimeType, size, modifiedTime, webViewLink",
                )
                .execute()
            )
            logger.info("Archivo '%s' actualizado en Google Drive", file_path)
            return result

        result = (
            service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id, name, mimeType, size, modifiedTime, webViewLink",
            )
            .execute()
        )
        logger.info("Archivo '%s' subido a Google Drive", file_path)
        return result

    def upload_file_to_folder(
        self,
        folder_id: str,
        file_name: str,
        content: bytes,
        mime_type: str = "text/csv",
    ) -> Dict[str, Any]:
        """Sube archivo directamente a carpeta compartida usando folder_id."""
        service = self._get_service()

        file_metadata = {
            "name": file_name,
            "parents": [folder_id],
        }

        media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime_type, resumable=True)

        try:
            file = (
                service.files()
                .create(
                    body=file_metadata,
                    media_body=media,
                    fields="id, name, mimeType, size, modifiedTime, webViewLink",
                )
                .execute()
            )

            logger.info(
                "[Drive] ✓ Archivo subido: %s (ID: %s) a folder %s",
                file_name,
                file.get("id"),
                folder_id,
            )
            return file

        except Exception as e:
            logger.error("[Drive] ✗ Error subiendo archivo %s: %s", file_name, str(e))
            raise

    def folder_exists(self, folder_path: str) -> bool:
        """Indica si la carpeta existe en Google Drive."""
        try:
            self._resolve_folder_id(folder_path.strip("/"), create=False)
            return True
        except FileNotFoundError:
            return False

    def ensure_folder(self, folder_path: str) -> str:
        """Crea (si no existe) la carpeta indicada y devuelve su ID."""
        return self._resolve_folder_id(folder_path.strip("/"), create=True)


__all__ = ["GoogleDriveClient"]


