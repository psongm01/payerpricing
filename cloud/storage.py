"""Storage helpers for monthly refresh artifacts.

The same interface supports local directories for dry runs and Azure Blob or
ADLS Gen2 URLs through the Azure Python SDK for production runs.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from urllib.parse import urlsplit

log = logging.getLogger(__name__)


class StorageError(RuntimeError):
    """Raised when a storage operation fails."""


class StorageClient:
    def __init__(self, root: str, dry_run: bool = False):
        self.root = root.rstrip("/")
        self.dry_run = dry_run
        self.is_remote = self.root.startswith(("http://", "https://", "abfs://", "abfss://"))
        self._container_client = None
        self._base_prefix = ""
        if self.is_remote and not self.dry_run:
            self._container_client, self._base_prefix = self._make_container_client()

    def uri(self, key: str) -> str:
        key = key.strip("/")
        if self.is_remote:
            blob_name = self._blob_name(key)
            return f"{self.root}/{blob_name}" if blob_name else self.root
        return str(Path(self.root) / key)

    def upload_file(self, local_path: Path, key: str, overwrite: bool = True) -> None:
        destination = self.uri(key)
        if self.dry_run:
            log.info("[dry-run] upload %s -> %s", local_path, destination)
            return
        if self.is_remote:
            blob_name = self._blob_name(key)
            log.info("Uploading %s -> %s", local_path, destination)
            with local_path.open("rb") as fh:
                self._container_client.upload_blob(
                    name=blob_name,
                    data=fh,
                    overwrite=overwrite,
                )
            return
        dest_path = Path(destination)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if dest_path.exists() and not overwrite:
            raise StorageError(f"Destination exists and overwrite is disabled: {dest_path}")
        shutil.copy2(local_path, dest_path)

    def download_file(self, key: str, local_path: Path, overwrite: bool = True) -> None:
        source = self.uri(key)
        if self.dry_run:
            log.info("[dry-run] download %s -> %s", source, local_path)
            return
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if self.is_remote:
            blob_name = self._blob_name(key)
            if local_path.exists() and not overwrite:
                raise StorageError(f"Local path exists and overwrite is disabled: {local_path}")
            log.info("Downloading %s -> %s", source, local_path)
            stream = self._container_client.download_blob(blob_name)
            with local_path.open("wb") as fh:
                stream.readinto(fh)
            return
        source_path = Path(source)
        if local_path.exists() and not overwrite:
            raise StorageError(f"Local path exists and overwrite is disabled: {local_path}")
        shutil.copy2(source_path, local_path)

    def exists(self, key: str) -> bool:
        if self.dry_run:
            return False
        if self.is_remote:
            from azure.core.exceptions import ResourceNotFoundError

            try:
                self._container_client.get_blob_client(self._blob_name(key)).get_blob_properties()
                return True
            except ResourceNotFoundError:
                return False
        return Path(self.uri(key)).exists()

    def upload_prefix(self, local_dir: Path, key_prefix: str, overwrite: bool = True) -> None:
        if not local_dir.exists():
            log.info("Skipping missing upload directory: %s", local_dir)
            return
        if self.dry_run:
            log.info("[dry-run] upload dir %s -> %s", local_dir, self.uri(key_prefix))
            return
        if self.is_remote:
            for path in local_dir.rglob("*"):
                if path.is_file():
                    relative = path.relative_to(local_dir).as_posix()
                    self.upload_file(path, f"{key_prefix.strip('/')}/{relative}", overwrite=overwrite)
            return
        dest_root = Path(self.uri(key_prefix))
        for path in local_dir.rglob("*"):
            if path.is_file():
                dest = dest_root / path.relative_to(local_dir)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, dest)

    def download_prefix(self, key_prefix: str, local_dir: Path, overwrite: bool = True) -> None:
        if self.dry_run:
            log.info("[dry-run] download dir %s -> %s", self.uri(key_prefix), local_dir)
            return
        local_dir.mkdir(parents=True, exist_ok=True)
        if self.is_remote:
            prefix = self._blob_name(key_prefix).rstrip("/")
            if prefix:
                prefix += "/"
            for blob in self._container_client.list_blobs(name_starts_with=prefix):
                relative = blob.name[len(prefix):]
                if not relative:
                    continue
                destination = local_dir / relative
                if destination.exists() and not overwrite:
                    raise StorageError(f"Local path exists and overwrite is disabled: {destination}")
                destination.parent.mkdir(parents=True, exist_ok=True)
                log.info("Downloading %s -> %s", blob.name, destination)
                stream = self._container_client.download_blob(blob.name)
                with destination.open("wb") as fh:
                    stream.readinto(fh)
            return
        source_root = Path(self.uri(key_prefix))
        if not source_root.exists():
            raise StorageError(f"Storage prefix does not exist: {source_root}")
        for path in source_root.rglob("*"):
            if path.is_file():
                dest = local_dir / path.relative_to(source_root)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, dest)

    def write_text(self, key: str, text: str, scratch_dir: Path) -> None:
        scratch_dir.mkdir(parents=True, exist_ok=True)
        temp = scratch_dir / Path(key).name
        temp.write_text(text, encoding="utf-8")
        self.upload_file(temp, key, overwrite=True)

    def _make_container_client(self):
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:  # pragma: no cover - environment setup issue
            raise StorageError(
                "Azure upload requires azure-identity and azure-storage-blob. "
                "Install requirements.txt on the worker VM."
            ) from exc

        parts = urlsplit(self.root)
        if parts.scheme in {"abfs", "abfss"}:
            account_host = parts.netloc.split("@")[-1]
            filesystem = parts.netloc.split("@")[0] if "@" in parts.netloc else ""
            path_parts = [part for part in parts.path.split("/") if part]
            container_name = filesystem or (path_parts[0] if path_parts else "")
            base_prefix = "/".join(path_parts[1:] if filesystem else path_parts[1:])
            account_url = f"https://{account_host.replace('.dfs.', '.blob.')}"
        else:
            path_parts = [part for part in parts.path.split("/") if part]
            if not path_parts:
                raise StorageError(f"Remote storage root must include a container/filesystem: {self.root}")
            container_name = path_parts[0]
            base_prefix = "/".join(path_parts[1:])
            account_url = f"{parts.scheme}://{parts.netloc.replace('.dfs.', '.blob.')}"

        if not container_name:
            raise StorageError(f"Could not parse container/filesystem from storage root: {self.root}")

        credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
        service_client = BlobServiceClient(account_url=account_url, credential=credential)
        return service_client.get_container_client(container_name), base_prefix

    def _blob_name(self, key: str) -> str:
        key = key.strip("/")
        if not self._base_prefix:
            return key
        return f"{self._base_prefix}/{key}" if key else self._base_prefix
