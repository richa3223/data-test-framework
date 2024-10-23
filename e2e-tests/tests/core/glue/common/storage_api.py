from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import FileSystemClient, DataLakeFileClient
from azure.core.exceptions import ServiceRequestError

from io import BytesIO
from pathlib import Path
import logging

from core.glue.common.utils import RetryBackoffIterator


class DataLakeClient:
    def __init__(
        self,
        storage_account: str,
        file_system: str,
        credential: DefaultAzureCredential = None,
    ) -> None:
        self._storage_account = storage_account
        self._file_system = file_system
        self._credential = credential
        self._file_system_client = self._get_file_system_client(
            self._storage_account, self._file_system
        )

    def _get_file_system_client(
        self, storage_account: str, file_system: str
    ) -> FileSystemClient:
        url = f"https://{storage_account}.dfs.core.windows.net"
        if not self._credential:
            self._credential = DefaultAzureCredential()
        return FileSystemClient(url, file_system, self._credential)

    def as_abfss_path(self, path: str):
        path = path[1:] if path.startswith("/") else path
        return f"abfss://{self._file_system}@{self._storage_account}.dfs.core.windows.net/{path}"

    # ------------------------------ DIRECTORY METHODS -------------------------------

    def directory_exists(self, directory: str) -> bool:
        return self._file_system_client.get_directory_client(directory).exists()

    def list_paths_recursive(self, directory: str = None) -> list:
        return self._file_system_client.get_paths(directory, recursive=True, timeout=60)

    def list_paths(
        self, directory: str = None, files_only: bool = False, raise_exception=True
    ) -> list:
        try:
            if not files_only:
                return [
                    path
                    for path in self._file_system_client.get_paths(
                        directory, timeout=60
                    )
                ]
            return [
                path
                for path in self._file_system_client.get_paths(directory, timeout=60)
                if not path.is_directory
            ]
        except ResourceNotFoundError:
            if raise_exception is True:
                logging.error("Directory not found.")
                raise
        except Exception as e:
            logging.critical(str(e))
            logging.critical(repr(e))
            if raise_exception == True:
                logging.error("Directory not found.")
                raise Exception("Directory not found.")
            return []

    def get_files_with_extension(self, directory: str, extension: str) -> list:
        return [
            path.name
            for path in self._file_system_client.get_paths(directory, timeout=60)
            if Path(path.name).suffix == f"{extension}"
        ]

    def create_new_directory(self, directory: str):
        self._file_system_client.create_directory(directory, timeout=60)

    def delete_directory(self, directory: str, raise_exception=True):
        directory_client = self._file_system_client.get_directory_client(directory)
        logging.info(f"removing directory {self._file_system}/{directory}")
        try:
            directory_client.delete_directory(timeout=60)
            logging.info(f"removed directory {self._file_system}/{directory}")
        except ResourceNotFoundError:
            if raise_exception is True:
                logging.exception(
                    f"directory {self._file_system}/{directory} not found"
                )
                raise
            else:
                logging.info(f"directory {self._file_system}/{directory} not found")

    def rename_directory(self, directory: str, new_dir_name: str, create_parents=True):
        directory_client = self._file_system_client.get_directory_client(directory)
        if create_parents:
            self._file_system_client.create_directory(new_dir_name, timeout=60)
        try:
            directory_client.rename_directory(
                new_name=directory_client.file_system_name + "/" + new_dir_name,
                timeout=60,
            )
        except ResourceNotFoundError:
            logging.error("Source path not found.")
            raise

    def empty_directory(self, directory: str, raise_exception=False) -> None:
        if not self.directory_exists(directory):
            if raise_exception is True:
                logging.exception(
                    f"Directory {self._file_system}/{directory} not found"
                )
                raise Exception(f"Directory {self._file_system}/{directory} not found")
            logging.info(f"Directory {self._file_system}/{directory} not found")
            return
        paths = self.list_paths(directory)
        files = filter(lambda path: not path.is_directory, paths)
        directories = filter(lambda path: path.is_directory, paths)
        logging.info(
            f"Removing {len(paths)} objects from {self._file_system}/{directory}"
        )
        for file in files:
            self.delete_file(file.name)
        for file in directories:
            self.delete_directory(file.name)

    def is_directory_empty(self, directory: str) -> bool:
        if not self.directory_exists(directory):
            logging.exception(f"Directory {directory} not found")
            raise Exception(f"Directory {directory} not found")
        return len(self.list_paths(directory)) == 0

    # --------------------------------- FILE METHODS ----------------------------------

    def _get_file_client(self, blob_path: str) -> DataLakeFileClient:
        return self._file_system_client.get_file_client(blob_path)

    def check_file_exists(self, blob_path: str) -> bool:
        file_client = self._get_file_client(blob_path)
        try:
            file_client.get_file_properties(timeout=60)
            return True
        except ResourceNotFoundError:
            return False

    def download_file_to_stream(self, blob_path: str) -> BytesIO:
        file_client = self._get_file_client(blob_path)
        for download in RetryBackoffIterator(
            lambda: file_client.download_file(timeout=60), [], ServiceRequestError, 1, 6
        ):
            if download is not None:
                stream = BytesIO()
                download.readinto(stream)
                return stream

    def download_file_to_file(self, blob_path: str, local_path: Path):
        file_client = self._get_file_client(blob_path)
        for download in RetryBackoffIterator(
            lambda: file_client.download_file(timeout=60), [], ServiceRequestError, 1, 6
        ):
            if download is not None:
                with open(local_path, "wb") as f:
                    f.write(download.readall())

    def download_files_to_directory(
        self, azure_directory: str, local_directory: str
    ) -> None:
        files = [
            path.name
            for path in self.list_paths(azure_directory)
            if not path.is_directory
        ]

        for file in files:
            stream = self.download_file_to_stream(file)
            relative_path = Path(file).relative_to(azure_directory)
            local_file_location = Path(local_directory) / relative_path
            local_file_location.parent.mkdir(parents=True, exist_ok=True)
            with open(local_file_location, "wb") as f:
                f.write(stream.getbuffer())

    def download_directory_to_local_directory(
        self, azure_directory: str, local_directory: Path
    ) -> None:
        """TODO write a docstring

        Args:
            azure_directory (str): _description_
            local_directory (str): _description_
        """
        logging.info(
            f"Downloading remote directory {azure_directory} to {local_directory}"
        )
        files = [
            path.name
            for path in self.list_paths_recursive(azure_directory)
            if not path.is_directory
        ]

        for file in files:
            logging.info(f"Downloading file {self._file_system}/{file}")
            relative_path = Path(file).relative_to(azure_directory)
            local_file_location = local_directory / relative_path
            local_file_location.parent.mkdir(parents=True, exist_ok=True)
            self.download_file_to_file(file, local_file_location)

    def delete_file(self, blob_path: str, raise_exception=False) -> None:
        file_client = self._get_file_client(blob_path)
        logging.info(f"Deleting blob: {self._file_system}/{blob_path}")

        try:
            file_client.delete_file(timeout=60)
            logging.info(f"Blob deleted: {self._file_system}/{blob_path}")
        except ResourceNotFoundError:
            if raise_exception is True:
                logging.exception(
                    f"Blob not found. Expected: {self._file_system}/{blob_path}"
                )
                raise
            else:
                logging.info(
                    f"Blob not found. Expected: {self._file_system}/{blob_path}"
                )

    def delete_files_in_directory(self, directory: str, raise_exception=True) -> None:
        files = [
            path.name
            for path in self.list_paths(directory, raise_exception=False)
            if not path.is_directory
        ]
        logging.info(
            f"Removing {len(files)} files from {self._file_system}/{directory}"
        )
        for file in files:
            if raise_exception == False:
                try:
                    self.delete_file(file)
                except:
                    return
            else:
                self.delete_file(file)

    def rename_file(self, old_blob_path: str, new_blob_path: str) -> None:
        file_client = self._get_file_client(old_blob_path)
        file_client.rename_file(f"{self._file_system}/" + new_blob_path, timeout=60)

    def get_last_modified_file_in_directory_path(self, blob_path: str) -> None:
        list_of_files = self.list_paths(blob_path, True)
        file_name = list_of_files[len(list_of_files) - 1].name
        return file_name

    def upload_file(
        self,
        local_file_path: str,
        blob_path: str,
        overwrite: bool = True,
        delete_local_file: bool = False,
    ) -> None:
        file_client = self._get_file_client(blob_path)

        if self.check_file_exists(blob_path) and not overwrite:
            raise ResourceExistsError(
                f"Blob already exists in location: {self._file_system}/{blob_path}"
            )
        logging.info(f"Uploading file {self._file_system}/{blob_path}")

        file_client.create_file(timeout=60)

        with open(local_file_path, "rb") as f:
            file_contents = f.read()
            file_client.upload_data(file_contents, overwrite=True, timeout=60)

        if delete_local_file:
            Path(local_file_path).unlink()

    def upload_directory_to_remote_directory(
        self, local_directory: Path, azure_directory: str
    ) -> None:
        """
        Useful for uploading data files for batch processing (e.g. creating multiple tables
        at once), or for uploading jsonl + schema files.

        args:
            local_directory: the path to root directory that you want to upload
            azure_directory: the path to the azure directory where you want to replicate the files
        """
        for path in local_directory.rglob("*"):
            if path.is_file():
                self.upload_file(
                    str(path),
                    str(azure_directory / path.relative_to(local_directory)),
                    overwrite=True,
                )
