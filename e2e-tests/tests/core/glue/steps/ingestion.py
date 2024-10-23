from collections import defaultdict
from functools import lru_cache
import hashlib
import logging
from pathlib import Path
from typing import Callable
from pytest_bdd import given, parsers, then

from core.glue.common.storage_api import DataLakeClient
from core.glue.common.utils import parse_str_table, prefix_path


@given(
    parsers.parse(
        'the contents of the "{origin_folder}" folder have been uploaded to the [{container}] "{directory}" folder'
    ),
)
def upload_local_folder_to_remote(
    origin_folder: str,
    container: str,
    directory: str,
    testdata_folder: Path,
    data_lake_clients: Callable[[str], DataLakeClient],
    testing_prefix: str,
):
    local_folder_to_upload = testdata_folder / origin_folder
    client = data_lake_clients(container)
    for file in local_folder_to_upload.rglob("*"):
        if file.is_file():
            path_relative_to_root = str(file.relative_to(local_folder_to_upload))
            client.upload_file(
                str(file),
                f"{prefix_path(directory, testing_prefix)}/{path_relative_to_root}",
            )
            logging.info(
                f"uploaded to: {prefix_path(directory, testing_prefix)}/{path_relative_to_root}"
            )


@then(
    parsers.parse(
        'the following files should be present in a subdirectory of the [{container}] "{directory}" folder:\n{attr_table}'
    )
)
def check_files_in_remote_folder(
    container: str,
    directory: str,
    attr_table: str,
    data_lake_clients: Callable[[str], DataLakeClient],
    tmp_path: Path,
    testing_prefix: str,
    testdata_folder: str,
):
    directory = prefix_path(directory, testing_prefix)
    files_to_check = [testdata_folder / row[0] for row in parse_str_table(attr_table)]
    client = data_lake_clients(container)
    client.download_directory_to_local_directory(directory, str(tmp_path))

    @lru_cache()
    def get_md5_from_path(local_path: Path):
        logging.debug(f"{local_path}")
        with open(local_path, "rb") as f:
            hash = hashlib.md5(f.read()).hexdigest()
            logging.debug(hash)
            return hash

    for file in files_to_check:
        assert any(
            file.name == downloaded_file.name
            and get_md5_from_path(file) == get_md5_from_path(downloaded_file)
            for downloaded_file in tmp_path.rglob("*")
            if downloaded_file.is_file()
        ), (
            f"No remote files in folder or subfolders of {container}/{directory} match {file}\n"
            f"Found Files:\n{[str(f.relative_to(tmp_path)) for f in tmp_path.rglob('*') if f.is_file()]}"
        )
