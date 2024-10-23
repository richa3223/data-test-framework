import base64
import json
import logging
import requests
import time

from collections import defaultdict
from enum import Enum, auto
from typing import Dict, Set, Tuple, Callable, List, Optional
from pathlib import Path
from uuid import uuid1, uuid4

import pytest
import pyspark.sql.types as st
import pyspark.sql.functions as sf
from pytest_bdd import given, parsers, when, then
from pyspark.sql import SparkSession, DataFrame

from core.glue.common.azure import is_running_as_sp, get_current_user_without_domain
from core.glue.common.base_api import BaseClient
from core.glue.common.storage_api import DataLakeClient
from core.glue.common.utils import parse_str_table, prefix_path, prefix_string
from core.glue.common.databricks_api import DatabricksClient


@pytest.fixture()
def testing_prefix(worker_id: str, prefix_paths_cleanup: Dict, cleanup_cosmos_databases: Dict) -> str:
    prefix = f"__test_{uuid1().hex}_{worker_id}_"
    logging.info(f"RUNNING WITH TESTING PREFIX: {prefix}")
    prefix_paths_cleanup["prefix"] = prefix
    cleanup_cosmos_databases["prefix"] = prefix
    return prefix


@pytest.fixture(scope="session")
def run_cleanup_routine():
    yield True


@pytest.fixture(scope="session", autouse=True)
def synapse_script_dir(
    credential,
    pytestconfig,
    management_token,
    worker_id,
    synapse_scripts_path,
    databricks_client: DatabricksClient,
):
    """Upload script files to unique directory. The directory will be unique per
    azure user, except for service principles for whom it will be entirely unique.
    For Service Principles, the folder is also cleaned up after the test run"""
    subscription = pytestconfig.getoption("subscription")
    synapse_resource_group = pytestconfig.getoption("resource_group")
    synapse_name = pytestconfig.getoption("workspace")
    unique_dir = (
        f"{uuid1().hex}_{worker_id}"
        if is_running_as_sp(subscription)
        else f"_test_scripts_{get_current_user_without_domain()}_{worker_id}"
    )
    unique_dir_on_datalake = f"tmp/{unique_dir}"
    unique_dir_on_databricks = f"_synergy_testing/{unique_dir}"
    logging.info(f"Setting up remote scripts folder {unique_dir}")
    url = (
        "https://management.azure.com"
        f"/subscriptions/{subscription}"
        f"/resourceGroups/{synapse_resource_group}"
        f"/providers/Microsoft.Synapse/workspaces/{synapse_name}"
        "?api-version=2019-06-01-preview"
    )
    headers = {"Authorization": f"Bearer {management_token()}"}
    response = requests.get(url, headers=headers)
    synapse_storage_url = response.json()["properties"]["defaultDataLakeStorage"][
        "accountUrl"
    ]
    data_lake_client = DataLakeClient(
        synapse_storage_url.split("/")[2].split(".")[0], "default", credential
    )

    data_lake_client.create_new_directory(unique_dir_on_datalake)

    # upload our scripts
    for file in synapse_scripts_path.glob("*.py"):
        data_lake_client.upload_file(str(file), f"{unique_dir_on_datalake}/{file.name}")
    databricks_client.upload_notebook_folder(
        synapse_scripts_path, unique_dir_on_databricks, overwrite=True
    )
    yield unique_dir
    if is_running_as_sp(subscription):
        logging.info("cleaning synapse_scripts folder")
        data_lake_client.delete_directory(unique_dir_on_datalake, raise_exception=False)


@pytest.fixture(scope="session")
def spark_object_cleanup(
    synapse_script_dir: str,
    synapse_storage_account: str,
    run_cleanup_routine: bool,
    subscription,
    databricks_client: DatabricksClient,
) -> Set[Tuple[str, str]]:
    cleanup_objects = set()
    logging.info("Set up new database cleanup context")
    yield cleanup_objects
    objects_to_delete = []
    for cleanup_object in cleanup_objects:
        object_to_delete = {"database": cleanup_object[0]}
        if cleanup_object[1]:
            object_to_delete["table"] = cleanup_object[1]
        objects_to_delete.append(object_to_delete)

    if run_cleanup_routine:
        delete_sql_objects(
            client=databricks_client,
            synapse_scripts_dir=synapse_script_dir,
            objects_to_delete=objects_to_delete,
            synapse_storage_account=synapse_storage_account,
            wait_for_completion=is_running_as_sp(subscription),
            subscription=subscription,
        )
    else:
        logging.critical(
            f"Cleanup routine not executed - remember to clean up your mess for: {testing_prefix}"
        )


@pytest.fixture(scope="session", autouse=True)
def prefix_paths_cleanup(
    data_lake_clients, run_cleanup_routine: bool
) -> Set[Tuple[str, str]]:
    cleanup_objects = {
        "cleanup_objects": {"default", "ingestion", "raw", "curated", "certified"},
        "prefix": None,
    }
    logging.info("Set up new prefix path cleanup context")
    yield cleanup_objects
    if not cleanup_objects["prefix"]:
        logging.critical("No prefix")
        return

    if not run_cleanup_routine:
        logging.critical(
            f"Cleanup routine not executed - remember to clean up your mess for: {cleanup_objects['prefix']}"
        )

    logging.info(
        f"Cleaning up prefix_path directories in containers: {cleanup_objects}"
    )
    for container in cleanup_objects["cleanup_objects"]:
        client: DataLakeClient = data_lake_clients(container)
        client.delete_directory(cleanup_objects["prefix"], raise_exception=False)


@pytest.fixture(scope="session")
def downloaded_tables() -> Dict[str, DataFrame]:
    downloaded_tables = {}
    yield downloaded_tables
    for df in downloaded_tables.values():
        df.unpersist()
    downloaded_tables = {}


def delete_sql_objects(
    client: BaseClient,
    synapse_scripts_dir: str,
    synapse_storage_account: str,
    objects_to_delete: List[Dict[str, str]],
    subscription: str,
    wait_for_completion: bool = True,
):
    logging.info(f"Cleaning up Spark sql objects: {objects_to_delete}")
    client.run_remote_script(
        "drop_sql_object_if_exists.py",
        synapse_scripts_dir,
        args=base64.b64encode(json.dumps(objects_to_delete).encode("ascii")).decode(
            "ascii"
        ),
        storage_account=synapse_storage_account,
        wait_for_completion=wait_for_completion,
        subscription=subscription,
    )


def create_sql_tables(
    client: BaseClient,
    synapse_scripts_dir: str,
    storage_account: str,
    create_args: Dict[str, str],
    subscription: str,
):
    logging.info(f"Creating SQL tables from config: {create_args}")
    client.run_remote_script(
        "create_tables.py",
        synapse_scripts_dir,
        storage_account=storage_account,
        args=base64.b64encode(json.dumps(create_args).encode("ascii")).decode("ascii"),
        subscription=subscription,
    )


@given(parsers.parse("I wait {n} seconds"))
@when(parsers.parse("I wait {n} seconds"))
@then(parsers.parse("I wait {n} seconds"))
def wait_n_seconds(n):
    time.sleep(int(n))


@given(parsers.parse("the following databases/tables do not exist\n{table}"))
def ensure_database_not_exists(
    synapse_script_dir: str,
    synapse_storage_account: str,
    table: str,
    testing_prefix: Optional[str],
    databricks_client: DatabricksClient,
    subscription: str,
):
    if testing_prefix:
        logging.info(
            "Unique testing prefix is set, database cannot exist therefore not checking"
        )
        return
    objects_to_drop = []
    for database, _table in parse_str_table(table):
        drop_dict = {"database": prefix_string(database, testing_prefix)}
        if _table:
            drop_dict["table"] = _table
        objects_to_drop.append(drop_dict)

    delete_sql_objects(
        databricks_client,
        synapse_script_dir,
        synapse_storage_account,
        objects_to_drop,
        subscription=subscription,
    )


@given(
    parsers.parse(
        "the following containers have been flagged for prefix paths cleanup\n{table}"
    )
)
def flag_tables_for_cleanup(
    table: str,
    prefix_paths_cleanup: Set[str],
):
    for row in parse_str_table(table):
        prefix_paths_cleanup["cleanup_objects"].add(row[0])


@given(
    parsers.parse("the following databases have been flagged for cleanup\n{attr_table}")
)
def flag_tables_for_cleanup(
    attr_table: str,
    spark_object_cleanup: Set[Tuple[str, str]],
    testing_prefix: Optional[str],
):
    for database in parse_str_table(attr_table):
        spark_object_cleanup.add((prefix_string(database, testing_prefix), ''))


@given(parsers.parse("the following files/directories don't exist\n{attr_table}"))
def delete_files_directories(
    attr_table: str, data_lake_clients, testing_prefix: Optional[str]
):
    objects_to_flag = parse_str_table(attr_table)
    deletes_per_container = defaultdict(list)
    for container, directory, file in objects_to_flag:
        deletes_per_container[container].append(
            (prefix_path(directory, testing_prefix), file)
        )

    for container, paths in deletes_per_container.items():
        client: DataLakeClient = data_lake_clients(container)
        for directory, file in paths:
            if not file:
                client.delete_directory(directory, raise_exception=False)
                continue
            client.delete_file(f"{directory}/{file}")


@given(parsers.parse("the following directories do exist\n{attr_table}"))
def create_directories(
    attr_table: str, data_lake_clients, testing_prefix: Optional[str]
):
    directories_to_check = parse_str_table(attr_table)
    directories_per_container = defaultdict(list)
    for container, directory in directories_to_check:
        directories_per_container[container].append(
            prefix_path(directory, testing_prefix)
        )

    for container, paths in directories_per_container.items():
        client: DataLakeClient = data_lake_clients(container)
        for directory in paths:
            client.create_new_directory(directory)


@when(parsers.parse("I run the {pipeline} pipeline"), target_fixture="running_pipeline")
def trigger_pipeline(
    pipeline: str,
    testing_prefix: Optional[str],
    databricks_client: DatabricksClient,
):
    logging.info(f"Triggering pipeline {pipeline}")
    client = (
        databricks_client
    )
    run_id = (
        client.run_pipeline(pipeline)
        if not testing_prefix
        else client.run_pipeline(
            pipeline, pipeline_parameters={"testingPrefix": testing_prefix}
        )
    )
    logging.info(f"Got run_id: {run_id}")
    return run_id


@when(
    parsers.parse("I run the {pipeline} pipeline with parameters\n{attr_table}"),
    target_fixture="running_pipeline",
)
def trigger_pipeline_with_params(
    pipeline: str,
    attr_table: str,
    testing_prefix: Optional[str],
    databricks_client: DatabricksClient,
):
    client = (
        databricks_client
    )

    parsed_parameters = {
        parameter_name: parameter_value
        for parameter_name, parameter_value in parse_str_table(attr_table)
    }
    if testing_prefix:
        parsed_parameters["testingPrefix"] = testing_prefix

    logging.info(f"Triggering pipeline {pipeline} with parameters: {parsed_parameters}")
    run_id = client.run_pipeline(pipeline, parsed_parameters)
    logging.info(f"Got run_id: {run_id}")
    return run_id


def _trigger_step_in_pipeline_impl(
    databricks_client: DatabricksClient,
    pipeline: str,
    task: str,
    subscription: str,
    run_params: Dict[str, str],
) -> int:
    logging.info(
        f"Triggering task {task} from pipeline {pipeline} with params {run_params}"
    )
    job_id = databricks_client.get_job_id_from_job_name(pipeline)
    job_spec = databricks_client.get_job(job_id).get("settings", {})
    tasks = job_spec.get("tasks", [])
    for _task in tasks:
        if _task.get("task_key") == task:
            task_spec: dict = _task
            break
    else:
        raise Exception(f"No Task named {task} in pipeline {pipeline}")

    if "job_cluster_key" in task_spec:
        requested_job_cluster_name = task_spec.pop("job_cluster_key")
        for cluster in job_spec.get("job_clusters", []):
            if cluster["job_cluster_key"] == requested_job_cluster_name:
                task_spec["new_cluster"] = cluster["new_cluster"]
                if "autoscale" in task_spec["new_cluster"]:
                    task_spec["new_cluster"].pop("autoscale")
                task_spec["new_cluster"]["num_workers"] = 1
                break
        else:
            raise Exception("No cluster key found, this should not happen")

    if "depends_on" in task_spec:
        task_spec.pop("depends_on")
    if "notebook_task" not in task_spec:
        raise Exception("Only notebook tasks may be run independently")
    if run_params:
        task_spec["notebook_task"]["base_parameters"] = {
            **task_spec["notebook_task"].get("base_parameters", {}),
            **run_params,
        }
    logging.info(f"Triggering task {task} from pipeline {pipeline}")
    new_job_spec = {"tasks": [task_spec], "run_name": f"{pipeline}_{task}"}
    if is_running_as_sp(subscription):
        me = databricks_client.get_me()
        new_job_spec["access_control_list"] = [
            {"group_name": "developers", "permission_level": "CAN_VIEW"},
            {
                "service_principal_name": databricks_client.get_sp_id_from_name(
                    "SynergyDatabricksSPN"
                ),
                "permission_level": "IS_OWNER",
            },
            {
                "service_principal_name": me["userName"],
                "permission_level": "CAN_MANAGE_RUN",
            },
        ]
    response = databricks_client.run_one_time_job(new_job_spec)
    run_id = response["run_id"]
    logging.info(f"Got run_id: {run_id}")
    return run_id


@when(
    parsers.parse("I run the {task} task from the pipeline {pipeline}"),
    target_fixture="running_pipeline",
)
def trigger_step_in_pipeline(
    task: str,
    pipeline: str,
    testing_prefix: Optional[str],
    databricks_client: DatabricksClient,
    subscription: str,
):
    run_params = {"testing_prefix": testing_prefix} if testing_prefix else {}
    return _trigger_step_in_pipeline_impl(
        databricks_client, pipeline, task, subscription, run_params
    )


@when(
    parsers.parse(
        "I run with parameters the {task} task from the pipeline {pipeline}\n{attr_table}"
    ),
    target_fixture="running_pipeline",
)
def trigger_step_in_pipeline_with_params(
    task: str,
    pipeline: str,
    attr_table: str,
    testing_prefix: Optional[str],
    databricks_client: DatabricksClient,
    subscription: str,
):
    parsed_parameters = {
        parameter_name: parameter_value
        for parameter_name, parameter_value in parse_str_table(attr_table)
    }
    if testing_prefix:
        parsed_parameters["testing_prefix"] = testing_prefix
    return _trigger_step_in_pipeline_impl(
        databricks_client, pipeline, task, subscription, parsed_parameters
    )


@then(parsers.parse('the pipeline result is "{result}"'))
def wait_for_pipeline_result(
    result: str,
    running_pipeline: str,
    databricks_client: DatabricksClient,
):
    client = (
        databricks_client
    )
    client.wait_for_pipeline(result, running_pipeline)


def _assert_remote_dir_is_empty_if_exists(client: DataLakeClient, directory: str):
    assert not client.directory_exists(directory) or client.is_directory_empty(
        directory
    )


@then(
    parsers.parse(
        'the [{container}] "{directory}" directory should be empty if it exists'
    )
)
def check_files_in_remote_folder(
    container: str,
    directory: str,
    data_lake_clients: Callable[[str], DataLakeClient],
    testing_prefix: Optional[str],
):
    client = data_lake_clients(container)
    _assert_remote_dir_is_empty_if_exists(
        client, prefix_path(directory, testing_prefix)
    )


@then(
    parsers.parse("the following directories should be empty if they exist:\n{table}")
)
def check_files_in_remote_folder(
    table: str,
    data_lake_clients: Callable[[str], DataLakeClient],
    testing_prefix: Optional[str],
):
    directories_by_container = defaultdict(list)
    for container, directory in parse_str_table(table):
        directories_by_container[container].append(
            prefix_path(directory, testing_prefix)
        )

    for container, directories in directories_by_container.items():
        client = data_lake_clients(container)
        for directory in directories:
            _assert_remote_dir_is_empty_if_exists(client, directory)


def remove_testing_prefixes(df: DataFrame, testing_prefix: str) -> DataFrame:
    if not testing_prefix:
        return df
    logging.info("Removing testing prefix from result data")
    # remove testing_prefix from _metadata.previous_surrogate_keys[].source_table
    schema: st.StructType = df.schema
    if (
        "_metadata" in schema.fieldNames()
        and "previous_surrogate_keys" in schema["_metadata"].dataType.fieldNames()
    ):
        metadata_fields = schema["_metadata"].dataType.fieldNames()
        new_previous_sk_column = sf.expr(
            f"""transform(
            _metadata.previous_surrogate_keys,
            col -> named_struct(
                "source_table",
                regexp_replace(col.source_table, '{testing_prefix}', ''), 
                "surrogate_key_previous",
                col.surrogate_key_previous
            )
        )"""
        )
        df = df.withColumn(
            "_metadata",
            sf.struct(
                *(
                    new_previous_sk_column.alias(field)
                    if field == "previous_surrogate_keys"
                    else sf.col("_metadata").getField(field).alias(field)
                    for field in metadata_fields
                )
            ),
        )
    if "SOURCE_SURROGATE_KEY" in schema.fieldNames():
        df = df.withColumn(
            "SOURCE_SURROGATE_KEY",
            sf.expr(
                f"""transform(
                    SOURCE_SURROGATE_KEY,
                    col -> named_struct(
                        "source_table",
                        regexp_replace(col.source_table, '{testing_prefix}', ''), 
                        "surrogate_key_previous",
                        col.surrogate_key_previous
                    )
                )"""
            ),
        )
    if "TARGET_TABLE" in schema.fieldNames():
        df = df.withColumn(
            "TARGET_TABLE",
            sf.regexp_replace(sf.col("TARGET_TABLE"), testing_prefix, ""),
        )
    if "VALIDATION_METADATA" in schema.fieldNames():
        df = df.withColumn(
            "VALIDATION_METADATA",
            sf.regexp_replace(sf.col("VALIDATION_METADATA"), testing_prefix, ""),
        )

    return df


@when(parsers.parse("I download the following tables\n{table}"))
def download_tables(
    table: str,
    tmp_path: Path,
    spark: SparkSession,
    data_lake_clients: Callable[[str], DataLakeClient],
    testing_prefix: Optional[str],
    downloaded_tables: Dict[str, DataFrame],
):
    for i, (fq_table, db_location_container, db_location, format) in enumerate(
        parse_str_table(table)
    ):
        db_location = prefix_path(db_location, testing_prefix)

        db, table = fq_table.split(".")
        client = data_lake_clients(db_location_container)
        remote_table_dir = f"{db_location}/{table}"
        
        if not client.directory_exists(remote_table_dir):
            raise ValueError("No table data found in specified location")
        local_path = tmp_path / uuid1().hex / f"{i}fq_table"
        logging.info(f"Downloading {fq_table} from {remote_table_dir} to {local_path}")
        client.download_directory_to_local_directory(remote_table_dir, local_path)
        df = spark.read.format(format).load(str(local_path))
        df = remove_testing_prefixes(df, testing_prefix)
        downloaded_tables[fq_table] = df.cache()


@given(parsers.parse('I {create_or_append} the input tables for scenario "{scenario}"'))
def create_input_tables(
    create_or_append: str,
    scenario: str,
    testdata_folder: Path,
    databricks_client: DatabricksClient,
    data_lake_clients: Callable[[str], DataLakeClient],
    synapse_script_dir: str,
    testing_prefix: str,
    synapse_storage_account: str,
    subscription: str,
):
    if create_or_append not in ("create", "append to"):
        raise ValueError(
            "Invalid value for create or append, should be one of ['create', 'append to']"
        )

    write_mode = "overwrite" if create_or_append == "create" else "append"
    unique_folder_name = f"input_{uuid4().hex}"
    dl_client = data_lake_clients("default")
    local_folder_to_upload = testdata_folder / scenario / "input/tables"
    if not local_folder_to_upload.exists():
        raise ValueError(f"Folder {local_folder_to_upload} not found")

    try:
        dl_client.upload_directory_to_remote_directory(
            local_folder_to_upload, unique_folder_name
        )
        create_sql_tables(
            databricks_client,
            synapse_scripts_dir=synapse_script_dir,
            create_args={
                "input_folder": dl_client.as_abfss_path(unique_folder_name),
                "testing_prefix": testing_prefix,
                "write_mode": write_mode,
            },
            storage_account=synapse_storage_account,
            subscription=subscription,
        )
    finally:
        dl_client.delete_directory(unique_folder_name, raise_exception=False)


@given(
    parsers.parse(
        "I pre-create the following databases at the following locations\n{table}"
    )
)
def create_input_tables_with_locations(
    databricks_client: DatabricksClient,
    synapse_storage_account: str,
    subscription: str,
    synapse_script_dir: str,
    testing_prefix: str,
    table: str,
):
    database_create_args = [
        {
            "database": prefix_string(database, testing_prefix),
            "container": container,
            "path": prefix_path(path, testing_prefix),
        }
        for database, container, path in parse_str_table(table)
    ]
    logging.info(f"Pre-Creating databases: {database_create_args}")
    client = (
        databricks_client
    )
    client.run_remote_script(
        "create_databases.py",
        synapse_script_dir,
        args=base64.b64encode(json.dumps(database_create_args).encode("ascii")).decode(
            "ascii"
        ),
        storage_account=synapse_storage_account,
        subscription=subscription,
    )
