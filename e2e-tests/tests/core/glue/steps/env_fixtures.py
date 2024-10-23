import logging
import os

from pathlib import Path

import pytest

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Generator

import requests
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential
from azure.mgmt.cosmosdb import CosmosDBManagementClient
from filelock import FileLock
from pyspark.sql import SparkSession

from core.glue.common.databricks_api import DatabricksClient
from core.glue.common.storage_api import DataLakeClient


@pytest.fixture(scope="session", autouse=True)
def _setup_multithreaded_logging(worker_id: str):
    if worker_id != "master":
        worker_n_id = int(worker_id[2:]) % 7
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            f"\033[4{worker_n_id}m[{worker_id}][%(asctime)s][%(levelname)-8s] %(message)s\033[0m"
        )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)


@pytest.fixture(scope="session")
def synapse_name(pytestconfig) -> str:
    return pytestconfig.getoption("workspace")


@pytest.fixture(scope="session")
def synapse_resource_group(pytestconfig) -> str:
    return pytestconfig.getoption("resource_group")


@pytest.fixture(scope="session")
def synapse_scripts_path() -> Path:
    return Path(__file__).parent.absolute().resolve() / ".." / ".." / "synapse_scripts"


@pytest.fixture(scope="session")
def synapse_storage_account_url(synapse_resource: dict) -> str:
    """Returns https://{synapse_storage_account_name}.dfs.core.windows.net"""
    return (
        synapse_resource.get("properties", {})
        .get("defaultDataLakeStorage", {})
        .get("accountUrl")
    )


@pytest.fixture(scope="session")
def synapse_storage_account(synapse_storage_account_url: str) -> str:
    """Returns synapse storage account name"""
    return synapse_storage_account_url.split("/")[2].split(".")[0]


@pytest.fixture(scope="session")
def synapse_resource(
    management_token, subscription, synapse_resource_group, synapse_name
) -> str:
    url = (
        "https://management.azure.com"
        f"/subscriptions/{subscription}"
        f"/resourceGroups/{synapse_resource_group}"
        f"/providers/Microsoft.Synapse/workspaces/{synapse_name}"
        "?api-version=2019-06-01-preview"
    )
    print("*** DEBUG INFO***")
    print(url)
    headers = {"Authorization": f"Bearer {management_token()}"}
    response = requests.get(url, headers=headers)
    return response.json()


@pytest.fixture(scope="session")
def management_token(credential, token_lock_file) -> Callable[[None], str]:
    def get_token():
        with FileLock(str(token_lock_file)):
            return credential.get_token("https://management.azure.com/.default").token

    return get_token


@pytest.fixture(scope="session")
def credential() -> DefaultAzureCredential:
    return DefaultAzureCredential()


@pytest.fixture(scope="session")
def token_lock_file(tmp_path_factory):
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    return root_tmp_dir / ".token_lock"


@pytest.fixture(scope="session")
def subscription(pytestconfig) -> str:
    return pytestconfig.getoption("subscription")


@pytest.fixture(scope="session")
def cosmos_client(
    credential: DefaultAzureCredential,
    token_lock_file,
    subscription: str,
    synapse_resource_group: str,
):
    cosmos_endpoint = os.environ.get("COSMOS_ENDPOINT")
    if not cosmos_endpoint:
        raise TypeError("Environment variable COSMOS_ENDPOINT is not set")
    logging.info(f"Connecting to cosmosdb: {cosmos_endpoint}")
    cosmos_name = cosmos_endpoint.split("//", 1)[1].split(".", 1)[0]
    cosmos_resource_group = synapse_resource_group.replace("core-dataplt", "app")

    with FileLock(str(token_lock_file)):

        return CosmosClient(
            url=cosmos_endpoint,
            credential=os.environ.get("COSMOS_KEY")
            or CosmosDBManagementClient(
                credential=credential, subscription_id=subscription
            )
            .database_accounts.list_keys(cosmos_resource_group, cosmos_name)
            .primary_master_key,
        )

@pytest.fixture(scope="session")
def databricks_client(databricks_url, credential, token_lock_file):
    return DatabricksClient(databricks_url, token_lock_file, credential)


@pytest.fixture(scope="session")
def databricks_resource_group(pytestconfig) -> str:
    return pytestconfig.getoption("db_resource_group")


@pytest.fixture(scope="session")
def databricks_name(pytestconfig) -> str:
    return pytestconfig.getoption("databricks_name")


@pytest.fixture(scope="session")
def databricks_url(
    management_token, subscription, databricks_resource_group, databricks_name
) -> str:
    if url := os.environ.get("DATABRICKS_URL"):
        logging.warning("Databricks URL environment variable found. Using it directly")
        return url
    url = (
        "https://management.azure.com"
        f"/subscriptions/{subscription}"
        f"/resourceGroups/{databricks_resource_group}"
        f"/providers/Microsoft.Databricks/workspaces/{databricks_name}"
        "?api-version=2022-04-01-preview"
    )
    headers = {"Authorization": f"Bearer {management_token()}"}
    response = requests.get(url, headers=headers)

    return "https://" + response.json()["properties"]["workspaceUrl"]


@pytest.fixture(scope="session")
def data_lake_clients(
    synapse_storage_account, credential
) -> Callable[[str], DataLakeClient]:
    return lambda container: DataLakeClient(
        synapse_storage_account, container, credential
    )


def get_spark_test(app_name: str, local_files_location: str) -> SparkSession:
    """Creates and returns a spark session optimised for testing
    Contains a number of config tweaks to optimise testing
    * Sets sensible cluster configs for testing
    * Configures the delta extensions for use

    Args:
        app_name (str): name to give the spark session (used in the UI if wanted)

        local_files_location (str): Path to use for files created by the spark session
        For example the hive metastore, derby logs and managed tables and files created
        during the session. Recommend cleaning this path after finishing the session

    Returns:
        SparkSession: The configured spark session
    """
    warehouse_dir = Path(local_files_location) / "warehouse"
    derby_dir = Path(local_files_location) / "derby"
    log4j_config_path = (
        Path(__file__).parent.absolute()
        / "log4j-driver.properties"
    )
    session_builder = (
        SparkSession.builder.appName(app_name)
        # set number of cores per session
        .master(f"local[{os.getenv('SYNERGY_TEST_PARALLEL_SESSION_CORES', '*')}]")
        # Hive config
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        # Add additional jars
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
        # Using an in memory metastore_db.
        # If this causes memory issues could be swapped with a temp folder instead
        .config("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:db;create=true")
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home={str(derby_dir)} "
            f"-Dlog4j.configuration={log4j_config_path.resolve().as_uri()}",
        )
        # Some spark optimisations to make tests run faster
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SYNERGY_TEST_SHUFFLE_PARTITIONS", "2"),
        )
        .config("hive.metastore.schema.verification", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # Reducing RAM usage
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "512m"))
        .config(
            "spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "512m")
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
        .config("spark.executorEnv.SYNERGY_ENV", "local")
        .config("spark.executorEnv.SYNERGY_KV_NAME", "local_kv")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )

    # get_or_create_spark_application
    return session_builder.getOrCreate()


@pytest.fixture(scope="session")
def spark(worker_id) -> Generator[SparkSession, None, None]:
    """Yield fixture setting up the SparkSession for a test session"""
    with TemporaryDirectory(
        suffix="_synergy_test_framework", prefix="test_spark_"
    ) as temp_dir:
        session = get_spark_test(
            f"synergy_test_{worker_id}", local_files_location=temp_dir
        )
        yield session
