import logging
import os

from pathlib import Path

import pytest
#from dotenv import load_dotenv
from pytest_bdd import given, parsers

#from azure.cosmos import CosmosClient

from core.glue.common.utils import prefix_string


from pathlib import Path

from core.glue.steps import *


#load_dotenv()

TESTDATA_FOLDER = (Path(__file__).parent / "data").absolute().resolve()


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--subscription", action="store", default=os.environ.get("SUBSCRIPTION_ID")
    )
    parser.addoption(
        "--resource-group",
        action="store",
        default=os.environ.get("SYNAPSE_RESOURCE_GROUP"),
    )
    parser.addoption(
        "--db-resource-group",
        action="store",
        default=os.environ.get("DATABRICKS_RESOURCE_GROUP"),
    )
    parser.addoption(
        "--workspace", action="store", default=os.environ.get("SYNAPSE_NAME")
    )
    parser.addoption(
        "--databricks-name", action="store", default=os.environ.get("DATABRICKS_NAME")
    )
    parser.addoption(
        "--log-disable",
        action="append",
        default=[
            "azure.core.pipeline.policies.http_logging_policy",
            "azure.identity",
            "msal_extensions.libsecret",
            "py.warnings",
            "blib2to3.pgen2.driver",
        ],
    )


def pytest_configure(config):
    for name in config.getoption("--log-disable", default=[]):
        logger = logging.getLogger(name)
        logger.propagate = False


# --------------------------------------- HOOKS ---------------------------------------


def pytest_bdd_step_error(
    request, feature, scenario, step, step_func, step_func_args, exception
):
    import inspect

    msg = (
        f"{TerminalColours.RED}Step failed: {step}\n"
        f"Error in: {inspect.getsourcefile(step_func)}:{inspect.getsourcelines(step_func)[1]}\n"
        f"Exception:\n{exception}{TerminalColours.END}\n"
    )
    print(msg)
    logging.error(msg)


def pytest_bdd_before_scenario(request, feature, scenario):
    msg = (
        f"{TerminalColours.BLUE}Running Scenario: {scenario.name}{TerminalColours.END}"
    )
    logging.info(msg)


def pytest_bdd_before_step(request, feature, scenario, step, step_func):
    msg = f"\n{TerminalColours.CYAN}Running Step:\n{step}{TerminalColours.END}\n"
    logging.info(msg)


def pytest_bdd_after_step(request, feature, scenario, step, step_func, step_func_args):
    msg = (
        f"{TerminalColours.CYAN}Step {TerminalColours.GREEN}PASSED{TerminalColours.END}"
    )
    logging.info(msg)


@given(
    parsers.parse('the testdata folder: "{folder}"'),
    target_fixture="testdata_folder",
)
def set_testdata_folder(folder: str) -> Path:
    logging.info(f"Using {folder} as the testdata root folder")
    folder = (TESTDATA_FOLDER / folder).absolute().resolve()
    if not folder.exists():
        raise FileNotFoundError(f"Path {folder} not found")
    return folder


# @given(parsers.parse('the CosmosDB container "{container}" contains non-Synergy data with a later last_updated value than existing records'))
# def update_record(
#     container: str,
#     cosmos_client: CosmosClient,
#     testing_prefix: str,
# ):
#     database_name, container_name = container.split(".", 1)
#     database_client = cosmos_client.get_database_client(
#         prefix_string(database_name, testing_prefix)
#     )
#     container_client = database_client.get_container_client(container_name)

#     last_updated = "2022-09-30T08:08:08.000Z"
    
#     sts_object = {
#         "id": "INSERTED_DIRECTLY_INTO_COSMOSDB",
#         "trade_id": "some_trade_id",
#         "sts_id": "10329824_2882_3",
#         "mother_stem_id": "10329824_2882_1",
#         "daughter_stem_id": "10329824_2882_2",
#         "grade": {
#             "grade_name": "My Grade 1",
#             "grade_api": 1.01,
#             "grade_sulfur": 1.02
#         },
#         "product": "Product_1",
#         "group": "Group_1",
#         "family": "Family_1",
#         "sts_zone": "zone:1",
#         "rank": 1,
#         "mother_vessel_id": "inserted-mother-vessel-id",
#         "daughter_vessel_id": "inserted-daughter-vessel-id",
#         "sts_start_time": "2021-05-08T14:16:00.000",
#         "sts_end_time": "2021-05-09T15:15:00.000",
#         "volume_transferred_kt": 49961,
#         "volume_transferred_kbbl": 99922,
#         "provider": "Kpler",
#         "audit": {
#             "created_on": last_updated,
#             "created_by": "a_user@glencore.co.uk",
#             "updated_on": last_updated,
#             "updated_by": "a_user@glencore.co.uk"
#         },
#         "processing_ts": last_updated,
#         "ttl": -1,
#     }
#     container_client.upsert_item(sts_object)