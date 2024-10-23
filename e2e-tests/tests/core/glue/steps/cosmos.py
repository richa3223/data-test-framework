import copy
from functools import partial
from itertools import islice
import logging
import json
from pathlib import Path
import pprint
from typing import Dict, List
from azure.cosmos import CosmosClient, PartitionKey
from core.glue.common.utils import TerminalColours, parse_str_table, prefix_string

from pytest_bdd import given, parsers, when, then
import pytest
import jsonschema

COMMON_COSMOS_IGNORE_COLS = ["_rid", "_self", "_etag", "_attachments", "_ts"]


def remove_fields_from_loaded_json(loaded_json: object, fields: List[str]):
    temp_object = copy.copy(loaded_json)
    if isinstance(temp_object, list):
        return list(
            map(partial(remove_fields_from_loaded_json, fields=fields), temp_object)
        )
    for field in fields:
        path = field.split(".", 1)
        if len(path) == 1:
            if path[0] in temp_object:
                temp_object.pop(path[0])
            continue
        else:
            if path[0] in temp_object:
                temp_object[path[0]] = remove_fields_from_loaded_json(
                    temp_object[path[0]], [path[1]]
                )
    return temp_object


def _check_cosmosdb_data_impl(
    testdata_folder: Path,
    scenario: str,
    container: str,
    cosmos_client: CosmosClient,
    ignore_columns: List[str],
    testing_prefix: str,
):
    local_expected_table_folder = (
        testdata_folder / scenario / "output" / "cosmos" / container
    )
    with open(local_expected_table_folder / "schema.json") as f:
        expected_schema = json.load(f)

    with open(local_expected_table_folder / "data.json") as f:
        expected_data = json.load(f)

    database_name, container_name = container.split(".", 1)
    database_client = cosmos_client.get_database_client(
        prefix_string(database_name, testing_prefix)
    )
    container_client = database_client.get_container_client(container_name)
    actual_data = list(
            container_client.query_items(
                query="SELECT * FROM container",
                enable_cross_partition_query=True,
            )
        
    )
    validation_failures = []
    for record in actual_data:
        logging.debug(f"validating {record}")
        try:
            jsonschema.validate(record, schema=expected_schema)
        except jsonschema.exceptions.ValidationError as e:
            validation_failures.append((record, str(e)))

    if validation_failures:
        validation_string = ""
        for record, message in validation_failures:
            validation_string += "=" * 20 + "\n"
            validation_string += (
                f"{TerminalColours.RED}RECORD:{TerminalColours.END}\n{record}\n"
                f"\n{TerminalColours.RED}Error Message:{TerminalColours.END}\n{message}\n"
            )
        logging.error(f"Schema of some cosmosdb records failed:\n{validation_string}")
        raise AssertionError("Schema validation failed")

    expected_data = remove_fields_from_loaded_json(expected_data, ignore_columns)
    actual_data = remove_fields_from_loaded_json(actual_data, ignore_columns)

    records_in_actual_not_in_expected = [
        record for record in actual_data if record not in expected_data
    ]
    records_in_expected_not_in_actual = [
        record for record in expected_data if record not in actual_data
    ]
    message = ""
    if records_in_actual_not_in_expected:
        message += (
            TerminalColours.RED
            + "\nRecord in actual data not in expected data:\n"
            + TerminalColours.END
            + "\n"
            + json.dumps(records_in_actual_not_in_expected, indent=True)
            + "\n"
        )
    if records_in_expected_not_in_actual:
        message += (
            TerminalColours.RED
            + "\nRecord in expected data not in actual data:\n"
            + TerminalColours.END
            + "\n"
            + json.dumps(records_in_expected_not_in_actual, indent=True)
            + "\n"
        )
    if message:
        logging.error(message)
        raise AssertionError("Data from cosmosdb was not as expected")


@then(
    parsers.parse(
        'The CosmosDB container "{container}" contains the "{scenario}" output data'
    ),
)
def the_cosmosdb_container_contains_data(
    container: str,
    scenario: str,
    cosmos_client: CosmosClient,
    testdata_folder: Path,
    testing_prefix: str,
):
    _check_cosmosdb_data_impl(
        testdata_folder,
        scenario,
        container,
        cosmos_client,
        COMMON_COSMOS_IGNORE_COLS,
        testing_prefix,
    )


@then(
    parsers.parse(
        'The CosmosDB container "{container}" contains the "{scenario}" output data\n{table}'
    ),
)
def the_cosmosdb_container_contains_data(
    container: str,
    scenario: str,
    cosmos_client: CosmosClient,
    testdata_folder: Path,
    testing_prefix: str,
    table: str,
):
    _check_cosmosdb_data_impl(
        testdata_folder,
        scenario,
        container,
        cosmos_client,
        COMMON_COSMOS_IGNORE_COLS + [row[0] for row in parse_str_table(table)],
        testing_prefix,
    )


@pytest.fixture(scope="session", autouse=True)
def cleanup_cosmos_databases(cosmos_client: CosmosClient) -> Dict:
    params = {"prefix": None}
    yield params

    if prefix := params.get("prefix"):
        logging.info("Cleaning up CosmosDB objects")
        for database in cosmos_client.list_databases():
            if database.get("id", "").startswith(prefix):
                 logging.info(f"DELETING COSMOSDB DATABASE {database['id']}")
                 cosmos_client.delete_database(database["id"])
