"""This script is used to create a databases at given locations
the location should NOT contain the _db part of the path

Example:
```
[
  {"database": "maritime", "container": "certified", "path": "relational/maritime},
  {"database": "shipdb_curated", "container": "curated", "path": "VT/ShipDB}
  ...
]
```"""
# COMMAND ----------

# MAGIC %run _synergy_release/_system/setup_common

# COMMAND ----------

import os
import sys

from synergy.utils.driver_utils import driver_setup
from synergy.utils.spark_utils import database_exists
from synergy.utils.synergy_utils import get_synergy_data_lake_url, is_databricks

# COMMAND ----------


if not is_databricks():
    from synergy.utils.driver_utils import driver_setup

    logger, spark = driver_setup()
    sys.path.append(os.getcwd())  # makes common scripts available in synapse
    from synapse_scripts_common import parse_synapse_script_arguments
else:
    from synergy import structured_logger as logger

# COMMAND ----------

# MAGIC %run ./synapse_scripts_common.py

# COMMAND ----------


def create_managed_db(spark, name: str, path: str):
    db_path = f"{path}/_db"
    create_table_statement = f"CREATE DATABASE `{name}` LOCATION '{db_path}'"
    logger.debug(create_table_statement)
    spark.sql(create_table_statement)


def run():
    encoded_args = (
        sys.argv[1] if not is_databricks() else dbutils.widgets.get("encoded_args")
    )
    args = parse_synapse_script_arguments(encoded_args)
    logger.debug(f"got arguments {args}")
    for db_spec in args:
        database = db_spec["database"]
        container = db_spec["container"]
        path = db_spec["path"]

        if database_exists(spark, database):
            logger.warning(f"Database {database} already exists. Skipping...")
        create_managed_db(
            spark, database, get_synergy_data_lake_url(spark, container, path)
        )


if __name__ == "__main__":
    run()
