"""This script is used to create a set of tables from a folder in the datalake.
The folder should be structued as follows

- folder
-- db.table1
--- data.jsonl
--- schema.json
-- db.table2
--- ...

It will create all tables in the input folder as specified by the folder name and its
data

Arguments to this function should be the path to the input folder and the testing prefix
Example:
```
{
  "input_folder": "abfss://default@storage.dfs...",
  "testing_prefix": "my_prefix_",
  "write_mode": "overwrite",
}
```"""
# COMMAND ----------

# MAGIC %run _synergy_release/_system/setup_common

# COMMAND ----------

import os
import sys
import json

from synergy.utils.hdfs_utils import HDFSFileSystemClient
from synergy.utils.spark_utils import table_exists
from synergy.utils.synergy_utils import is_databricks

from pyspark.sql.types import StructType

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


def run():
    encoded_args = (
        sys.argv[1] if not is_databricks() else dbutils.widgets.get("encoded_args")
    )
    args = parse_synapse_script_arguments(encoded_args)
    logger.debug(f"got arguments {args}")
    testing_prefix = args["testing_prefix"]
    input_folder = args["input_folder"]
    write_mode = args["write_mode"]

    if is_databricks():
        tables_to_create = {
            (
                status.path[:-1].rsplit("/", 1)[-1].split(".")[0]
                if not testing_prefix
                else f"{testing_prefix}{status.path[:-1].rsplit('/', 1)[-1].split('.')[0]}",
                status.path[:-1].rsplit("/", 1)[-1].split(".")[1],
            ): status.path[:-1]
            for status in dbutils.fs.ls(input_folder)
            if status.isDir()
        }
    else:
        client = HDFSFileSystemClient(spark, input_folder)
        tables_to_create = {
            (
                status.uri.rsplit("/", 1)[-1].split(".")[0]
                if not testing_prefix
                else f"{testing_prefix}{status.uri.rsplit('/', 1)[-1].split('.')[0]}",
                status.uri.rsplit("/", 1)[-1].split(".")[1],
            ): status.uri
            for status in client.get_directories()
        }
    logger.debug(f"Creating tables from data {tables_to_create}")

    if write_mode == "overwrite" and any(
        table_exists(spark, db, table) for db, table in tables_to_create.keys()
    ):
        existing_table = [
            f"{db}.{table}"
            for db, table in tables_to_create.keys()
            if table_exists(spark, db, table)
        ]
        logger.warning(f"Requested tables already exist {existing_table}")

    for (db, table), data_uri in tables_to_create.items():
        schema_path = f"{data_uri}/schema.json"
        data_path = f"{data_uri}/data.jsonl"
        string_schema = spark.read.text(schema_path, wholetext=True).head()[0]
        schema = StructType.fromJson(json.loads(string_schema))
        df = spark.read.json(data_path, allowComments=True, schema=schema)
        sql = f"CREATE DATABASE IF NOT EXISTS `{db}`"
        logger.info(f"RUNNING SQL {sql}")
        spark.sql(sql)
        fq_table = f"`{db}`.`{table}`"
        logger.info(f"Creating table {fq_table}")
        df.write.mode(write_mode).format("delta").saveAsTable(fq_table)


if __name__ == "__main__":
    run()
