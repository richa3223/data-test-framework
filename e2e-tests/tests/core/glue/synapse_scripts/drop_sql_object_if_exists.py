"""This script is used to drop any tables or databases from the synapse spark metastore
Arguments should be a json object containing an array of databases and tables to drop
if a table is not specified then the whole database will be dropped
Example:
```
[
    {"database": "db_1", "table": "table_1"}, # only the table is dropped
    {"database": "db_2"} # whole database will be dropped
]
```"""
# COMMAND ----------

# MAGIC %run _synergy_release/_system/setup_common

# COMMAND ----------

import os
import sys

from synergy.utils.synergy_utils import is_databricks

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
    objects_to_drop = parse_synapse_script_arguments(encoded_args)
    logger.debug(f"got objects to drop:\n {objects_to_drop}")

    for object_to_drop in objects_to_drop:
        if "table" in object_to_drop:
            fq_table = f"`{object_to_drop['database']}`.`{object_to_drop['table']}`"
            sql = f"DROP TABLE IF EXISTS {fq_table}"
            logger.debug(f"running sql: `{sql}`")
            spark.sql(sql)
            continue
        sql = f"DROP DATABASE IF EXISTS `{object_to_drop['database']}` CASCADE"
        logger.debug(f"running sql: `{sql}`")
        spark.sql(sql)
        continue


if __name__ == "__main__":
    run()
