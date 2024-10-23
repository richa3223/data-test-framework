import json
import pyspark.sql.types as st

from typing import Dict, List, Optional
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as sf
from pytest_bdd import parsers, then

from core.glue.common.utils import parse_str_table
from core.glue.common.data_comparison import small_dataframes_equal


COMMON_SCHEMAS_PATH = Path(__file__).parent.absolute() / ".." / ".." / "common_schemas"


def _remote_table_is_as_expected_impl(
    fq_table: str,
    scenario: str,
    ignore_cols: List[str],
    testdata_folder: Path,
    spark: SparkSession,
    downloaded_tables: Dict[str, DataFrame],
    common_schema_file: Optional[str] = None,
):

    local_expected_table_folder = (
        testdata_folder / scenario / "output" / "tables" / fq_table
    )

    actual_df = downloaded_tables[fq_table]
    schema_file_path = (
        local_expected_table_folder / "schema.json"
        if not common_schema_file
        else COMMON_SCHEMAS_PATH / common_schema_file
    )
    with open(schema_file_path) as schema_file:
        schema = st.StructType.fromJson(json.load(schema_file))
    expected_df = spark.read.json(
        str(local_expected_table_folder / "data.jsonl"),
        allowComments=True,
        schema=schema,
    )

    assert small_dataframes_equal(actual_df, expected_df, ignore_data_cols=ignore_cols)


@then(
    parsers.parse(
        'the downloaded "{fq_table}" table should contain the "{scenario}" output data\n{table}'
    )
)
def remote_table_as_expected_ignore_cols(
    fq_table: str,
    scenario: str,
    table: str,
    testdata_folder: Path,
    spark: SparkSession,
    downloaded_tables: Dict[str, DataFrame],
):
    ignore_cols = [row[0] for row in parse_str_table(table)]
    _remote_table_is_as_expected_impl(
        fq_table, scenario, ignore_cols, testdata_folder, spark, downloaded_tables
    )


@then(
    parsers.parse(
        'the downloaded "{fq_table}" table should contain the "{scenario}" output data using common schema "{schema}"\n{table}'
    )
)
def remote_table_as_expected_ignore_cols_common_schema(
    fq_table: str,
    scenario: str,
    table: str,
    schema: str,
    testdata_folder: Path,
    spark: SparkSession,
    downloaded_tables: Dict[str, DataFrame],
):
    ignore_cols = [row[0] for row in parse_str_table(table)]
    _remote_table_is_as_expected_impl(
        fq_table,
        scenario,
        ignore_cols,
        testdata_folder,
        spark,
        downloaded_tables,
        common_schema_file=schema,
    )


@then(
    parsers.parse(
        'the downloaded "{fq_table}" table should contain the "{scenario}" output data using common schema "{schema}"'
    )
)
def remote_table_as_expected_common_schema(
    fq_table: str,
    scenario: str,
    schema: str,
    testdata_folder: Path,
    spark: SparkSession,
    downloaded_tables: Dict[str, DataFrame],
):
    _remote_table_is_as_expected_impl(
        fq_table,
        scenario,
        None,
        testdata_folder,
        spark,
        downloaded_tables,
        common_schema_file=schema,
    )


@then(
    parsers.parse(
        'the values in the "{column}" column of the downloaded "{fq_table}" tables should be unique'
    )
)
def check_column_in_table_is_unique(
    fq_table: str,
    column: str,
    downloaded_tables: Dict[str, DataFrame],
):
    df = downloaded_tables[fq_table]
    assert (
        df.select(sf.col(column).alias("col"))
        .groupBy(sf.col("col"))
        .agg(sf.count("col").alias("count"))
        .where(sf.col("count") > sf.lit(1))
        .count()
        == 0
    )


@then(
    parsers.parse(
        'each row in the raw table "{raw_table}" can be joined to the ingestion log table "{log_table}"'
    )
)
def check_log_matches_raw(
    raw_table: str,
    log_table: str,
    downloaded_tables: Dict[str, DataFrame],
):
    raw_df = downloaded_tables[raw_table]
    log_df = downloaded_tables[log_table]
    file_keys_in_log = {row[0] for row in log_df.select("unique_file_key").collect()}
    file_keys_in_raw = {
        row[0]
        for row in raw_df.select("_metadata.unique_file_key").distinct().collect()
    }
    assert file_keys_in_raw.issubset(file_keys_in_log)
