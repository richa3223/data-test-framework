from collections import Counter, defaultdict
from itertools import zip_longest
import logging
from pathlib import Path
from pprint import pformat, pprint
import shutil
import tempfile
from typing import Iterator, List, Optional, Union, Any
from uuid import uuid4
import pyspark.sql.types as st
import pyspark.sql.functions as sf
from pyspark.sql import DataFrame, Row
import logging


def spark_types_equal(
    left: st.DataType, right: st.DataType, enforce_nullability: bool = True
) -> bool:
    """
    Check if pair of Spark data types are equivalent. Supports nested struct types

    Args:
        left: The left data type to compare
        right: The right data type to compare
        enforce_nullability: Whether to consider the nullability of struct fields

    Returns:
        Whether the two data types given are equivalent
    """
    if type(left) != type(right):
        return False

    if isinstance(left, st.StructType):
        if enforce_nullability:
            return left == right

        if len(left.fields) != len(right.fields):
            return False

        return not any(
            left_field.name != right_field.name
            or not spark_types_equal(
                left_field.dataType, right_field.dataType, enforce_nullability
            )
            for left_field, right_field in zip(left.fields, right.fields)
        )

    if isinstance(left, st.ArrayType):
        return spark_types_equal(
            left.elementType, right.elementType, enforce_nullability
        )

    if isinstance(left, st.MapType):
        return (
            spark_types_equal(left.keyType, right.keyType, enforce_nullability)
            and spark_types_equal(left.valueType, right.valueType, enforce_nullability)
            and (
                not enforce_nullability
                or left.valueContainsNull is right.valueContainsNull
            )
        )

    return left == right


def _collect_for_testing(df: DataFrame) -> Iterator[Row]:
    """safely collects a dataframe for testing,
    converts all array types to tuples after converting to allow
    set operations"""
    def _convert_list(element: Union[dict, list, Any]):
        """recursive conversion"""
        if isinstance(element, list):
            return tuple(map(_convert_list, element))
        if not isinstance(element, dict):
            return element
        for k, v in element.items():
            element[k] = _convert_list(v)
        return Row(**element)
    # select("*") to deal with odd caching behaviour with .collect()
    return map(lambda e: _convert_list(e.asDict(recursive=True)), df.select("*").collect())


def _get_diff_string(actual_row: Optional[Row], expected_row: Optional[Row]) -> str:
    left_row_dict = actual_row.asDict() if actual_row is not None else {}
    right_row_dict = expected_row.asDict() if expected_row is not None else {}
    print_str = ""
    for l_item, r_item in zip_longest(left_row_dict.items(), right_row_dict.items()):
        l_column_name, l_value = l_item or (None, None)
        r_column_name, r_value = r_item or (None, None)
        name_value_sep = " "
        if (l_column_name, l_value) == (r_column_name, r_value):
            continue
        if isinstance(l_value, Row) and isinstance(r_value, Row):
            l_value = l_value.asDict()
            r_value = r_value.asDict()
            name_value_sep = "\n"
        elif isinstance(l_value, tuple) and isinstance(r_value, tuple):
            l_value = tuple(v.asDict() if isinstance(v, Row) else v for v in l_value)
            r_value = tuple(v.asDict() if isinstance(v, Row) else v for v in r_value)
            name_value_sep = "\n"
        print_str += "actual:\n"
        print_str += str(l_column_name) + "=" + str(name_value_sep)
        print_str += pformat(l_value, width=120) + "\n"
        print_str += "expected:\n"
        print_str += str(r_column_name) + "=" + str(name_value_sep)
        print_str += pformat(r_value, width=120) + "\n\n"
    return print_str


def _remove_columns_from_df(df: DataFrame, cols: List[str]) -> DataFrame:
    nested_columns_to_drop = defaultdict(set)
    for column in cols:
        if not "." in column:
            if column.lower() in set(map(str.lower, df.columns)):
                df = df.drop(column)
            continue
        root, field = column.split(".", 1)
        nested_columns_to_drop[root].add(field)
    for root, nested_fields in nested_columns_to_drop.items():
        if root.lower() in set(map(str.lower, df.columns)):
            df = df.withColumn(
                root,
                sf.struct(
                    *(
                        sf.col(root).getField(field).alias(field)
                        for field in set(df.schema[root].dataType.fieldNames())
                        - nested_fields
                    )
                ),
            )
    return df


def small_dataframes_equal(
    actual: DataFrame,
    expected: DataFrame,
    enforce_nullability: bool = False,
    ignore_data_cols: Optional[List[str]] = None,
    verbose: bool = True,
) -> bool:
    """
    Simple dataframe comparison function. Compares dataframes row by row in memory.
    DO NOT USE FOR LARGE DATAFRAMES THAT WILL NOT FIT IN MEMORY.
    Row order is not considered.

    Note: Does not work if dataframe contains a MAP type

    Args:
        left: The left data frame to compare
        right: The right data frame to compare
        enforce_nullability: Whether to consider the nullability of columns
        ignore_data_cols: Optional[List[str]]: List of columns to only validate schema
    """
    log_string = ""

    def output_actual_file(input_actual: DataFrame):
        unique_file_name = uuid4().hex
        actual_output_path = Path(f"{tempfile.gettempdir()}/{unique_file_name}")
        input_actual.coalesce(1).write.mode("overwrite").json(
            str(actual_output_path), ignoreNullFields=False
        )
        destination_file_name = f"{tempfile.gettempdir()}/{unique_file_name}.jsonl"
        for file in actual_output_path.glob("*.json"):
            file.rename(destination_file_name)
            break
        shutil.rmtree(actual_output_path, ignore_errors=True)
        logging.warning(
            f"Data comparison failed. Actual output written to: {destination_file_name}"
        )

    def print_and_add(s: str, *args, **kwargs):
        nonlocal log_string
        print(s, *args, **kwargs)
        log_string += s + "\n"

    input_actual = actual
    if "_metadata" in map(
        str.lower, actual.columns
    ) and "surrogate_key_previous" in map(
        str.lower, actual.schema["_metadata"].dataType.fieldNames()
    ):
        actual = actual.orderBy(sf.col("_metadata").getField("surrogate_key_previous"))
    if "_metadata" in map(
        str.lower, expected.columns
    ) and "surrogate_key_previous" in map(
        str.lower, expected.schema["_metadata"].dataType.fieldNames()
    ):
        expected = expected.orderBy(
            sf.col("_metadata").getField("surrogate_key_previous")
        )

    if ignore_data_cols:
        actual = _remove_columns_from_df(actual, ignore_data_cols)
        expected = _remove_columns_from_df(expected, ignore_data_cols)

    if not spark_types_equal(actual.schema, expected.schema, enforce_nullability):
        print_and_add("DataFrame Schema mismatch\nactual:")
        print_and_add(actual._jdf.schema().treeString())
        print_and_add("\nexpected:")
        print_and_add(expected._jdf.schema().treeString())
        output_actual_file(input_actual)
        logging.warning(f"Comparison Result:\n{log_string}")
        return False

    rows1 = Counter(_collect_for_testing(actual))
    rows2 = Counter(_collect_for_testing(expected))

    if rows1 == rows2:
        return True

    print_and_add("\nRecords in actual not in expected:\n")
    for row, count in (rows1 - rows2).items():
        for _ in range(count):
            print_and_add(f"\t{row}")
    print_and_add("\nRecords in expected not in actual:\n")
    for row, count in (rows2 - rows1).items():
        for _ in range(count):
            print_and_add(f"\t{row}")

    if verbose:
        diff_string = ""
        diff_string += "\nDIFF:\n"
        for i, (actual_row, expected_row) in enumerate(
            zip_longest(
                _collect_for_testing(actual.exceptAll(expected)),
                _collect_for_testing(expected.exceptAll(actual)),
            )
        ):
            if actual_row == expected_row:
                continue
            diff_string += "-" * 10
            diff_string += f"record {i}"
            diff_string += "-" * 10
            diff_string += "\n"
            diff_string += _get_diff_string(actual_row, expected_row)
            diff_string += "\n"
        if len(split_diff := diff_string.splitlines(keepends=True)) > (
            max_lines := 500
        ):
            diff_string = "".join(split_diff[:max_lines])
            diff_string += f"\n DIFF LONGER THAN {max_lines} lines, truncating..."
        print_and_add(diff_string)
    output_actual_file(input_actual)
    logging.warning(f"Comparison Result:\n{log_string}")

    return False
