from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, array_union, explode, col, \
    collect_set
from typing import List
from collections import defaultdict


def extract_json_to_struct(df: DataFrame, struct_schema, columns: List[str],
                           result_columns: List[str]) -> DataFrame:
    """
        Processes JSON values in the given columns and extracts the values into
        specified structures to the new columns.

        :param df: dataframe
        :param struct_schema: wanted struct schema
        :param columns: specific columns to process
        :param result_columns: result columns
        :return: modified dataframe
    """
    assert len(columns) == len(result_columns)

    for column, result_column in zip(columns, result_columns):
        df = df.withColumn(
            result_column, from_json(column, struct_schema))

    return df


def merge_array_columns(df: DataFrame, columns: List[str],
                        result_column: str) -> DataFrame:
    """
        Merges the given array type columns into the specified single
        resulting column.

        :param df: dataframe
        :param columns: specific columns to process
        :param result_column: result column
        :return: modified dataframe
    """
    assert len(columns) >= 2

    df = df.withColumn(result_column, array_union(columns[0], columns[1]))
    for column in columns[2:]:
        df = df.withColumn(result_column, array_union(result_column, column))

    return df


def expand_struct_to_columns(df: DataFrame, struct_column: str,
                             struct_fields: List[str],
                             result_columns: List[str]) -> DataFrame:
    """
        Processes the given structure column and expands its fields to the
        resulting columns.

        :param df: dataframe
        :param struct_column: column with the struct values
        :param struct_fields: fields of the given structure from the column
        :param result_columns: resulting columns
        :return: modified dataframe
    """
    df = df.withColumn("tmp", explode(struct_column))

    for field, result_column in zip(struct_fields, result_columns):
        df = df.withColumn(result_column, col("tmp." + field))

    df = df.drop("tmp")
    return df


def extract_columns_to_map(df: DataFrame, key_column: str,
                           value_column: str) -> dict:
    """
        Extracts the data from the given columns and creates a map.

        :param df: dataframe
        :param key_column: column whose values should be used as key for the
                            result map
        :param value_column: column whose values should be used as values for
                                the result map
        :return: extracted map
    """
    return df.select(key_column, value_column).rdd.collectAsMap()


def invert_map(d: dict) -> dict:
    """
        Inverts the given key-value map and inverts it to value-key map.

        :param d: original map
        :return: inverted map
    """
    reversed_d = defaultdict(list)

    for key, value in d.items():
        reversed_d[value].append(key)

    return reversed_d


def group_columns_to_set(df: DataFrame, group_by_columns: List[str],
                         to_group_column: str,
                         result_grouped_column: str) -> DataFrame:
    """

        :param df: dataframe
        :param group_by_columns: columns by which to group values
        :param to_group_column: column which should be aggregated
        :param result_grouped_column: resulting name of the aggregated column
        :return: grouped dataframe
    """
    return df.groupby(group_by_columns) \
        .agg(collect_set(to_group_column).alias(result_grouped_column))

