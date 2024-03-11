import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, IntegerType
from data_processing.processing import extract_json_to_struct, merge_array_columns, \
    expand_struct_to_columns, extract_columns_to_map, invert_map, \
    group_columns_to_set


@pytest.fixture
def session_fixture():
    session = SparkSession.builder.getOrCreate()
    yield session


def test_extract_json_to_struct(session_fixture):
    schema = ArrayType(StructType()
        .add("v", IntegerType(), False))

    original_data = [
        {"id": 1, "json": "{'v': 1}"},
        {"id": 2, "json": "{'v': 2}"}
    ]

    original_df = session_fixture.createDataFrame(original_data)

    modified_df = extract_json_to_struct(
        original_df, schema, ["json"], ["extracted"])

    expected_data = [
        {"id": 1, "json": "{'v': 1}", "extracted": [[1]]},
        {"id": 2, "json": "{'v': 2}", "extracted": [[2]]}
    ]

    expected_df = session_fixture.createDataFrame(
        expected_data, schema=modified_df.schema)

    assertDataFrameEqual(modified_df, expected_df)


def test_merge_array_columns(session_fixture):
    original_data = [
        {"id": 1, "value1": ["red"], "value2": ["blue"]},
        {"id": 2, "value1": ["yes"], "value2": ["no"]}
    ]

    original_df = session_fixture.createDataFrame(original_data)

    modified_df = merge_array_columns(
        original_df, ["value1", "value2"], "merged")

    expected_data = [
        {"id": 1, "value1": ["red"], "value2": ["blue"], "merged": ["red", "blue"]},
        {"id": 2, "value1": ["yes"], "value2": ["no"], "merged": ["yes", "no"]}
    ]

    expected_df = session_fixture.createDataFrame(
        expected_data, schema=modified_df.schema)

    assertDataFrameEqual(modified_df, expected_df)


def test_expand_struct_to_columns(session_fixture):
    original_data = [
        {"id": 1, "value": [{"v": 1}]},
        {"id": 2, "value": [{"v": 2}]}
    ]

    original_df = session_fixture.createDataFrame(original_data)

    modified_df = expand_struct_to_columns(
        original_df, "value", ["v"], ["new_value"])

    expected_data = [
        {"id": 1, "value": [{"v": 1}], "new_value": 1},
        {"id": 2, "value": [{"v": 2}], "new_value": 2}
    ]

    expected_df = session_fixture.createDataFrame(
        expected_data, schema=modified_df.schema)

    assertDataFrameEqual(modified_df, expected_df)


def test_extract_columns_to_map(session_fixture):
    original_data = [
        {"id": 1, "name": "Mark"},
        {"id": 2, "name": "John"}
    ]

    original_df = session_fixture.createDataFrame(original_data)

    result_map = extract_columns_to_map(original_df, "id", "name")

    expected_map = {1: "Mark", 2: "John"}

    assert result_map == expected_map


def test_invert_map():
    original_map = {1: "Mark", 2: "Mark", 3: "John"}

    inverted_map = invert_map(original_map)

    expected_map = {"Mark": [1, 2], "John": [3]}

    assert inverted_map == expected_map


def test_group_columns_to_set(session_fixture):
    original_data = [
        {"name": "Mark", "id": 1},
        {"name": "Mark", "id": 2},
        {"name": "John", "id": 3}
    ]

    original_df = session_fixture.createDataFrame(original_data)

    modified_df = group_columns_to_set(
        original_df, ["name"], "id", "grouped_ids")

    expected_data = [
        {"name": "Mark", "grouped_ids": [1, 2]},
        {"name": "John", "grouped_ids": [3]}
    ]

    expected_df = session_fixture.createDataFrame(
        expected_data, schema=modified_df.schema)

    assertDataFrameEqual(modified_df, expected_df)
