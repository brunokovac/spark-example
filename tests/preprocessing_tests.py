import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession
from data_processing.preprocessing import RegexReplacement, NestedSingleQuotesFixer


@pytest.fixture
def session_fixture():
    session = SparkSession.builder.getOrCreate()
    yield session


def test_regex_replacement(session_fixture):
    original_data = [
        {"id": 1, "value": "test - value - 1"},
        {"id": 2, "value": "other value"}
    ]

    original_df = session_fixture.createDataFrame(original_data)

    modified_df = RegexReplacement({"-": "#"}).process(original_df, ["value"])

    expected_data = [
        {"id": 1, "value": "test # value # 1"},
        {"id": 2, "value": "other value"}
    ]

    expected_df = session_fixture.createDataFrame(expected_data)

    assertDataFrameEqual(modified_df, expected_df)


def test_fixing_nested_single_quotes(session_fixture):
    original_data = [
        {"id": 1, "value": "item's value"},
        {"id": 2, "value": "item\'s value"},
        {"id": 3, "value": "correct value"},
    ]

    original_df = session_fixture.createDataFrame(original_data)

    modified_df = NestedSingleQuotesFixer().process(original_df, ["value"])

    expected_data = [
        {"id": 1, "value": "item\'s value"},
        {"id": 2, "value": "item\'s value"},
        {"id": 3, "value": "correct value"},
    ]

    expected_df = session_fixture.createDataFrame(expected_data)

    assertDataFrameEqual(modified_df, expected_df)
