from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import regexp_replace, udf
from typing import List
import re


class Preprocessing(ABC):
    """
        Template class used for preprocessing original data from the dataframes.
    """

    @abstractmethod
    def process(self, df: DataFrame, columns: List[str]) -> DataFrame:
        """
            Processes specified columns and returns modified dataframe.

            :param df: dataframe
            :param columns: columns to be processed
            :return: modified dataframe
        """
        pass


class RegexReplacement(Preprocessing):
    """
        Preprocessing class used for replacing certain values in the columns
        based on the specified regexes.
    """

    def __init__(self, replacement_config: dict = {}):
        self.replacement_config = replacement_config

    def __replace_values_for_column(self, df: DataFrame, column: str) -> DataFrame:
        """
            Performs replacement actions on the specific column.

            :param df: dataframe
            :param column: specified column
            :return: modified dataframe
        """
        for pattern, replacement in self.replacement_config.items():
            df = df.withColumn(
                column, regexp_replace(column, pattern, replacement))

        return df

    def process(self, df: DataFrame, columns: List[str]) -> DataFrame:
        for column in columns:
            df = self.__replace_values_for_column(df, column)

        return df


class NestedSingleQuotesFixer(Preprocessing):
    """
        Preprocessing class used for replacing non-escaped single quotes inside
        the specific values.
    """

    def __init__(self):
        self.nested_qutoes_fixer_udf = udf(lambda x: self.__fix_nested_quotes(x))

    def __fix_nested_quotes(self, text: str) -> str:
        """
            Processes the given text and escapes all the single quotes.

            :param text: original text
            :return: modified text
        """
        matches = re.findall(": '(.*?)(?=', '|'})", text)
        matches.sort(key=len, reverse=True)

        replacements = [m.replace("\\'", "'") for m in matches]
        replacements = [m.replace("'", "\\'") for m in replacements]

        for match, replacement in zip(matches, replacements):
            text = text.replace(match, replacement)

        return text

    def process(self, df: DataFrame, columns: List[str]) -> DataFrame:
        for column in columns:
            df = df.withColumn(column, self.nested_qutoes_fixer_udf(column))

        return df
