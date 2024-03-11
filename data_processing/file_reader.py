from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class FilesReader(ABC):
    """
        Template class used for reading data from the files.
    """

    @abstractmethod
    def read_files(self, files_path: str) -> DataFrame:
        """
            Reads and return data from the given files.

            :param files_path: path to data files
            :return: dataframe containing read data from the files
        """
        pass


class CsvFilesReader(FilesReader):
    """
        Reader used for reading the data from CSV files.
    """

    __DEFAULT_CONFIG = {
        "escape": "\"",
        "quote": "\"",
        "multiline": True
    }

    def __init__(self, session: SparkSession, config: dict = __DEFAULT_CONFIG):
        self.session = session
        self.config = config

        self.reader = self.session.read
        for option, value in self.config.items():
            self.reader = self.reader.option(option, value)

    def read_files(self, files_path: str) -> DataFrame:
        """
            Reads and return data from the given CSV files.

            :param files_path: path to CSV data files
            :return: dataframe containing read data from the files
        """
        return self.reader.csv(files_path, header=True)

