import os
from pyspark.sql import SparkSession
from config.constants import *


class CustomSparkSession:
    """
        Class used for creating and fetching Spark session instance.
    """

    def __init__(self, name: str):
        self.name = name

        self.session = SparkSession.builder \
            .appName(self.name) \
            .config(SPARK_CONFIG_MASTER, os.getenv(ENV_MASTER, DEFAULT_MASTER)) \
            .config(SPARK_CONFIG_MEMORY, os.getenv(ENV_MAX_MEMORY, DEFAULT_MAX_MEMORY))

    def get(self) -> SparkSession:
        """
            Creates and get Spark session.

            :return: Spark session
        """
        return self.session.getOrCreate()
