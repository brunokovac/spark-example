from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructType, IntegerType
from data_processing.file_reader import FilesReader
from data_processing.preprocessing import RegexReplacement, NestedSingleQuotesFixer
from data_processing.processing import *
from data_processing.constants import *


class DataHandler(ABC):
    """
        Abstract class used as template for processing some data.
    """

    @abstractmethod
    def process(self, files_path: str):
        """
            Processes data based on the defined processing pipeline.

            :param files_path: path to data files
        """
        pass


class MoviesPeopleHandler(DataHandler):
    """
        Data handler used for processing movies data and extracting cast/crew
        information.
    """

    PERSON_SCHEMA = ArrayType(StructType()
        .add(PERSON_ID, IntegerType(), False) \
        .add(PERSON_NAME, StringType(), False))

    def __init__(self, session: SparkSession, files_reader: FilesReader):
        self.session = session
        self.files_reader = files_reader

    def process(self, files_path: str):
        """
            Processes movies data from the data files and extracts people
            person_name -> person_IDs and person_ID -> movies_IDs mappings.

            :param files_path: path to data files
        """
        df = self.files_reader.read_files(files_path)

        columns = [MOVIE_CAST_HEADER, MOVIE_CREW_HEADER]
        preprocessing_methods = [
            RegexReplacement({"None": "null"}),
            NestedSingleQuotesFixer()
        ]
        for preprocessing_method in preprocessing_methods:
            df = preprocessing_method.process(df=df, columns=columns)

        df = extract_json_to_struct(
            df=df,
            struct_schema=MoviesPeopleHandler.PERSON_SCHEMA,
            columns=[MOVIE_CAST_HEADER, MOVIE_CREW_HEADER],
            result_columns=[CAST_EXTRACTED_STRUCT, CREW_EXTRACTED_STRUCT]
        )

        df = merge_array_columns(
            df=df,
            columns=[CAST_EXTRACTED_STRUCT, CREW_EXTRACTED_STRUCT],
            result_column=MERGED_PEOPLE_STRUCT
        )

        df = expand_struct_to_columns(
            df=df,
            struct_column=MERGED_PEOPLE_STRUCT,
            struct_fields=[PERSON_ID, PERSON_NAME],
            result_columns=[EXTRACTED_PERSON_ID, EXTRACTED_PERSON_NAME]
        )

        people_movies_df = group_columns_to_set(
            df=df,
            group_by_columns=[EXTRACTED_PERSON_ID, EXTRACTED_PERSON_NAME],
            to_group_column=MOVIE_ID_HEADER,
            result_grouped_column=MOVIE_IDS
        )

        self.person_id_name_map = extract_columns_to_map(
            df=people_movies_df,
            key_column=EXTRACTED_PERSON_ID,
            value_column=EXTRACTED_PERSON_NAME
        )

        self.person_name_ids_map = invert_map(self.person_id_name_map)

        self.person_id_movies_map = extract_columns_to_map(
            df=people_movies_df,
            key_column=EXTRACTED_PERSON_ID,
            value_column=MOVIE_IDS
        )

    def get_person_name_ids_map(self) -> dict:
        """
            Gets dictionary mapping people names to matched IDs.

            :return: name->IDs map
        """
        return self.person_name_ids_map

    def get_person_id_movies_map(self) -> dict:
        """
            Gets dictionary mapping people IDs to matched movies IDs.

            :return: person_ID->movies_IDs map
        """
        return self.person_id_movies_map
