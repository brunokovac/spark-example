import os

from fastapi import FastAPI
import logging
from data_processing.session import CustomSparkSession
from data_processing.file_reader import CsvFilesReader
from data_processing.data_handler import MoviesPeopleHandler
from config.constants import *


logging.basicConfig(level = logging.INFO)

session = CustomSparkSession(name="movies_session").get()
logging.info(session.sparkContext.getConf().getAll())

csv_reader = CsvFilesReader(session=session)

data_handler = MoviesPeopleHandler(session=session, files_reader=csv_reader)

csvs_path = os.getenv(ENV_DATA_PATH, DEFAULT_DATA_PATH)

logging.info("Processing movies data... " + csvs_path)
data_handler.process(csvs_path)

person_name_ids_map = data_handler.get_person_name_ids_map()
person_id_movies_map = data_handler.get_person_id_movies_map()

session.stop()

logging.info("Starting API...")
app = FastAPI()

@app.get("/people/{person_name}")
async def get_person_movies(person_name: str) -> dict:
    person_ids = person_name_ids_map.get(person_name, [])

    return {
        person_id: person_id_movies_map.get(person_id, [])
        for person_id in person_ids
    }
