import os
import shutil
import sys

from src.config.constants import SHADOW_PETASTORM_STORAGE_FOLDER, \
                                PETASTORM_STORAGE_FOLDER
from src.storage.partitioned_petastorm_storage_engine import PartitionedPetastormStorageEngine
from src.storage.petastorm_storage_engine import PetastormStorageEngine
from test.utils.util_functions import write_file


def setUp(partitioned: bool):
    if os.path.isdir(PETASTORM_STORAGE_FOLDER):
        shutil.rmtree(PETASTORM_STORAGE_FOLDER)
    if os.path.isdir(SHADOW_PETASTORM_STORAGE_FOLDER):
        shutil.rmtree(SHADOW_PETASTORM_STORAGE_FOLDER)

    storage_engine = PartitionedPetastormStorageEngine() if partitioned else PetastormStorageEngine()
    dataframe_metadata = write_file(storage_engine, 'traffic001_150', include_lsn=partitioned)
    shutil.copytree(PETASTORM_STORAGE_FOLDER, SHADOW_PETASTORM_STORAGE_FOLDER, dirs_exist_ok=True)

    return (storage_engine, dataframe_metadata)

def tearDown():
    shutil.rmtree(SHADOW_PETASTORM_STORAGE_FOLDER)
