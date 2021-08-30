import sys
sys.path.append('../../')
import os
from pathlib import Path
from typing import Union

import fire

from historical_odds_processing.datamodel.constants import COUNTRY_CODES_OF_INTEREST
from historical_odds_processing.scripts.file_processing_steps.merge_all_id_feature_maps import merge_all_mappings
from historical_odds_processing.scripts.file_processing_steps.process_bz2_odds_files import process_all_bz2_files
from historical_odds_processing.scripts.file_processing_steps.remap_all_id_features import remap_all_id_features
from multiprocessing import cpu_count


def main(inputDirectory: Union[str, Path], maxNumThreads: int = None) -> None:
    outputDirectory = os.environ['POSTGRES_HISTORICAL_ODDS_DIR']
    numThreads = maxNumThreads or cpu_count()
    process_all_bz2_files(inputDirectory=inputDirectory, outputDirectory=outputDirectory, validCountryCodes=COUNTRY_CODES_OF_INTEREST, numThreads=numThreads)
    merge_all_mappings(outputDirectory=outputDirectory)
    remap_all_id_features(numThreads=numThreads, outputDirectory=outputDirectory, chunkSize=250000)


if __name__ == '__main__':
    fire.Fire(main)
