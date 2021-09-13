import numpy as np
from pathlib import Path
import pickle
from typing import Dict, List, Tuple, Union

import fire
from tqdm.auto import tqdm

from historical_odds_processing.store.db_creation.csv_remapper import CSVRemapper
from historical_odds_processing.store.db_creation.output_filenames import OutputFilenames
from utils.batching import get_data_batches, run_multiprocessing
from utils.paths import get_path


def remap_csv(args: Tuple[List[Union[Path, str]], Dict[str, str], str, int, Union[str, Path]]) -> None:
    filesToRemap, mappingDict, fileType, chunkSize, outputDirectory = args
    loadedMaps = {
        mappingName: pickle.load(open(mappingFileLocation, "rb")) for mappingName, mappingFileLocation in mappingDict.items()
    }
    for csvFile in tqdm(filesToRemap, desc=f"remapping {fileType}"):
        csvRemapper = CSVRemapper(
            csvToRemap=str(csvFile),
            remappingDict=loadedMaps,
            outputDirectory=get_path(outputDirectory, "remapped_files"),
            chunkSize=chunkSize,
        )
        csvRemapper.process_csv()


def remap_market_info(numThreads: int, chunkSize: int, outputDirectory: Union[str, Path]) -> None:
    mappingDict = {
        "betting_type": f"{outputDirectory}/{OutputFilenames.BETTING_TYPES}_final_mapping.pkl",
        "market_type": f"{outputDirectory}/{OutputFilenames.MARKET_TYPES}_final_mapping.pkl",
        "country_code": f"{outputDirectory}/{OutputFilenames.COUNTRY_CODES}_final_mapping.pkl",
        "timezone": f"{outputDirectory}/{OutputFilenames.TIMEZONES}_final_mapping.pkl",
    }
    allFiles = list(Path(outputDirectory).glob(f"**/{OutputFilenames.MARKET_INFO}.csv"))
    fileBatches = get_data_batches(data=allFiles, numBatches=np.ceil(len(allFiles) / numThreads))
    run_multiprocessing(
        functionToProcess=remap_csv,
        parameterList=[
            (fileBatch, mappingDict, OutputFilenames.MARKET_INFO, chunkSize, outputDirectory) for fileBatch in fileBatches
        ],
        threads=numThreads,
    )


def remap_market_definitions(numThreads: int, chunkSize: int, outputDirectory: Union[str, Path]) -> None:
    mappingDict = {"market_status": f"{outputDirectory}/{OutputFilenames.MARKET_STATUS}_final_mapping.pkl"}
    allFiles = list(Path(outputDirectory).glob(f"**/{OutputFilenames.MARKET_DEFINITIONS}.csv"))
    fileBatches = get_data_batches(data=allFiles, numBatches=np.ceil(len(allFiles) / numThreads))
    run_multiprocessing(
        functionToProcess=remap_csv,
        parameterList=[
            (fileBatch, mappingDict, OutputFilenames.MARKET_DEFINITIONS, chunkSize, outputDirectory)
            for fileBatch in fileBatches
        ],
        threads=numThreads,
    )


def remap_runner_status_updates(numThreads: int, chunkSize: int, outputDirectory: Union[str, Path]) -> None:
    mappingDict = {
        "status_id": f"{outputDirectory}/{OutputFilenames.RUNNER_STATUS}_final_mapping.pkl",
        "betfair_runner_table_id": f"{outputDirectory}/{OutputFilenames.RUNNERS}_final_mapping.pkl",
    }
    allFiles = list(Path(outputDirectory).glob(f"**/{OutputFilenames.RUNNER_STATUS_UPDATES}.csv"))
    fileBatches = get_data_batches(data=allFiles, numBatches=np.ceil(len(allFiles) / numThreads))
    run_multiprocessing(
        functionToProcess=remap_csv,
        parameterList=[
            (fileBatch, mappingDict, OutputFilenames.RUNNER_STATUS_UPDATES, chunkSize, outputDirectory)
            for fileBatch in fileBatches
        ],
        threads=numThreads,
    )


def remap_last_traded_price(numThreads: int, chunkSize: int, outputDirectory: Union[str, Path]) -> None:
    mappingDict = {"betfair_runner_table_id": f"{outputDirectory}/{OutputFilenames.RUNNERS}_final_mapping.pkl"}
    allFiles = list(Path(outputDirectory).glob(f"**/{OutputFilenames.LAST_TRADED_PRICE}.csv"))
    fileBatches = get_data_batches(data=allFiles, numBatches=np.ceil(len(allFiles) / numThreads))
    run_multiprocessing(
        functionToProcess=remap_csv,
        parameterList=[
            (fileBatch, mappingDict, OutputFilenames.LAST_TRADED_PRICE, chunkSize, outputDirectory)
            for fileBatch in fileBatches
        ],
        threads=numThreads,
    )


def remap_all_id_features(numThreads: int, outputDirectory: Union[str, Path], chunkSize: int = 250000) -> None:
    remap_market_info(numThreads=numThreads, chunkSize=chunkSize, outputDirectory=outputDirectory)
    remap_market_definitions(numThreads=numThreads, chunkSize=chunkSize, outputDirectory=outputDirectory)
    remap_runner_status_updates(numThreads=numThreads, chunkSize=chunkSize, outputDirectory=outputDirectory)
    remap_last_traded_price(numThreads=numThreads, chunkSize=chunkSize, outputDirectory=outputDirectory)


if __name__ == "__main__":
    fire.Fire(remap_all_id_features)
