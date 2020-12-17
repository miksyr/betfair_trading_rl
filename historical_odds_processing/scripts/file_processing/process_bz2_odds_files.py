import fire
import re

from pathlib import Path
from tqdm.auto import tqdm

from historical_odds_processing.datamodel.data_store_schema.historical_trading_schema import LastTradedPrice
from historical_odds_processing.datamodel.data_store_schema.historical_trading_schema import MarketDefinitions
from historical_odds_processing.datamodel.data_store_schema.historical_trading_schema import MarketInfo
from historical_odds_processing.datamodel.data_store_schema.historical_trading_schema import RunnerStatusUpdates
from historical_odds_processing.store.db_insertion.bz2_processor import BZ2Processor
from historical_odds_processing.store.db_insertion.csv_output_handler import CSVOutputHandler
from historical_odds_processing.store.db_insertion.output_filenames import OutputFilenames
from historical_odds_processing.utils.batching import get_data_batches
from historical_odds_processing.utils.batching import run_multiprocessing
from historical_odds_processing.utils.paths import get_path


def process_bz2_file(args):
    filepathBatch, validCountryCodes, outputPath, chunkId = args
    outputPath = get_path(outputPath, chunkId)
    marketInfoHandler = CSVOutputHandler(fileName=f'{outputPath}/{OutputFilenames.MARKET_INFO}.csv', tableFields=MarketInfo().get_column_names())
    marketDefinitionHandler = CSVOutputHandler(fileName=f'{outputPath}/{OutputFilenames.MARKET_DEFINITIONS}.csv', tableFields=MarketDefinitions().get_column_names())
    runnerStatusHandler = CSVOutputHandler(fileName=f'{outputPath}/{OutputFilenames.RUNNER_STATUS_UPDATES}.csv', tableFields=RunnerStatusUpdates().get_column_names())
    lastPriceHandler = CSVOutputHandler(fileName=f'{outputPath}/{OutputFilenames.LAST_TRADED_PRICE}.csv', tableFields=LastTradedPrice().get_column_names())
    fileProcessor = BZ2Processor(
        bz2FilePaths=filepathBatch,
        marketInfoHandler=marketInfoHandler,
        marketDefinitionHandler=marketDefinitionHandler,
        runnerStatusUpdateHandler=runnerStatusHandler,
        lastTradedPriceHandler=lastPriceHandler,
        countryCodeFilter=validCountryCodes,
    )
    fileProcessor.process_files(outputDirectory=outputPath)


def process_all_bz2_files(inputDirectory, outputDirectory, validCountryCodes, numThreads=None):
    for yearDir in tqdm(list(Path(inputDirectory).glob('*')), position=0, desc='looping through years'):
        for monthDir in tqdm(list(yearDir.glob('*')), position=1, desc='looping through months'):
            outputPath = get_path(outputDirectory, yearDir.parts[-1], monthDir.parts[-1])
            marketFilepaths = Path(monthDir).glob('**/*.bz2')
            marketFilepaths = [file for file in marketFilepaths if re.search('^\d\.\d.*\.bz2', file.parts[-1]) is not None]
            filepathBatches = get_data_batches(data=marketFilepaths, numBatches=numThreads)
            run_multiprocessing(
                functionToProcess=process_bz2_file,
                parameterList=[(paths, validCountryCodes, outputPath, i) for i, paths in enumerate(filepathBatches)]
            )


if __name__ == '__main__':
    fire.Fire(process_all_bz2_files)
