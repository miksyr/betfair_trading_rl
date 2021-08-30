from pathlib import Path
from typing import Any, Dict

import pandas as pd

from historical_odds_processing.utils.paths import get_path


class CSVRemapper:

    def __init__(self, csvToRemap: str, remappingDict: Dict[str, Dict[Any, Any]], outputDirectory: str, chunkSize: int = 100000):
        self.csvToRemap = csvToRemap
        self.remappingDict = remappingDict
        self.outputDirectory = outputDirectory
        self.chunkSize = chunkSize

    def process_csv(self) -> None:
        existingPathParts = Path(self.csvToRemap).parts
        outputPath = get_path(self.outputDirectory, existingPathParts[-4], existingPathParts[-3], existingPathParts[-2])
        dataframeChunkIterator = pd.read_csv(self.csvToRemap, chunksize=self.chunkSize)
        for chunkIndex, chunk in enumerate(dataframeChunkIterator):
            outputFilename = f'{existingPathParts[-1].split(".")[0]}_{chunkIndex}.csv'
            for column, remappingDictionary in self.remappingDict.items():
                chunk[column] = chunk[column].map(remappingDictionary.get)
            chunk.to_csv(f'{outputPath}/{outputFilename}', index=False)
