import fire
import pickle

from pathlib import Path
from tqdm.auto import tqdm

from historical_odds_processing.store.db_creation.output_filenames import OutputFilenames
from historical_odds_processing.utils.batching import run_multiprocessing


def merge_mapping(args):
    workingDirectory, mappingName = args
    allEntries = set()
    allMappingFiles = list(Path(workingDirectory).glob(f'**/{mappingName}.pkl'))
    for file in tqdm(allMappingFiles, desc=f'Merging {mappingName} mapping files'):
        entries = pickle.load(open(file, 'rb'))
        allEntries = allEntries.union(entries)
    mapping = {entry: i for i, entry in enumerate(allEntries)}
    pickle.dump(mapping, open(f'{workingDirectory}/{mappingName}_final_mapping.pkl', 'wb'))


def merge_all_mappings(outputDirectory):
    run_multiprocessing(
        functionToProcess=merge_mapping,
        parameterList=[(outputDirectory, filename) for filename in OutputFilenames.ALL_MAPPING_FILES]
    )


if __name__ == '__main__':
    fire.Fire(merge_all_mappings)
