from itertools import groupby
from multiprocessing import cpu_count
from multiprocessing import Pool
from typing import Any, Callable, List, Optional, Sequence


def run_multiprocessing(functionToProcess: Callable, parameterList: Sequence[Any], threads: Optional[int] = None) -> Any:
    pool = Pool(cpu_count() if not threads else threads)
    results = pool.map(functionToProcess, parameterList)
    pool.close()
    pool.join()
    pool.terminate()
    return results


def get_data_batches(data: Sequence[Any], numBatches: Optional[int] = None) -> List[List[Any]]:
    numBatches = numBatches or cpu_count()
    batchIds = [list(v) for k, v in groupby(list(range(len(data))), key=lambda x: x // (len(data) / numBatches))]
    return [[data[index] for index in batchIndices] for batchIndices in batchIds]
