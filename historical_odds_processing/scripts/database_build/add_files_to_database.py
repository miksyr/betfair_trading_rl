import sys

sys.path.append("../../../")
sys.path.append("../../")
import os
import pandas as pd
from pathlib import Path
import pickle
from typing import List, Union

import fire
from tqdm.auto import tqdm

from historical_odds_processing.datamodel.data_store_schema.database_components import Table
from historical_odds_processing.datamodel.data_store_schema.indexes import INDEXES
from historical_odds_processing.datamodel.data_store_schema.historical_trading_schema import ALL_HISTORICAL_SCHEMAS
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import Runners
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import ALL_MAPPING_SCHEMAS
from historical_odds_processing.datamodel.data_store_schema.views import ALL_VIEWS
from historical_odds_processing.store.postgres_insertion_engine import PostgresInsertionEngine
from historical_odds_processing.utils.runner_identifier import break_runner_identifier_string


def copy_rows_into_table(
    insertionEngine: PostgresInsertionEngine, table: Table, orderedColumns: List[str], csvPaths: List[Union[str, Path]]
) -> None:
    tableName = table.tableName
    tableColumns = ", ".join(orderedColumns)
    for csvPath in tqdm(csvPaths, position=0, desc=f"inserting {table.tableName} CSVs"):
        insertionEngine.create_table(
            schema=f"""
                COPY {tableName}(
                    {tableColumns}
                )
                FROM '{csvPath}' DELIMITER ',' CSV HEADER;
            """
        )


def process_mapping(insertionEngine: PostgresInsertionEngine, table: Table, pickleMappingFile: Union[str, Path]) -> None:
    mapping = pickle.load(open(pickleMappingFile, "rb"))
    if isinstance(table, Runners):
        tableMap = pd.DataFrame(data=mapping.items(), columns=["combined_runner_info", "id"])
        splitRunnerInfo = [break_runner_identifier_string(runnerIdentifier=v) for v in tableMap["combined_runner_info"]]
        tableMap["runner_name"] = [v[0] for v in splitRunnerInfo]
        tableMap["betfair_id"] = [v[1] for v in splitRunnerInfo]
        tableMap = tableMap.drop("combined_runner_info", axis=1)
    else:
        tableMap = pd.DataFrame(data=mapping.items(), columns=list(reversed(table.get_column_names())))
    csvOutputPath = f"{pickleMappingFile.parent}/{table.tableName}_final_mapping.csv"
    tableMap.sort_values("id").to_csv(csvOutputPath, index=False)
    copy_rows_into_table(
        insertionEngine=insertionEngine, table=table, orderedColumns=list(tableMap.columns), csvPaths=[csvOutputPath]
    )


def add_files_to_database(insertionEngine: PostgresInsertionEngine, inputDirectory: Union[str, Path]) -> None:
    for table in ALL_MAPPING_SCHEMAS:
        mappingFilePath = list(Path(inputDirectory).glob(f"{table.savingIdentifier}*final_mapping.pkl"))
        if len(mappingFilePath) != 1:
            raise Exception(f"Check mapping file for {table.tableName}")
        process_mapping(insertionEngine=insertionEngine, table=table, pickleMappingFile=mappingFilePath[0])

    for table in ALL_HISTORICAL_SCHEMAS:
        allCsvPaths = list(Path(f"{inputDirectory}/remapped_files").glob(f"**/{table.savingIdentifier}*.csv"))
        copy_rows_into_table(
            insertionEngine=insertionEngine, table=table, orderedColumns=table.get_column_names(), csvPaths=allCsvPaths
        )


def add_foreign_keys(insertionEngine: PostgresInsertionEngine) -> None:
    fkQuery = """
        ALTER TABLE {tableName}
        ADD CONSTRAINT {constraintName} {foreignKeySQL};
    """
    for table in tqdm(ALL_HISTORICAL_SCHEMAS, desc="adding foreign keys"):
        for constraint in table.foreign_key_constraints():
            insertionEngine.create_table(
                schema=fkQuery.format(
                    tableName=table.tableName, constraintName=constraint.constraintName, foreignKeySQL=str(constraint)
                )
            )


def add_indexes(insertionEngine: PostgresInsertionEngine) -> None:
    for index in tqdm(INDEXES, desc="adding indexes"):
        insertionEngine.create_index(tableName=index["table"], column=index["column"])


def add_views(insertionEngine: PostgresInsertionEngine) -> None:
    for view in tqdm(ALL_VIEWS, desc="adding views"):
        insertionEngine.create_table(schema=view)


def main(inputDirectory: Union[str, Path]) -> None:
    insertionEngine = PostgresInsertionEngine(user=os.environ["POSTGRES_USERNAME"], password=os.environ["POSTGRES_PASSWORD"])
    add_files_to_database(insertionEngine=insertionEngine, inputDirectory=inputDirectory)
    add_foreign_keys(insertionEngine=insertionEngine)
    add_indexes(insertionEngine=insertionEngine)
    add_views(insertionEngine=insertionEngine)


if __name__ == "__main__":
    fire.Fire(main)
