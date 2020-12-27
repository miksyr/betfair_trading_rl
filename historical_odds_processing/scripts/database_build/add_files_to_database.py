import sys
sys.path.append('../../../../')
import fire
import pandas as pd
import pickle

from tqdm.auto import tqdm
from pathlib import Path

from historical_odds_processing.datamodel.data_store_schema.indexes import INDEXES
from historical_odds_processing.datamodel.data_store_schema.historical_trading_schema import ALL_HISTORICAL_SCHEMAS
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import Runners
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import ALL_MAPPING_SCHEMAS
from historical_odds_processing.datamodel.data_store_schema.views import ALL_VIEWS
from historical_odds_processing.store.db_creation.postgres_insertion_engine import PostgresInsertionEngine
from historical_odds_processing.utils.paths import get_path
from historical_odds_processing.utils.runner_identifier import break_runner_identifier_string

REMAPPED_DIRECTORY = get_path(OUTPUT_DIRECTORY, 'remapped_files')

insertionEngine = PostgresInsertionEngine()


def copy_rows_into_table(table, orderedColumns, csvPaths):
    tableName = table.tableName
    tableColumns = ', '.join(orderedColumns)
    for csvPath in tqdm(csvPaths, position=0, desc=f'inserting {table.tableName} CSVs'):
        insertionEngine.create_table(
            schema=f"""
                COPY {tableName}(
                    {tableColumns}
                )
                FROM '{csvPath}' DELIMITER ',' CSV HEADER;
            """
        )


def process_mapping(table, pickleMappingFile):
    mapping = pickle.load(open(pickleMappingFile, 'rb'))
    if isinstance(table, Runners):
        tableMap = pd.DataFrame(data=mapping.items(), columns=['combined_runner_info', 'id'])
        splitRunnerInfo = [break_runner_identifier_string(runnerIdentifier=v) for v in tableMap['combined_runner_info']]
        tableMap['runner_name'] = [v[0] for v in splitRunnerInfo]
        tableMap['betfair_id'] = [v[1] for v in splitRunnerInfo]
        tableMap = tableMap.drop('combined_runner_info', axis=1)
    else:
        tableMap = pd.DataFrame(data=mapping.items(), columns=list(reversed(table.get_column_names())))
    csvOutputPath = f'{pickleMappingFile.parent}/{table.tableName}_final_mapping.csv'
    tableMap.sort_values('id').to_csv(csvOutputPath, index=False)
    copy_rows_into_table(table=table, orderedColumns=list(tableMap.columns), csvPaths=[csvOutputPath])


def add_files_to_database():
    # mapping files
    for table in ALL_MAPPING_SCHEMAS:
        mappingFilePath = list(Path(OUTPUT_DIRECTORY).glob(f'{table.savingIdentifier}*final_mapping.pkl'))
        if len(mappingFilePath) != 1:
            raise Exception(f'Check mapping file for {table.tableName}')
        process_mapping(table=table, pickleMappingFile=mappingFilePath[0])

    # info files
    for table in ALL_HISTORICAL_SCHEMAS:
        allCsvPaths = list(Path(REMAPPED_DIRECTORY).glob(f'**/{table.savingIdentifier}*.csv'))
        copy_rows_into_table(table=table, orderedColumns=table.get_column_names(), csvPaths=allCsvPaths)


def add_foreign_keys():
    fkQuery = """
        ALTER TABLE {tableName} 
        ADD CONSTRAINT {constraintName} {foreignKeySQL};
    """
    for table in tqdm(ALL_HISTORICAL_SCHEMAS, desc='adding foreign keys'):
        for constraint in table.foreign_key_constraints():
            insertionEngine.create_table(
                schema=fkQuery.format(
                    tableName=table.tableName,
                    constraintName=constraint.constraintName,
                    foreignKeySQL=str(constraint)
                )
            )


def add_indexes():
    for index in tqdm(INDEXES, desc='adding indexes'):
        insertionEngine.create_index(tableName=index['table'], column=index['column'])


def add_views():
    for view in tqdm(ALL_VIEWS, desc='adding views'):
        insertionEngine.create_table(schema=view)


def main():
    add_files_to_database()
    add_foreign_keys()
    add_indexes()
    add_views()


if __name__ == '__main__':
    fire.Fire(main)
