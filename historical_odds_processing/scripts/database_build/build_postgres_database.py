import sys
sys.path.append('../../../')
import logging
import os

import fire

from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import ALL_MAPPING_SCHEMAS
from historical_odds_processing.datamodel.data_store_schema.historical_trading_schema import ALL_HISTORICAL_SCHEMAS
from historical_odds_processing.store.postgres_insertion_engine import PostgresInsertionEngine


def create_postgres_data_store() -> None:
    postgresEngine = PostgresInsertionEngine(
        user=os.environ['POSTGRES_USERNAME'],
        password=os.environ['POSTGRES_PASSWORD']
    )
    for tableSchema in ALL_MAPPING_SCHEMAS + ALL_HISTORICAL_SCHEMAS:
        postgresEngine.create_table(schema=tableSchema.create_table_sql())
        logging.info(f'Added new table: {tableSchema.tableName}')

    logging.info('Done!')


if __name__ == '__main__':
    fire.Fire(create_postgres_data_store)
