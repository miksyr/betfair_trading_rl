import testing.postgresql
from unittest import TestCase

from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import ALL_MAPPING_SCHEMAS
from historical_odds_processing.datamodel.data_store_schema.historical_trading_schema import ALL_HISTORICAL_SCHEMAS
from historical_odds_processing.store.postgres_insertion_engine import PostgresInsertionEngine


def create_table(postgresqlConnection):
    config = postgresqlConnection.dsn()
    dbEngine = PostgresInsertionEngine(
        databaseName=config['database'],
        user=config['user'],
        password=config.get('password'),
        port=config['port'],
        host=config['host']
    )
    for table in ALL_MAPPING_SCHEMAS:
        dbEngine.create_table(schema=table.create_table_sql())
    for table in ALL_HISTORICAL_SCHEMAS:
        dbEngine.create_table(schema=table.create_table_sql())


# Generate Postgresql class which shares the generated database
Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True, on_initialized=create_table)


def tearDownModule():
    # clear cached database at end of tests
    Postgresql.clear_cache()


class TestPostgresEngine(TestCase):

    def __init__(self, methodName='runTest'):
        super(TestPostgresEngine, self).__init__(methodName=methodName)
        self.firstCustomerId = 10
        self.firstCustomerName = 'Mary'
        self.secondCustomerId = 50
        self.secondCustomerName = 'John'

    def setUp(self):
        super().setUp()
        self.postgresql = Postgresql()
        config = self.postgresql.dsn()
        self.dbEngine = PostgresInsertionEngine(
            databaseName=config['database'],
            user=config['user'],
            password=config.get('password'),
            port=config['port'],
            host=config['host']
        )

    def tearDown(self):
        super().tearDown()
        self.postgresql.stop()

    def test_betting_type(self):
        BET_TYPE_1 = 'testBetType1'
        BET_TYPE_2 = 'testBetType2'
        insertedId1 = self.dbEngine.insert_betting_type(bettingTypeName=BET_TYPE_1)
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_betting_type(bettingTypeName=BET_TYPE_2)
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_betting_type(bettingTypeName=BET_TYPE_1)
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query='SELECT * FROM tbl_betfair_betting_types')
        self.assertSequenceEqual(list(fullTableResults['betting_type_name'].values), [BET_TYPE_1, BET_TYPE_2])

        betTypeId1 = self.dbEngine.get_betting_type_index(bettingTypeName=BET_TYPE_1)
        self.assertEqual(betTypeId1, 1)
        betTypeId2 = self.dbEngine.get_betting_type_index(bettingTypeName=BET_TYPE_2)
        self.assertEqual(betTypeId2, 2)

    def test_market_type(self):
        MARKET_TYPE_1 = 'testMarketType1'
        MARKET_TYPE_2 = 'testMarketType2'
        insertedId1 = self.dbEngine.insert_market_type(marketType=MARKET_TYPE_1)
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_market_type(marketType=MARKET_TYPE_2)
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_market_type(marketType=MARKET_TYPE_1)
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query='SELECT * FROM tbl_betfair_market_types')
        self.assertSequenceEqual(list(fullTableResults['market_type'].values), [MARKET_TYPE_1, MARKET_TYPE_2])

        marketTypeId1 = self.dbEngine.get_market_type_index(marketType=MARKET_TYPE_1)
        self.assertEqual(marketTypeId1, 1)
        marketTypeId2 = self.dbEngine.get_market_type_index(marketType=MARKET_TYPE_2)
        self.assertEqual(marketTypeId2, 2)

    def test_market_status(self):
        MARKET_STATUS_1 = 'testMarketStatus1'
        MARKET_STATUS_2 = 'testMarketStatus2'
        insertedId1 = self.dbEngine.insert_market_status(marketStatus=MARKET_STATUS_1)
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_market_status(marketStatus=MARKET_STATUS_2)
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_market_status(marketStatus=MARKET_STATUS_1)
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query='SELECT * FROM tbl_betfair_market_status')
        self.assertSequenceEqual(list(fullTableResults['market_status_name'].values), [MARKET_STATUS_1, MARKET_STATUS_2])

        marketTypeId1 = self.dbEngine.get_market_status_index(marketStatus=MARKET_STATUS_1)
        self.assertEqual(marketTypeId1, 1)
        marketTypeId2 = self.dbEngine.get_market_status_index(marketStatus=MARKET_STATUS_2)
        self.assertEqual(marketTypeId2, 2)
