import testing.postgresql

from datetime import datetime
from unittest import TestCase

from historical_odds_processing.datamodel.constants import BETFAIR_DATETIME_FORMAT
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import ALL_MAPPING_SCHEMAS
from historical_odds_processing.datamodel.data_store_schema.historical_trading_schema import ALL_HISTORICAL_SCHEMAS
from historical_odds_processing.store.postgres_insertion_engine import PostgresInsertionEngine


def create_table(postgresqlConnection):
    config = postgresqlConnection.dsn()
    dbEngine = PostgresInsertionEngine(
        databaseName=config["database"],
        user=config["user"],
        password=config.get("password"),
        port=config["port"],
        host=config["host"],
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
    def __init__(self, methodName="runTest"):
        super(TestPostgresEngine, self).__init__(methodName=methodName)
        self.firstCustomerId = 10
        self.firstCustomerName = "Mary"
        self.secondCustomerId = 50
        self.secondCustomerName = "John"

    def setUp(self):
        super().setUp()
        self.postgresql = Postgresql()
        config = self.postgresql.dsn()
        self.dbEngine = PostgresInsertionEngine(
            databaseName=config["database"],
            user=config["user"],
            password=config.get("password"),
            port=config["port"],
            host=config["host"],
        )

    def tearDown(self):
        super().tearDown()
        self.postgresql.stop()

    def test_betting_type(self):
        BET_TYPE_1 = "testBetType1"
        BET_TYPE_2 = "testBetType2"
        insertedId1 = self.dbEngine.insert_betting_type(bettingTypeName=BET_TYPE_1)
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_betting_type(bettingTypeName=BET_TYPE_2)
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_betting_type(bettingTypeName=BET_TYPE_1)
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_betting_types")
        self.assertSequenceEqual(list(fullTableResults["betting_type_name"].values), [BET_TYPE_1, BET_TYPE_2])

        betTypeId1 = self.dbEngine.get_betting_type_index(bettingTypeName=BET_TYPE_1)
        self.assertEqual(betTypeId1, 1)
        betTypeId2 = self.dbEngine.get_betting_type_index(bettingTypeName=BET_TYPE_2)
        self.assertEqual(betTypeId2, 2)
        badQuery = self.dbEngine.get_betting_type_index(bettingTypeName="asjhdaf")
        self.assertIsNone(badQuery)

    def test_market_type(self):
        MARKET_TYPE_1 = "testMarketType1"
        MARKET_TYPE_2 = "testMarketType2"
        insertedId1 = self.dbEngine.insert_market_type(marketType=MARKET_TYPE_1)
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_market_type(marketType=MARKET_TYPE_2)
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_market_type(marketType=MARKET_TYPE_1)
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_market_types")
        self.assertSequenceEqual(list(fullTableResults["market_type"].values), [MARKET_TYPE_1, MARKET_TYPE_2])

        marketTypeId1 = self.dbEngine.get_market_type_index(marketType=MARKET_TYPE_1)
        self.assertEqual(marketTypeId1, 1)
        marketTypeId2 = self.dbEngine.get_market_type_index(marketType=MARKET_TYPE_2)
        self.assertEqual(marketTypeId2, 2)
        badQuery = self.dbEngine.get_market_type_index(marketType="asjhdaf")
        self.assertIsNone(badQuery)

    def test_market_status(self):
        MARKET_STATUS_1 = "testMarketStatus1"
        MARKET_STATUS_2 = "testMarketStatus2"
        insertedId1 = self.dbEngine.insert_market_status(marketStatus=MARKET_STATUS_1)
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_market_status(marketStatus=MARKET_STATUS_2)
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_market_status(marketStatus=MARKET_STATUS_1)
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_market_status")
        self.assertSequenceEqual(list(fullTableResults["market_status_name"].values), [MARKET_STATUS_1, MARKET_STATUS_2])

        marketTypeId1 = self.dbEngine.get_market_status_index(marketStatus=MARKET_STATUS_1)
        self.assertEqual(marketTypeId1, 1)
        marketTypeId2 = self.dbEngine.get_market_status_index(marketStatus=MARKET_STATUS_2)
        self.assertEqual(marketTypeId2, 2)
        badQuery = self.dbEngine.get_market_status_index(marketStatus="asjhdaf")
        self.assertIsNone(badQuery)

    def test_country_code(self):
        COUNTRY_CODE_1 = "testCountryCode1"
        COUNTRY_CODE_2 = "testCountryCode2"
        insertedId1 = self.dbEngine.insert_country_code(countryCode=COUNTRY_CODE_1)
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_country_code(countryCode=COUNTRY_CODE_2)
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_country_code(countryCode=COUNTRY_CODE_1)
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_country_codes")
        self.assertSequenceEqual(list(fullTableResults["country_code"].values), [COUNTRY_CODE_1, COUNTRY_CODE_2])

        marketTypeId1 = self.dbEngine.get_country_code_index(countryCode=COUNTRY_CODE_1)
        self.assertEqual(marketTypeId1, 1)
        marketTypeId2 = self.dbEngine.get_country_code_index(countryCode=COUNTRY_CODE_2)
        self.assertEqual(marketTypeId2, 2)
        badQuery = self.dbEngine.get_country_code_index(countryCode="asjhdaf")
        self.assertIsNone(badQuery)

    def test_timezone(self):
        TIMEZONE_1 = "testTimezone1"
        TIMEZONE_2 = "testTimezone2"
        insertedId1 = self.dbEngine.insert_timezone(timezone=TIMEZONE_1)
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_timezone(timezone=TIMEZONE_2)
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_timezone(timezone=TIMEZONE_1)
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_timezones")
        self.assertSequenceEqual(list(fullTableResults["timezone"].values), [TIMEZONE_1, TIMEZONE_2])

        marketTypeId1 = self.dbEngine.get_timezone_index(timezone=TIMEZONE_1)
        self.assertEqual(marketTypeId1, 1)
        marketTypeId2 = self.dbEngine.get_timezone_index(timezone=TIMEZONE_2)
        self.assertEqual(marketTypeId2, 2)
        badQuery = self.dbEngine.get_timezone_index(timezone="asjhdaf")
        self.assertIsNone(badQuery)

    def test_market(self):
        MARKET_1 = {
            "betfairMarketId": "1",
            "eventName": "testEvent1",
            "eventId": 2,
            "eventTypeId": 3,
            "bettingTypeId": 4,
            "marketTypeId": 5,
            "countryCodeId": 6,
            "timezoneId": 7,
        }
        MARKET_2 = {
            "betfairMarketId": "2",
            "eventName": "testEvent2",
            "eventId": 3,
            "eventTypeId": 4,
            "bettingTypeId": 5,
            "marketTypeId": 6,
            "countryCodeId": 7,
            "timezoneId": 8,
        }

        insertedId1 = self.dbEngine.insert_market(
            betfairMarketId=MARKET_1["betfairMarketId"],
            eventName=MARKET_1["eventName"],
            eventId=MARKET_1["eventId"],
            eventTypeId=MARKET_1["eventTypeId"],
            bettingTypeId=MARKET_1["bettingTypeId"],
            marketTypeId=MARKET_1["marketTypeId"],
            countryCodeId=MARKET_1["countryCodeId"],
            timezoneId=MARKET_1["timezoneId"],
        )
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_market(
            betfairMarketId=MARKET_2["betfairMarketId"],
            eventName=MARKET_2["eventName"],
            eventId=MARKET_2["eventId"],
            eventTypeId=MARKET_2["eventTypeId"],
            bettingTypeId=MARKET_2["bettingTypeId"],
            marketTypeId=MARKET_2["marketTypeId"],
            countryCodeId=MARKET_2["countryCodeId"],
            timezoneId=MARKET_2["timezoneId"],
        )
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_market(
            betfairMarketId=MARKET_1["betfairMarketId"],
            eventName=MARKET_1["eventName"],
            eventId=MARKET_1["eventId"],
            eventTypeId=MARKET_1["eventTypeId"],
            bettingTypeId=MARKET_1["bettingTypeId"],
            marketTypeId=MARKET_1["marketTypeId"],
            countryCodeId=MARKET_1["countryCodeId"],
            timezoneId=MARKET_1["timezoneId"],
        )
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_markets")
        self.assertEqual(len(fullTableResults), 2)

        marketTypeId1 = self.dbEngine.get_market_id(eventId=MARKET_1["eventId"], marketTypeId=MARKET_1["marketTypeId"])
        self.assertEqual(marketTypeId1, str(MARKET_1["betfairMarketId"]))
        marketTypeId2 = self.dbEngine.get_market_id(eventId=MARKET_2["eventId"], marketTypeId=MARKET_2["marketTypeId"])
        self.assertEqual(marketTypeId2, str(MARKET_2["betfairMarketId"]))
        marketInfo1 = self.dbEngine.get_market(betfairEventId=MARKET_1["eventId"], betfairMarketId=MARKET_1["betfairMarketId"])
        self.assertEqual(len(marketInfo1), 1)
        marketInfo2 = self.dbEngine.get_market(betfairEventId=MARKET_2["eventId"], betfairMarketId=MARKET_2["betfairMarketId"])
        self.assertEqual(len(marketInfo2), 1)

    def test_market_definition(self):
        TEST_MARKET_DEFINITION = {
            "betfairMarketId": "1.000",
            "eventId": 10101,
            "unixTimestamp": 1000,
            "marketStatus": 2,
            "marketDefinition": {
                "marketTime": "1970-01-01T12:00:00",
                "suspendTime": "1970-01-01T11:59:59",
                "openDate": "1970-01-01T01:00:00",
                "version": 111111,
                "bspMarket": False,
                "turnInPlayEnabled": True,
                "persistenceEnabled": True,
                "marketBaseRate": 0.05,
                "numberOfWinners": 1,
                "bspReconciled": True,
                "complete": True,
                "inPlay": False,
                "crossMatching": False,
                "runnersVoidable": True,
                "numberOfActiveRunners": 3,
                "betDelay": 5,
                "regulators": "TEST",
                "discountAllowed": False,
            },
        }

        insertedId1 = self.dbEngine.insert_market_definition(
            betfairMarketId=TEST_MARKET_DEFINITION["betfairMarketId"],
            eventId=TEST_MARKET_DEFINITION["eventId"],
            unixTimestamp=TEST_MARKET_DEFINITION["unixTimestamp"],
            marketStatusId=TEST_MARKET_DEFINITION["marketStatus"],
            marketDefinition=TEST_MARKET_DEFINITION["marketDefinition"],
        )
        self.assertEqual(insertedId1, 1)

        insertedId2 = self.dbEngine.insert_market_definition(
            betfairMarketId=TEST_MARKET_DEFINITION["betfairMarketId"],
            eventId=TEST_MARKET_DEFINITION["eventId"],
            unixTimestamp=TEST_MARKET_DEFINITION["unixTimestamp"],
            marketStatusId=TEST_MARKET_DEFINITION["marketStatus"],
            marketDefinition=TEST_MARKET_DEFINITION["marketDefinition"],
        )
        self.assertEqual(insertedId2, 2)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_market_definitions")
        self.assertEqual(len(fullTableResults), 2)

        marketDefinition = self.dbEngine.check_last_market_definition(
            betfairMarketId=TEST_MARKET_DEFINITION["betfairMarketId"]
        )
        self.assertEqual(len(marketDefinition), 1)
        self.assertEqual(TEST_MARKET_DEFINITION["unixTimestamp"], marketDefinition["unix_timestamp"].values[0])
        self.assertEqual(TEST_MARKET_DEFINITION["marketStatus"], marketDefinition["market_status"].values[0])
        self.assertEqual(TEST_MARKET_DEFINITION["marketDefinition"]["version"], marketDefinition["version"].values[0])
        self.assertEqual(
            TEST_MARKET_DEFINITION["marketDefinition"]["marketBaseRate"], marketDefinition["market_base_rate"].values[0]
        )
        self.assertEqual(
            TEST_MARKET_DEFINITION["marketDefinition"]["complete"], marketDefinition["market_is_complete"].values[0]
        )

        eventStartTime = self.dbEngine.get_event_id_start_time(betfairEventId=TEST_MARKET_DEFINITION["eventId"])
        self.assertEqual(
            eventStartTime, datetime.strptime(TEST_MARKET_DEFINITION["marketDefinition"]["openDate"], BETFAIR_DATETIME_FORMAT)
        )

    def test_runner(self):
        TEST_RUNNER_1 = {"runnerName": "testRunner1", "betfairId": 123}
        TEST_RUNNER_2 = {"runnerName": "testRunner2", "betfairId": 234}
        insertedId1 = self.dbEngine.insert_runner(runnerName=TEST_RUNNER_1["runnerName"], betfairId=TEST_RUNNER_1["betfairId"])
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_runner(runnerName=TEST_RUNNER_2["runnerName"], betfairId=TEST_RUNNER_2["betfairId"])
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_runner(runnerName=TEST_RUNNER_1["runnerName"], betfairId=TEST_RUNNER_1["betfairId"])
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_runners")
        self.assertEqual(len(fullTableResults), 2)

        runnerNameQueryId = self.dbEngine.get_runner_id_by_name(runnerName=TEST_RUNNER_1["runnerName"])
        self.assertEqual(runnerNameQueryId, 1)
        betfairIdQueryId = self.dbEngine.get_runner_id_by_betfair_id(betfairRunnerId=TEST_RUNNER_2["betfairId"])
        self.assertEqual(betfairIdQueryId, 2)

    def test_runner_status(self):
        TEST_RUNNER_STATUS_1 = "testRunnerStatus1"
        TEST_RUNNER_STATUS_2 = "testRunnerStatus2"
        insertedId1 = self.dbEngine.insert_runner_status(runnerStatus=TEST_RUNNER_STATUS_1)
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_runner_status(runnerStatus=TEST_RUNNER_STATUS_2)
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_runner_status(runnerStatus=TEST_RUNNER_STATUS_1)
        self.assertEqual(insertedId3, 1)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_runner_status")
        self.assertEqual(len(fullTableResults), 2)

        queryResult1 = self.dbEngine.get_runner_status_id(runnerStatus=TEST_RUNNER_STATUS_1)
        self.assertEqual(queryResult1, 1)
        queryResult2 = self.dbEngine.get_runner_status_id(runnerStatus=TEST_RUNNER_STATUS_2)
        self.assertEqual(queryResult2, 2)

    def test_runner_status_update(self):
        RUNNER_STATUS_UPDATE_1 = {"timestamp": 1, "statusId": 2, "betfairMarketId": "3", "betfairRunnerTableId": 4}
        RUNNER_STATUS_UPDATE_2 = {"timestamp": 5, "statusId": 6, "betfairMarketId": "7", "betfairRunnerTableId": 8}
        insertedId1 = self.dbEngine.insert_runner_status_update(
            unixTimestamp=RUNNER_STATUS_UPDATE_1["timestamp"],
            statusId=RUNNER_STATUS_UPDATE_1["statusId"],
            betfairMarketId=RUNNER_STATUS_UPDATE_1["betfairMarketId"],
            betfairRunnerTableId=RUNNER_STATUS_UPDATE_1["betfairRunnerTableId"],
        )
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_runner_status_update(
            unixTimestamp=RUNNER_STATUS_UPDATE_2["timestamp"],
            statusId=RUNNER_STATUS_UPDATE_2["statusId"],
            betfairMarketId=RUNNER_STATUS_UPDATE_2["betfairMarketId"],
            betfairRunnerTableId=RUNNER_STATUS_UPDATE_2["betfairRunnerTableId"],
        )
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_runner_status_update(
            unixTimestamp=RUNNER_STATUS_UPDATE_1["timestamp"],
            statusId=RUNNER_STATUS_UPDATE_1["statusId"],
            betfairMarketId=RUNNER_STATUS_UPDATE_1["betfairMarketId"],
            betfairRunnerTableId=RUNNER_STATUS_UPDATE_1["betfairRunnerTableId"],
        )
        self.assertEqual(insertedId3, 3)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_runner_status_updates")
        self.assertEqual(len(fullTableResults), 3)

        queryResult1 = self.dbEngine.get_last_runner_status_info(
            betfairMarketId=RUNNER_STATUS_UPDATE_1["betfairMarketId"],
            betfairRunnerTableId=RUNNER_STATUS_UPDATE_1["betfairRunnerTableId"],
        )
        self.assertEqual(queryResult1["unix_timestamp"].values[0], RUNNER_STATUS_UPDATE_1["timestamp"])
        self.assertEqual(queryResult1["status_id"].values[0], RUNNER_STATUS_UPDATE_1["statusId"])
        queryResult2 = self.dbEngine.get_last_runner_status_info(
            betfairMarketId=RUNNER_STATUS_UPDATE_2["betfairMarketId"],
            betfairRunnerTableId=RUNNER_STATUS_UPDATE_2["betfairRunnerTableId"],
        )
        self.assertEqual(queryResult2["unix_timestamp"].values[0], RUNNER_STATUS_UPDATE_2["timestamp"])
        self.assertEqual(queryResult2["status_id"].values[0], RUNNER_STATUS_UPDATE_2["statusId"])

    def test_last_traded_price(self):
        LAST_TRADED_PRICE_1 = {"timestamp": 1, "price": 2, "betfairMarketId": "3", "betfairRunnerTableId": 4}
        LAST_TRADED_PRICE_2 = {"timestamp": 5, "price": 6, "betfairMarketId": "7", "betfairRunnerTableId": 8}
        insertedId1 = self.dbEngine.insert_last_traded_price(
            unixTimestamp=LAST_TRADED_PRICE_1["timestamp"],
            betfairMarketId=LAST_TRADED_PRICE_1["betfairMarketId"],
            betfairRunnerTableId=LAST_TRADED_PRICE_1["betfairRunnerTableId"],
            price=LAST_TRADED_PRICE_1["price"],
        )
        self.assertEqual(insertedId1, 1)
        insertedId2 = self.dbEngine.insert_last_traded_price(
            unixTimestamp=LAST_TRADED_PRICE_2["timestamp"],
            betfairMarketId=LAST_TRADED_PRICE_2["betfairMarketId"],
            betfairRunnerTableId=LAST_TRADED_PRICE_2["betfairRunnerTableId"],
            price=LAST_TRADED_PRICE_2["price"],
        )
        self.assertEqual(insertedId2, 2)
        insertedId3 = self.dbEngine.insert_last_traded_price(
            unixTimestamp=LAST_TRADED_PRICE_1["timestamp"],
            betfairMarketId=LAST_TRADED_PRICE_1["betfairMarketId"],
            betfairRunnerTableId=LAST_TRADED_PRICE_1["betfairRunnerTableId"],
            price=LAST_TRADED_PRICE_1["price"],
        )
        self.assertEqual(insertedId3, 3)

        fullTableResults = self.dbEngine.run_select_query(query="SELECT * FROM tbl_betfair_last_traded_price")
        self.assertEqual(len(fullTableResults), 3)

        queryResult1 = self.dbEngine.get_last_traded_price(
            betfairMarketId=LAST_TRADED_PRICE_1["betfairMarketId"],
            betfairRunnerTableId=LAST_TRADED_PRICE_1["betfairRunnerTableId"],
        )
        self.assertEqual(queryResult1["unix_timestamp"].values[0], LAST_TRADED_PRICE_1["timestamp"])
        self.assertEqual(queryResult1["price"].values[0], LAST_TRADED_PRICE_1["price"])
        queryResult2 = self.dbEngine.get_last_traded_price(
            betfairMarketId=LAST_TRADED_PRICE_2["betfairMarketId"],
            betfairRunnerTableId=LAST_TRADED_PRICE_2["betfairRunnerTableId"],
        )
        self.assertEqual(queryResult2["unix_timestamp"].values[0], LAST_TRADED_PRICE_2["timestamp"])
        self.assertEqual(queryResult2["price"].values[0], LAST_TRADED_PRICE_2["price"])
