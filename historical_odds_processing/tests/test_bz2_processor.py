from datetime import datetime
from pathlib import Path
from shutil import rmtree
from unittest import TestCase

from historical_odds_processing.datamodel.constants import BETFAIR_DATETIME_FORMAT
from historical_odds_processing.store.db_creation.bz2_processor import BZ2Processor
from historical_odds_processing.store.db_creation.csv_output_handler import CSVOutputHandler
from historical_odds_processing.tests.example_bz2_data.raw_market_change_update import RAW_MARKET_CHANGE_UPDATE


class DummyOutputHandler(CSVOutputHandler):

    def __init__(self):
        self.data = None

    def add(self, data):
        self.data = data


class TestBZ2Processor(TestCase):

    def __init__(self, methodName='runTest'):
        super(TestBZ2Processor, self).__init__(methodName=methodName)
        self.testBetfairMarketId = RAW_MARKET_CHANGE_UPDATE['mc'][0]['id']
        self.testUnixTimestamp = int(RAW_MARKET_CHANGE_UPDATE['pt'] / 1000)
        self.testMarketChangeData = RAW_MARKET_CHANGE_UPDATE['mc'][0]

    def setUp(self):
        super().setUp()
        self.bz2Processor = BZ2Processor(
            bz2FilePaths=None,
            marketInfoHandler=DummyOutputHandler(),
            marketDefinitionHandler=DummyOutputHandler(),
            runnerStatusUpdateHandler=DummyOutputHandler(),
            lastTradedPriceHandler=DummyOutputHandler(),
            countryCodeFilter=None
        )

    def test_process_market_info(self):
        self.bz2Processor.process_market_info(marketChangeData=self.testMarketChangeData)
        self.assertEqual(self.bz2Processor.bettingTypes, {'ODDS', })
        self.assertEqual(self.bz2Processor.marketTypes, {'FIRST_HALF_GOALS_25', })
        self.assertEqual(self.bz2Processor.countryCodes, {'SE', })
        self.assertEqual(self.bz2Processor.timezones, {'GMT', })
        expectedData = {
            'betfair_market_id': '1.159798672',
            'event_id': 29312140,
            'event_name': 'Djurgardens v Kalmar FF',
            'event_type_id': 1,
            'betting_type': 'ODDS',
            'market_type': 'FIRST_HALF_GOALS_25',
            'country_code': 'SE',
            'timezone': 'GMT'
        }
        self.assertEqual(self.bz2Processor.marketInfoHandler.data, expectedData)

    def test_process_market_definition(self):
        self.bz2Processor.process_market_definition(
            betfairMarketId=self.testBetfairMarketId,
            unixTimestamp=self.testUnixTimestamp,
            marketChangeData=self.testMarketChangeData
        )
        self.assertEqual(self.bz2Processor.marketStatuses, {'OPEN', })
        expectedData = {
                'betfair_market_id': '1.159798672',
                'event_id': 29312140,
                'unix_timestamp': 1561827667,
                'version': 2840273014,
                'bsp_market': 0,
                'turn_in_play_enabled': 1,
                'persistence_enabled': 1,
                'market_base_rate': 5.0,
                'num_winners': 1,
                'market_start_time': datetime.strptime('2019-07-01T17:00:00', BETFAIR_DATETIME_FORMAT),
                'market_suspend_time': datetime.strptime('2019-07-01T17:00:00', BETFAIR_DATETIME_FORMAT),
                'bsp_reconciled': 0,
                'market_is_complete': 1,
                'in_play': 0,
                'cross_matching': 0,
                'runners_voidable': 0,
                'num_active_runners': 2,
                'bet_delay': 0,
                'market_status': 'OPEN',
                'regulators': ['MR_INT'],
                'discount_allowed': 1,
                'open_date': datetime.strptime('2019-07-01T17:00:00', BETFAIR_DATETIME_FORMAT)
            }
        self.assertEqual(self.bz2Processor.marketDefinitionHandler.data, expectedData)

    def test_process_runners(self):
        runners = self.testMarketChangeData['marketDefinition']['runners']
        self.bz2Processor.process_runners(
            runners=runners,
            unixTimestamp=self.testUnixTimestamp,
            betfairMarketId=self.testBetfairMarketId,
            eventId=self.testMarketChangeData['marketDefinition']['eventId']
        )
        self.assertEqual(self.bz2Processor.runners, {'Under 2.5 Goals__47972', 'Over 2.5 Goals__47973'})
        self.assertEqual(self.bz2Processor.runnerStatus, {'ACTIVE', 'INACTIVE'})
        expectedData = {
            'unix_timestamp': self.testUnixTimestamp,
            'status_id': 'INACTIVE',
            'betfair_runner_table_id': 'Over 2.5 Goals__47973',
            'betfair_market_id': self.testBetfairMarketId,
            'event_id': 29312140
        }
        self.assertEqual(self.bz2Processor.runnerStatusUpdateHandler.data, expectedData)

    def test_process_last_traded_price(self):
        testEventId = 1234
        testRunnerId = 5678
        testPrice = 1.4
        self.bz2Processor.process_last_traded_price(
            unixTimestamp=self.testUnixTimestamp,
            betfairMarketId=self.testBetfairMarketId,
            eventId=testEventId,
            runnerIdentifier=testRunnerId,
            price=testPrice
        )
        expectedData = {
            'unix_timestamp': self.testUnixTimestamp,
            'betfair_market_id': self.testBetfairMarketId,
            'event_id': testEventId,
            'betfair_runner_table_id': testRunnerId,
            'price': testPrice
        }
        self.assertEqual(self.bz2Processor.lastTradedPriceHandler.data, expectedData)

    def test_process_files(self):
        filePaths = list(Path('./example_bz2_data/files/').glob('*.bz2'))
        bz2Processor = BZ2Processor(
            bz2FilePaths=filePaths,
            marketInfoHandler=DummyOutputHandler(),
            marketDefinitionHandler=DummyOutputHandler(),
            runnerStatusUpdateHandler=DummyOutputHandler(),
            lastTradedPriceHandler=DummyOutputHandler(),
            countryCodeFilter=None
        )
        testOutputDirectory = './test_outputs/'
        bz2Processor.process_files(outputDirectory=testOutputDirectory)
        rmtree(testOutputDirectory)
