from historical_odds_processing.datamodel.data_store_schema.database_components import Column
from historical_odds_processing.datamodel.data_store_schema.database_components import Table
from historical_odds_processing.store.db_creation.output_filenames import OutputFilenames


class BettingTypes(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_betting_types'
        self.columns = [
            Column(name='id', dataType='SERIAL', primaryKey=True),
            Column(name='betting_type_name', dataType='TEXT')
        ]

    @property
    def savingIdentifier(self) -> str:
        return OutputFilenames.BETTING_TYPES


class CountryCodes(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_country_codes'
        self.columns = [
            Column(name='id', dataType='SERIAL', primaryKey=True),
            Column(name='country_code', dataType='TEXT')
        ]

    @property
    def savingIdentifier(self) -> str:
        return OutputFilenames.COUNTRY_CODES


class MarketStatus(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_market_status'
        self.columns = [
            Column(name='id', dataType='SERIAL', primaryKey=True),
            Column(name='market_status_name', dataType='TEXT')
        ]

    @property
    def savingIdentifier(self) -> str:
        return OutputFilenames.MARKET_STATUS


class MarketTypes(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_market_types'
        self.columns = [
            Column(name='id', dataType='SERIAL', primaryKey=True),
            Column(name='market_type', dataType='TEXT')
        ]

    @property
    def savingIdentifier(self) -> str:
        return OutputFilenames.MARKET_TYPES


class Runners(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_runners'
        self.columns = [
            Column(name='id', dataType='SERIAL', primaryKey=True),
            Column(name='betfair_id', dataType='BIGINT'),
            Column(name='runner_name', dataType='TEXT'),
        ]

    @property
    def savingIdentifier(self) -> str:
        return OutputFilenames.RUNNERS


class RunnerStatus(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_runner_status'
        self.columns = [
            Column(name='id', dataType='BIGSERIAL', primaryKey=True),
            Column(name='status', dataType='TEXT'),
        ]

    @property
    def savingIdentifier(self) -> str:
        return OutputFilenames.RUNNER_STATUS


class Timezones(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_timezones'
        self.columns = [
            Column(name='id', dataType='SERIAL', primaryKey=True),
            Column(name='timezone', dataType='TEXT')
        ]

    @property
    def savingIdentifier(self) -> str:
        return OutputFilenames.TIMEZONES


ALL_MAPPING_SCHEMAS = [
    BettingTypes(), CountryCodes(), MarketStatus(), MarketTypes(), Runners(), RunnerStatus(), Timezones()
]
