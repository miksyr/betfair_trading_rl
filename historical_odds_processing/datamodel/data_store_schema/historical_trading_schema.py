from historical_odds_processing.datamodel.data_store_schema.database_components import Column
from historical_odds_processing.datamodel.data_store_schema.database_components import ForeignKey
from historical_odds_processing.datamodel.data_store_schema.database_components import Table
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import BettingTypes
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import CountryCodes
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import MarketStatus
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import MarketTypes
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import Runners
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import RunnerStatus
from historical_odds_processing.datamodel.data_store_schema.mapping_table_schema import Timezones
from historical_odds_processing.store.db_insertion.output_filenames import OutputFilenames


class MarketInfo(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_markets'
        self.columns = [
            Column(name='id', dataType='SERIAL', primaryKey=True),
            Column(name='betfair_market_id', dataType='TEXT'),
            Column(name='event_id', dataType='BIGINT'),
            Column(name='event_name', dataType='TEXT'),
            Column(name='event_type_id', dataType='INTEGER'),
            Column(name='betting_type', dataType='INTEGER'),
            Column(name='market_type', dataType='INTEGER'),
            Column(name='country_code', dataType='INTEGER'),
            Column(name='timezone', dataType='INTEGER'),
        ]

    def foreign_key_constraints(self):
        return [
            ForeignKey(columnName='betting_type', tableReferenced=BettingTypes().tableName, referenceColumn='id'),
            ForeignKey(columnName='market_type', tableReferenced=MarketTypes().tableName, referenceColumn='id'),
            ForeignKey(columnName='country_code', tableReferenced=CountryCodes().tableName, referenceColumn='id'),
            ForeignKey(columnName='timezone', tableReferenced=Timezones().tableName, referenceColumn='id'),
        ]

    def get_column_names(self):
        return [col.name for col in self.columns if col.name != 'id']

    @property
    def savingIdentifier(self):
        return OutputFilenames.MARKET_INFO


class MarketDefinitions(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_market_definitions'
        self.columns = [
            Column(name='id', dataType='SERIAL', primaryKey=True),
            Column(name='betfair_market_id', dataType='TEXT'),
            Column(name='event_id', dataType='BIGINT'),
            Column(name='unix_timestamp', dataType='BIGINT'),
            Column(name='version', dataType='BIGINT'),
            Column(name='bsp_market', dataType='BOOLEAN'),
            Column(name='turn_in_play_enabled', dataType='BOOLEAN'),
            Column(name='persistence_enabled', dataType='BOOLEAN'),
            Column(name='market_base_rate', dataType='FLOAT'),
            Column(name='num_winners', dataType='INTEGER'),
            Column(name='market_start_time', dataType='TIMESTAMP'),
            Column(name='market_suspend_time', dataType='TIMESTAMP'),
            Column(name='bsp_reconciled', dataType='BOOLEAN'),
            Column(name='market_is_complete', dataType='BOOLEAN'),
            Column(name='in_play', dataType='BOOLEAN'),
            Column(name='cross_matching', dataType='BOOLEAN'),
            Column(name='runners_voidable', dataType='BOOLEAN'),
            Column(name='num_active_runners', dataType='INTEGER'),
            Column(name='bet_delay', dataType='INTEGER'),
            Column(name='market_status', dataType='INTEGER'),
            Column(name='regulators', dataType='TEXT'),
            Column(name='discount_allowed', dataType='BOOLEAN'),
            Column(name='open_date', dataType='TIMESTAMP'),
        ]

    def foreign_key_constraints(self):
        return [
            ForeignKey(columnName='market_status', tableReferenced=MarketStatus().tableName, referenceColumn='id'),
        ]

    def get_column_names(self):
        return [col.name for col in self.columns if col.name != 'id']

    @property
    def savingIdentifier(self):
        return OutputFilenames.MARKET_DEFINITIONS


class RunnerStatusUpdates(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_runner_status_updates'
        self.columns = [
            Column(name='id', dataType='BIGSERIAL', primaryKey=True),
            Column(name='unix_timestamp', dataType='BIGINT'),
            Column(name='status_id', dataType='INTEGER'),
            Column(name='betfair_runner_table_id', dataType='BIGINT'),
            Column(name='betfair_market_id', dataType='TEXT'),
            Column(name='event_id', dataType='BIGINT'),
        ]

    def foreign_key_constraints(self):
        return [
            ForeignKey(columnName='status_id', tableReferenced=RunnerStatus().tableName, referenceColumn='id'),
            ForeignKey(columnName='betfair_runner_table_id', tableReferenced=Runners().tableName, referenceColumn='id'),
        ]

    def get_column_names(self):
        return [col.name for col in self.columns if col.name != 'id']

    @property
    def savingIdentifier(self):
        return OutputFilenames.RUNNER_STATUS_UPDATES


class LastTradedPrice(Table):

    def __init__(self):
        super().__init__()
        self.tableName = 'tbl_betfair_last_traded_price'
        self.columns = [
            Column(name='id', dataType='BIGSERIAL', primaryKey=True),
            Column(name='unix_timestamp', dataType='BIGINT'),
            Column(name='betfair_market_id', dataType='TEXT'),
            Column(name='event_id', dataType='BIGINT'),
            Column(name='betfair_runner_table_id', dataType='BIGINT'),
            Column(name='price', dataType='FLOAT'),
        ]

    def foreign_key_constraints(self):
        return [
            ForeignKey(columnName='betfair_runner_table_id', tableReferenced=Runners().tableName, referenceColumn='id'),
        ]

    def get_column_names(self):
        return [col.name for col in self.columns if col.name != 'id']

    @property
    def savingIdentifier(self):
        return OutputFilenames.LAST_TRADED_PRICE


ALL_HISTORICAL_SCHEMAS = [
    MarketInfo(), MarketDefinitions(), RunnerStatusUpdates(), LastTradedPrice()
]
