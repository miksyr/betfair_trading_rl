from easy_postgres_engine import PostgresEngine

from historical_odds_processing.utils.date_functions import from_isoformat
from historical_odds_processing.utils.text_processing import clean_text


class PostgresInsertionEngine(PostgresEngine):

    def __init__(self, user: str, password: str, databaseName: str = 'betfair_odds_data', host: str = 'localhost', port: int = 5432):
        super().__init__(user=user, password=password, databaseName=databaseName, host=host, port=port)

    def insert_betting_type(self, bettingTypeName):
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_betting_types(betting_type_name) 
                VALUES 
                    (%(bettingTypeName)s)
            """,
            parameters={'bettingTypeName': clean_text(text=bettingTypeName)}
        )

    def insert_market_type(self, marketType):
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_market_types(market_type) 
                VALUES 
                    (%(marketType)s)
            """,
            parameters={'marketType': clean_text(text=marketType)}
        )

    def insert_market_status(self, marketStatus):
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_market_status(market_status_name) 
                VALUES 
                    (%(marketStatus)s)
            """,
            parameters={'marketStatus': clean_text(text=marketStatus)}
        )

    def insert_country_code(self, countryCode):
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_country_codes(country_code) 
                VALUES 
                    (%(countryCode)s)
            """,
            parameters={'countryCode': clean_text(text=countryCode)}
        )

    def insert_timezone(self, timezone):
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_timezones(timezone) 
                VALUES 
                    (%(timezone)s)
            """,
            parameters={'timezone': clean_text(text=timezone)}
        )

    def insert_market(self, betfairMarketId, eventName, eventId, eventTypeId, bettingType, marketType, countryCode, timezone):
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_markets 
                    (
                        betfair_market_id, event_name, event_id, event_type_id, betting_type, market_type,
                        country_code, timezone
                    ) 
                VALUES
                    (
                        %(betfairMarketId)s, %(eventName)s, %(eventId)s, %(eventTypeId)s, %(bettingType)s, %(marketType)s,
                        %(countryCode)s, %(timezone)s 
                    )
            """,
            parameters={
                'betfairMarketId': float(betfairMarketId),
                'eventName': clean_text(text=eventName),
                'eventId': int(eventId),
                'eventTypeId': int(eventTypeId),
                'bettingType': int(bettingType),
                'marketType': int(marketType),
                'countryCode': int(countryCode),
                'timezone': int(timezone)
            }
        )

    def insert_market_definition(self, betfairMarketTableId, unixTimestamp, marketStatus, marketDefinition):
        marketStartTime = marketDefinition.get('marketTime')
        marketSuspendTime = marketDefinition.get('suspendTime')
        openDate = marketDefinition.get('openDate')
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_market_definitions 
                    (
                        betfair_market_table_id, unix_timestamp, version, bsp_market, 
                        turn_in_play_enabled, persistence_enabled, market_base_rate, num_winners, 
                        market_start_time, market_suspend_time, bsp_reconciled, market_is_complete, 
                        in_play, cross_matching, runners_voidable, num_active_runners, bet_delay, 
                        market_status, regulators, discount_allowed, open_date
                    ) 
                VALUES
                    (
                        %(betfairMarketTableId)s, %(unixTimestamp)s, %(version)s, %(bspMarket)s, 
                        %(turnInPlayEnabled)s, %(persistenceEnabled)s, %(marketBaseRate)s, %(numWinners)s, 
                        %(marketStartTime)s, %(marketSuspendTime)s, %(bspReconciled)s, %(marketIsComplete)s, 
                        %(inPlay)s, %(crossMatching)s, %(runnersVoidable)s, %(numActiveRunners)s, %(betDelay)s, 
                        %(marketStatus)s, %(regulators)s, %(discountAllowed)s, %(openDate)s
                    )
            """,
            parameters={
                'betfairMarketTableId': int(betfairMarketTableId),
                'unixTimestamp': int(unixTimestamp),
                'version': marketDefinition.get('version'),
                'bspMarket': marketDefinition.get('bspMarket'),
                'turnInPlayEnabled': marketDefinition.get('turnInPlayEnabled'),
                'persistenceEnabled': marketDefinition.get('persistenceEnabled'),
                'marketBaseRate': marketDefinition.get('marketBaseRate'),
                'numWinners': marketDefinition.get('numberOfWinners'),
                'marketStartTime': from_isoformat(marketStartTime.split('.')[0]) if marketStartTime is not None else None,
                'marketSuspendTime': from_isoformat(marketSuspendTime.split('.')[0]) if marketSuspendTime is not None else None,
                'bspReconciled': marketDefinition.get('bspReconciled'),
                'marketIsComplete': marketDefinition.get('complete'),
                'inPlay': marketDefinition.get('inPlay'),
                'crossMatching': marketDefinition.get('crossMatching'),
                'runnersVoidable': marketDefinition.get('runnersVoidable'),
                'numActiveRunners': marketDefinition.get('numberOfActiveRunners'),
                'betDelay': marketDefinition.get('betDelay'),
                'marketStatus': marketStatus,
                'regulators': marketDefinition.get('regulators'),
                'discountAllowed': marketDefinition.get('discountAllowed'),
                'openDate': from_isoformat(openDate.split('.')[0]) if openDate is not None else None
            }
        )

    def insert_runner(self, runnerName, betfairId, betfairSortPriority):
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_runners(runner_name, betfair_id, betfair_sort_priority) 
                VALUES 
                    (%(runnerName)s, %(betfairId)s, %(betfairSortPriority)s)
            """,
            parameters={
                'runnerName': clean_text(text=runnerName),
                'betfairId': int(betfairId),
                'betfairSortPriority': int(betfairSortPriority)
            }
        )

    def insert_runner_status(self, runnerStatus):
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_runner_status(status) 
                VALUES 
                    (%(status)s)
            """,
            parameters={'status': clean_text(text=runnerStatus)}
        )

    def insert_runner_status_update(self, unixTimestamp, statusId, betfairMarketTableId, betfairRunnerTableId):
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_runner_status_updates
                    (
                        unix_timestamp, status_id, betfair_market_table_id, betfair_runner_table_id
                    ) 
                VALUES 
                    (
                        %(unixTimestamp)s, %(statusId)s, %(betfairMarketTableId)s, %(betfairRunnerTableId)s
                    )
            """,
            parameters={
                'unixTimestamp': int(unixTimestamp),
                'statusId': int(statusId),
                'betfairMarketTableId': int(betfairMarketTableId),
                'betfairRunnerTableId': int(betfairRunnerTableId)
            }
        )

    def insert_last_traded_price(self, unixTimestamp, betfairMarketTableId, betfairRunnerTableId, price):
        return self.run_update_query(
            query="""
                INSERT INTO
                    tbl_betfair_last_traded_price
                    (
                        unix_timestamp, betfair_market_table_id, betfair_runner_table_id, price
                    ) 
                VALUES 
                    (
                        %(unixTimestamp)s, %(betfairMarketTableId)s, %(betfairRunnerTableId)s, %(price)s
                    )
            """,
            parameters={
                'unixTimestamp': int(unixTimestamp),
                'betfairMarketTableId': int(betfairMarketTableId),
                'betfairRunnerTableId': int(betfairRunnerTableId),
                'price': float(price)
            }
        )
