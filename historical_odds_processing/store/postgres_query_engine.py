from easy_postgres_engine import PostgresEngine


class PostgresQueryEngine(PostgresEngine):

    def __init__(self, user, password, databaseName='betfair_odds_data', host='localhost', port=5432):
        super().__init__(user=user, password=password, databaseName=databaseName, host=host, port=port)

    def get_betting_type_index(self, bettingTypeName):
        output = self.run_select_query(
            query="""
                SELECT
                    id
                FROM
                    tbl_betfair_betting_types
                WHERE
                    betting_type_name = %(bettingTypeName)s
            """,
            parameters={'bettingTypeName': bettingTypeName.strip()}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return int(output['id'].values[0])
        else:
            raise ValueError(f'No single id found for {bettingTypeName} in tbl_betfair_betting_types')

    def get_market_type_index(self, marketType):
        output = self.run_select_query(
            query="""
                SELECT
                    id
                FROM
                    tbl_betfair_market_types
                WHERE
                    market_type = %(marketType)s
            """,
            parameters={'marketType': marketType}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return int(output['id'].values[0])
        else:
            raise ValueError(f'No single id found for {marketType} in tbl_betfair_market_types')

    def get_market_status_index(self, marketStatus):
        output = self.run_select_query(
            query="""
                SELECT
                    id
                FROM
                    tbl_betfair_market_status
                WHERE
                    market_status_name = %(marketStatus)s
            """,
            parameters={'marketStatus': marketStatus}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return int(output['id'].values[0])
        else:
            raise ValueError(f'No single id found for {marketStatus} in tbl_betfair_market_status')

    def get_country_code_index(self, countryCode):
        output = self.run_select_query(
            query="""
                SELECT
                    id
                FROM
                    tbl_betfair_country_codes
                WHERE
                    country_code = %(countryCode)s
            """,
            parameters={'countryCode': countryCode}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return int(output['id'].values[0])
        else:
            raise ValueError(f'No single id found for {countryCode} in tbl_betfair_country_codes')

    def get_timezone_index(self, timezone):
        output = self.run_select_query(
            query="""
                SELECT
                    id
                FROM
                    tbl_betfair_timezones
                WHERE
                    timezone = %(timezone)s
            """,
            parameters={'timezone': timezone}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return int(output['id'].values[0])
        else:
            raise ValueError(f'No single id found for {timezone} in tbl_betfair_timezones')

    def get_market(self, betfairEventId, betfairMarketId):
        output = self.run_select_query(
            query="""
                SELECT
                    *
                FROM
                    tbl_betfair_markets
                WHERE
                    betfair_market_id = %(betfairMarketId)s AND
                    event_id = %(betfairEventId)s
            """,
            parameters={'betfairMarketId': str(betfairMarketId), 'betfairEventId': int(betfairEventId)}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return output
        else:
            raise ValueError(f'Error for event info: {betfairEventId}, market: {betfairMarketId} in tbl_betfair_markets')

    def check_last_market_definition(self, betfairMarketId):
        output = self.run_select_query(
            query="""
                SELECT
                    *
                FROM
                    tbl_betfair_market_definitions
                WHERE
                    betfair_market_table_id = %(betfairMarketId)s
                ORDER BY
                    unix_timestamp DESC
                LIMIT 1
            """,
            parameters={'betfairMarketId': int(betfairMarketId)}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return output
        else:
            raise ValueError(f'No last market definition found for {betfairMarketId} in tbl_betfair_market_definitions')

    def get_runner_index_by_name(self, runnerName):
        output = self.run_select_query(
            query="""
                SELECT
                    id
                FROM
                    tbl_betfair_runners
                WHERE
                    runner_name = %(runnerName)s
            """,
            parameters={'runnerName': runnerName}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return int(output['id'].values[0])
        else:
            return output['id'].values

    def get_runner_index_by_betfair_id(self, betfairRunnerId):
        output = self.run_select_query(
            query="""
                SELECT
                    id
                FROM
                    tbl_betfair_runners
                WHERE
                    betfair_id = %(betfairRunnerId)s
            """,
            parameters={'betfairRunnerId': betfairRunnerId}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return int(output['id'].values[0])
        else:
            raise ValueError(f'No single id found for {betfairRunnerId} in tbl_betfair_runners')

    def get_runner_status_index(self, runnerStatus):
        output = self.run_select_query(
            query="""
                SELECT
                    id
                FROM
                    tbl_betfair_runner_status
                WHERE
                    status = %(runnerStatus)s
            """,
            parameters={'runnerStatus': runnerStatus}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return int(output['id'].values[0])
        else:
            raise ValueError(f'No single id found for {runnerStatus} in tbl_betfair_runner_status')

    def get_last_runner_status_info(self, betfairMarketId, betfairRunnerTableId):
        output = self.run_select_query(
            query="""
                   SELECT
                       unix_timestamp, status_id
                   FROM
                       tbl_betfair_runner_status_updates
                   WHERE
                       betfair_market_id = %(betfairMarketId)s AND
                       betfair_runner_table_id = %(betfairRunnerTableId)s
                    ORDER BY
                        unix_timestamp DESC
                    LIMIT 1
               """,
            parameters={'betfairMarketId': betfairMarketId, 'betfairRunnerTableId': int(betfairRunnerTableId)}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return output
        else:
            raise ValueError(
                f'No last status found for runnerId {betfairRunnerTableId}, market: {betfairMarketId} in tbl_betfair_runner_status_updates')

    def get_last_traded_price(self, betfairMarketId, betfairRunnerTableId):
        output = self.run_select_query(
            query="""
                SELECT
                    unix_timestamp, price
                FROM
                    tbl_betfair_last_traded_price
                WHERE
                    betfair_market_table_id = %(betfairMarketId)s AND
                    betfair_runner_table_id = %(betfairRunnerTableId)s
                ORDER BY
                    unix_timestamp DESC
                LIMIT 1
               """,
            parameters={'betfairMarketId': betfairMarketId, 'betfairRunnerTableId': int(betfairRunnerTableId)}
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return output
        else:
            raise ValueError(f'No last traded price found for runnerId {betfairRunnerTableId}, market: {betfairMarketId} in tbl_betfair_last_traded_price')

    def get_odds_time_series(self, betfairMarketId, betfairRunnerTableId=None):
        runnerTableIdClause = 'AND betfair_runner_table_id = %(betfairRunnerTableId)s'
        parameters = {'betfairMarketId': betfairMarketId}
        if betfairRunnerTableId is not None:
            parameters.update({'betfairRunnerTableId': int(betfairRunnerTableId)})
        return self.run_select_query(
            query=f"""
                SELECT
                    *
                FROM
                    vw_last_traded_bets
                WHERE
                    betfair_market_id = %(betfairMarketId)s 
                    {runnerTableIdClause if betfairRunnerTableId is not None else ''}
                ORDER BY
                    unix_timestamp ASC
            """,
            parameters=parameters
        )

    def get_market_id(self, eventId, marketTypeId):
        output = self.run_select_query(
            query="""
                SELECT
                    *
                FROM
                    tbl_betfair_markets
                WHERE
                    event_id = %(eventId)s AND
                    market_type = %(marketTypeId)s
            """,
            parameters={
                'eventId': int(eventId),
                'marketTypeId': int(marketTypeId)
            }
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return output['betfair_market_id'][0]
        else:
            raise ValueError(f'Error for market: {marketTypeId} in event {eventId}')

    def get_event_id_start_time(self, betfairEventId):
        output = self.run_select_query(
            query="""
                SELECT
                    *
                FROM
                    tbl_betfair_market_definitions
                WHERE
                    event_id = %(betfairEventId)s AND
                    in_play = True
                ORDER BY
                    unix_timestamp ASC
                LIMIT 1
            """,
            parameters={
                'betfairEventId': int(betfairEventId)
            }
        )
        if len(output) == 0:
            return None
        elif len(output) == 1:
            return int(output['unix_timestamp'][0])
        else:
            raise ValueError(f'Error getting start time for event: {betfairEventId}')
