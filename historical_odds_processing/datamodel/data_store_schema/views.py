VW_BETFAIR_MARKETS = """
    CREATE OR REPLACE VIEW vw_betfair_markets AS
        SELECT
            tbl_betfair_markets.betfair_market_id as betfair_market_id,
            tbl_betfair_markets.event_id as event_id,
            tbl_betfair_markets.event_name as event_name,
            tbl_betfair_betting_types.betting_type_name as betting_type,
            tbl_betfair_betting_types.id as betting_type_id,
            tbl_betfair_market_types.market_type as market_type,
            tbl_betfair_market_types.id as market_type_id,
            tbl_betfair_country_codes.country_code as country_code,
            tbl_betfair_country_codes.id as country_code_id,
            tbl_betfair_timezones.timezone as timezone
        FROM
            tbl_betfair_markets
        LEFT JOIN
            tbl_betfair_betting_types
        ON
            tbl_betfair_markets.betting_type = tbl_betfair_betting_types.id
        LEFT JOIN
            tbl_betfair_market_types
        ON
            tbl_betfair_markets.market_type = tbl_betfair_market_types.id
        LEFT JOIN
            tbl_betfair_country_codes
        ON
            tbl_betfair_markets.country_code = tbl_betfair_country_codes.id
        LEFT JOIN
            tbl_betfair_timezones
        ON
            tbl_betfair_markets.timezone = tbl_betfair_timezones.id
        ;
"""

VW_LAST_TRADED_BETS = """
    CREATE OR REPLACE VIEW vw_last_traded_bets AS
        SELECT
            tbl_betfair_runners.runner_name as runner_name,
            tbl_betfair_runners.betfair_id as runner_betfair_id,
            tbl_betfair_last_traded_price.unix_timestamp as unix_timestamp,
            tbl_betfair_last_traded_price.betfair_market_id as betfair_market_id,
            vw_betfair_markets.market_type as market_type,
            vw_betfair_markets.market_type_id as market_type_id,
            vw_betfair_markets.event_name as event_name,
            tbl_betfair_last_traded_price.event_id as event_id,
            tbl_betfair_last_traded_price.price as price,
            vw_betfair_markets.betting_type as betting_type,
            vw_betfair_markets.betting_type_id as betting_type_id,
            vw_betfair_markets.country_code as country_code,
            vw_betfair_markets.country_code_id as country_code_id,
            vw_betfair_markets.timezone as timezone
        FROM
            tbl_betfair_last_traded_price
        LEFT JOIN
            tbl_betfair_runners
        ON
            tbl_betfair_last_traded_price.betfair_runner_table_id = tbl_betfair_runners.id
        LEFT JOIN
            vw_betfair_markets
        ON
            tbl_betfair_last_traded_price.betfair_market_id = vw_betfair_markets.betfair_market_id
        AND
            tbl_betfair_last_traded_price.event_id = vw_betfair_markets.event_id
"""

ALL_VIEWS = [VW_BETFAIR_MARKETS, VW_LAST_TRADED_BETS]
