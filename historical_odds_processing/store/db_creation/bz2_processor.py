import bz2
import logging
import json
import pickle

from datetime import datetime
from pathlib import Path
from tqdm.auto import tqdm
from typing import Any, Dict, Sequence, Tuple, Union

from historical_odds_processing.datamodel.constants import BETFAIR_DATETIME_FORMAT
from historical_odds_processing.datamodel.constants import BETFAIR_MARKET_DEFINITION_TAG
from historical_odds_processing.datamodel.constants import BETFAIR_RUNNER_CHANGE_TAG
from historical_odds_processing.store.db_creation.csv_output_handler import CSVOutputHandler
from historical_odds_processing.store.db_creation.output_filenames import OutputFilenames
from historical_odds_processing.utils.runner_identifier import get_runner_identifier


class BZ2Processor:
    def __init__(
        self,
        bz2FilePaths: Sequence[str],
        marketInfoHandler: CSVOutputHandler,
        marketDefinitionHandler: CSVOutputHandler,
        runnerStatusUpdateHandler: CSVOutputHandler,
        lastTradedPriceHandler: CSVOutputHandler,
        countryCodeFilter: Sequence[str] = None,
    ):
        self.bz2FilePaths = bz2FilePaths
        self.marketInfoHandler = marketInfoHandler
        self.marketDefinitionHandler = marketDefinitionHandler
        self.runnerStatusUpdateHandler = runnerStatusUpdateHandler
        self.lastTradedPriceHandler = lastTradedPriceHandler
        self.countryCodeFilter = countryCodeFilter
        self.bettingTypes = set()
        self.marketTypes = set()
        self.marketStatuses = set()
        self.countryCodes = set()
        self.timezones = set()
        self.runners = set()
        self.runnerStatus = set()
        self.lastRunnerStatus = {}

    def process_market_info(self, marketChangeData: Dict[str, Any]) -> Tuple[str, int]:
        marketDefinitionData = marketChangeData[BETFAIR_MARKET_DEFINITION_TAG]
        bettingType = marketDefinitionData["bettingType"]
        marketType = marketDefinitionData["marketType"]
        countryCode = marketDefinitionData["countryCode"]
        timezone = marketDefinitionData["timezone"]

        self.bettingTypes.add(bettingType)
        self.marketTypes.add(marketType)
        self.countryCodes.add(countryCode)
        self.timezones.add(timezone)

        betfairMarketId = str(marketChangeData["id"])
        eventId = int(marketDefinitionData["eventId"])
        self.marketInfoHandler.add(
            data={
                "betfair_market_id": str(betfairMarketId),
                "event_id": int(eventId),
                "event_name": marketDefinitionData["eventName"],
                "event_type_id": int(marketDefinitionData["eventTypeId"]),
                "betting_type": bettingType,
                "market_type": marketType,
                "country_code": countryCode,
                "timezone": timezone,
            }
        )
        return betfairMarketId, eventId

    def process_market_definition(self, betfairMarketId: str, unixTimestamp: int, marketChangeData: Dict[str, Any]) -> None:
        marketDefinitionData = marketChangeData[BETFAIR_MARKET_DEFINITION_TAG]
        marketStatus = marketDefinitionData.get("status")
        self.marketStatuses.add(marketStatus)
        self.marketDefinitionHandler.add(
            data={
                "betfair_market_id": str(betfairMarketId),
                "event_id": int(marketDefinitionData["eventId"]),
                "unix_timestamp": int(unixTimestamp),
                "version": int(marketDefinitionData.get("version")),
                "bsp_market": int(marketDefinitionData.get("bspMarket")),
                "turn_in_play_enabled": int(marketDefinitionData.get("turnInPlayEnabled")),
                "persistence_enabled": int(marketDefinitionData.get("persistenceEnabled")),
                "market_base_rate": float(marketDefinitionData.get("marketBaseRate")),
                "num_winners": int(marketDefinitionData.get("numberOfWinners")),
                "market_start_time": datetime.strptime(
                    marketDefinitionData["marketTime"].split(".")[0], BETFAIR_DATETIME_FORMAT
                ),
                "market_suspend_time": datetime.strptime(
                    marketDefinitionData["suspendTime"].split(".")[0], BETFAIR_DATETIME_FORMAT
                ),
                "bsp_reconciled": int(marketDefinitionData.get("bspReconciled")),
                "market_is_complete": int(marketDefinitionData.get("complete")),
                "in_play": int(marketDefinitionData.get("inPlay")),
                "cross_matching": int(marketDefinitionData.get("crossMatching")),
                "runners_voidable": int(marketDefinitionData.get("runnersVoidable")),
                "num_active_runners": int(marketDefinitionData.get("numberOfActiveRunners")),
                "bet_delay": int(marketDefinitionData.get("betDelay")),
                "market_status": marketStatus,
                "regulators": marketDefinitionData.get("regulators"),
                "discount_allowed": int(marketDefinitionData.get("discountAllowed")),
                "open_date": datetime.strptime(marketDefinitionData["openDate"].split(".")[0], BETFAIR_DATETIME_FORMAT),
            }
        )

    def process_runners(
        self, runners: Sequence[Dict[str, Any]], unixTimestamp: int, betfairMarketId: str, eventId: int
    ) -> Dict[str, str]:
        runnerIdentifierDict = {}
        for runner in runners:
            runnerIdentifier = get_runner_identifier(runner["name"], runner["id"])
            runnerIdentifierDict[runner["id"]] = runnerIdentifier
            runnerStatus = runner["status"]
            self.runners.add(runnerIdentifier)
            self.runnerStatus.add(runnerStatus)
            runnerInstance = get_runner_identifier(runner["name"], runner["id"], betfairMarketId, eventId)
            if runnerInstance not in self.lastRunnerStatus:
                self.lastRunnerStatus.update({runnerInstance: self.runnerStatus})
                self.runnerStatusUpdateHandler.add(
                    data={
                        "unix_timestamp": int(unixTimestamp),
                        "status_id": runnerStatus,
                        "betfair_runner_table_id": runnerIdentifier,
                        "betfair_market_id": str(betfairMarketId),
                        "event_id": int(eventId),
                    }
                )
            else:
                if self.lastRunnerStatus[runnerInstance] != runnerStatus:
                    self.lastRunnerStatus.update({runnerInstance: self.runnerStatus})
                    self.runnerStatusUpdateHandler.add(
                        data={
                            "unix_timestamp": int(unixTimestamp),
                            "status_id": runnerStatus,
                            "betfair_runner_table_id": runnerIdentifier,
                            "betfair_market_id": str(betfairMarketId),
                            "event_id": int(eventId),
                        }
                    )
        return runnerIdentifierDict

    def process_last_traded_price(
        self, unixTimestamp: int, betfairMarketId: str, eventId: int, runnerIdentifier: str, price: float
    ) -> None:
        self.lastTradedPriceHandler.add(
            data={
                "unix_timestamp": int(unixTimestamp),
                "betfair_market_id": betfairMarketId,
                "event_id": int(eventId),
                "betfair_runner_table_id": runnerIdentifier,
                "price": float(price),
            }
        )

    def save_outputs(self, outputDirectory: Union[str, Path]) -> None:
        pickle.dump(self.bettingTypes, open(f"{outputDirectory}/{OutputFilenames.BETTING_TYPES}.pkl", "wb"))
        pickle.dump(self.marketTypes, open(f"{outputDirectory}/{OutputFilenames.MARKET_TYPES}.pkl", "wb"))
        pickle.dump(self.marketStatuses, open(f"{outputDirectory}/{OutputFilenames.MARKET_STATUS}.pkl", "wb"))
        pickle.dump(self.countryCodes, open(f"{outputDirectory}/{OutputFilenames.COUNTRY_CODES}.pkl", "wb"))
        pickle.dump(self.timezones, open(f"{outputDirectory}/{OutputFilenames.TIMEZONES}.pkl", "wb"))
        pickle.dump(self.runners, open(f"{outputDirectory}/{OutputFilenames.RUNNERS}.pkl", "wb"))
        pickle.dump(self.runnerStatus, open(f"{outputDirectory}/{OutputFilenames.RUNNER_STATUS}.pkl", "wb"))

    def process_files(self, outputDirectory: Union[str, Path]) -> None:
        Path(outputDirectory).mkdir(parents=True, exist_ok=True)
        for filePath in tqdm(self.bz2FilePaths):
            try:
                with bz2.open(filename=filePath, mode="rb") as bz2file:
                    countryCode = None
                    betfairMarketId = None
                    eventId = None
                    runnerIdentifierDict = None
                    for line in bz2file:
                        info = json.loads(line)
                        unixTimestamp = int(info["pt"] / 1000)
                        marketChange = info["mc"][0]
                        if BETFAIR_MARKET_DEFINITION_TAG in marketChange:
                            countryCode = marketChange[BETFAIR_MARKET_DEFINITION_TAG].get("countryCode", countryCode)
                            if self.countryCodeFilter is not None and countryCode not in self.countryCodeFilter:
                                break

                            if betfairMarketId is None:
                                betfairMarketId, eventId = self.process_market_info(marketChangeData=marketChange)

                            self.process_market_definition(
                                unixTimestamp=unixTimestamp, betfairMarketId=betfairMarketId, marketChangeData=marketChange
                            )

                            runnerIdentifierDict = self.process_runners(
                                runners=marketChange["marketDefinition"]["runners"],
                                unixTimestamp=unixTimestamp,
                                betfairMarketId=betfairMarketId,
                                eventId=eventId,
                            )
                        if BETFAIR_RUNNER_CHANGE_TAG in marketChange:
                            for priceChange in marketChange["rc"]:
                                self.process_last_traded_price(
                                    unixTimestamp=unixTimestamp,
                                    betfairMarketId=betfairMarketId,
                                    eventId=eventId,
                                    runnerIdentifier=runnerIdentifierDict[priceChange["id"]],
                                    price=priceChange["ltp"],
                                )
            except Exception as ex:
                logging.exception(f"Error processing {filePath}:\n{ex}")
        self.save_outputs(outputDirectory=outputDirectory)
