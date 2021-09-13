from abc import ABC, abstractmethod

import pandas as pd
import numpy as np

from trading.datamodel.environment_step import EnvironmentStep
from trading.datamodel.outcomes.base_outcome import BaseOutcome


class BaseOddsSeries(ABC):
    def __init__(
        self, oddsDataframe: pd.DataFrame, endOutcome: BaseOutcome, doCycle: bool = False, backScalingFactor: float = 0.99
    ):
        self.oddsDataframe = oddsDataframe
        self.endOutcome = endOutcome
        self.doCycle = doCycle
        self.backScalingFactor = backScalingFactor
        self.episodeEnded = False
        self.groupedOddsUpdates = None
        self.orderedTimestamps = None
        self.totalNumSteps = None
        self.currentStepNumber = 0
        self.jitterOddsScale = None

    def get_end_outcome_vector(self) -> np.array:
        return np.array(self.endOutcome)

    def set_jitter_odds_scale(self, scale: float) -> None:
        self.jitterOddsScale = scale

    def apply_jitter_to_odds(self, odds: np.array) -> np.array:
        # jitters the implied probabilities of odds and then converts them back into odds
        jitteredProbabilities = np.array(
            [np.random.normal(loc=1 / v, scale=self.jitterOddsScale) if v > 0 else 0 for v in odds]
        )
        return np.array([1 / v if v > 0 else 0 for v in jitteredProbabilities])

    def initialize(self) -> None:
        self.groupedOddsUpdates = {
            timestamp: updateDataframe for timestamp, updateDataframe in self.oddsDataframe.groupby("unix_timestamp")
        }
        self.orderedTimestamps = sorted(self.groupedOddsUpdates.keys())
        self.totalNumSteps = len(self.orderedTimestamps)

    @property
    def isValid(self) -> bool:
        return len(self.oddsDataframe) > 0

    def get_step(self) -> np.array:
        updateTimestamp = self.orderedTimestamps[self.currentStepNumber]
        groupedDataframeUpdate = self.groupedOddsUpdates[updateTimestamp]
        self.currentStepNumber += 1
        self.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        layOddsVector = self._get_odds_vector()
        backOddsVector = (
            layOddsVector * self.backScalingFactor
        )  # always assume prices are for lay bets (and back would be lower)
        if self.jitterOddsScale is not None:
            backOddsVector = self.apply_jitter_to_odds(odds=backOddsVector)
            layOddsVector = self.apply_jitter_to_odds(odds=layOddsVector)
        matchStep = EnvironmentStep(offeredOdds=np.concatenate((backOddsVector, layOddsVector)))
        if self.currentStepNumber == self.totalNumSteps:
            if not self.doCycle:
                self.episodeEnded = True
            else:

                self.reset()
        return matchStep.get_numpy_array()

    @abstractmethod
    def update_step(self, groupedDataframeUpdate) -> None:
        pass

    @abstractmethod
    def _get_odds_vector(self) -> np.array:
        pass

    def reset(self) -> None:
        self.currentStepNumber = 0
        self.episodeEnded = False
        self._reset()

    @abstractmethod
    def _reset(self) -> None:
        pass
