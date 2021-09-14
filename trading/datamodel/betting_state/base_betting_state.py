from __future__ import annotations
from abc import ABC, abstractmethod

import numpy as np

from trading.datamodel.discounted_reward import DiscountedReward
from utils.betting_functions import calculate_back_bet_to_trade_out
from utils.betting_functions import calculate_lay_bet_to_trade_out


class BaseBettingState(ABC):
    def __init__(
        self,
        duplicateActionPenalty: float,
        onlyPositiveCashout: bool,
        discountFactor: float,
    ):
        self.duplicateActionPenalty = duplicateActionPenalty
        self.discountFactor = discountFactor
        self.onlyPositiveCashout = onlyPositiveCashout

        self.backStake = 1
        self.layStake = 1

    @property
    @abstractmethod
    def backBets(self) -> np.array:
        pass

    @property
    @abstractmethod
    def layBets(self) -> np.array:
        pass

    def _calculate_back_trade_out_winnings(self, layOdds: float, layStake: float, currentBackOdds: float) -> DiscountedReward:
        if currentBackOdds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        backBetToPlace = calculate_back_bet_to_trade_out(layOdds=layOdds, layStake=layStake, currentBackOdds=currentBackOdds)
        layLiability = (layOdds - 1) * layStake
        winningsIfNotOutcomeWin = layStake - backBetToPlace
        winningsIfOutcomeWin = (backBetToPlace * currentBackOdds) - backBetToPlace - layLiability
        guaranteedWinnings = min(winningsIfNotOutcomeWin, winningsIfOutcomeWin)
        return DiscountedReward(reward=guaranteedWinnings, discount=0)

    def _calculate_lay_trade_out_winnings(self, backOdds: float, backStake: float, currentLayOdds: float) -> DiscountedReward:
        if currentLayOdds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        layBetToPlace = calculate_lay_bet_to_trade_out(backOdds=backOdds, backStake=backStake, currentLayOdds=currentLayOdds)
        layLiability = (currentLayOdds - 1) * layBetToPlace
        winningsIfNotOutcomeWin = layBetToPlace - backStake
        winningsIfOutcomeWin = (backStake * backOdds) - backStake - layLiability
        guaranteedWinnings = min(winningsIfNotOutcomeWin, winningsIfOutcomeWin)
        return DiscountedReward(reward=guaranteedWinnings, discount=0)

    def calculate_return(self, outcomeVector: np.array) -> float:
        if len(outcomeVector) != len(self.backBets) or len(outcomeVector) != len(self.layBets):
            raise Exception("outcomeVector is incorrect size")
        if isinstance(outcomeVector, list):
            outcomeVector = np.array(outcomeVector)
        losingResults = np.array(outcomeVector != 1, dtype=np.int32)
        backReturnMinusStake = (self.backBets * self.backStake) - self.backStake
        potentialBackWinnings = np.where(backReturnMinusStake > 0, backReturnMinusStake, 0)
        backReturn = np.sum(outcomeVector * potentialBackWinnings)
        backLosses = np.sum(losingResults * (np.array(self.backBets > 0, dtype=np.float32)) * self.backStake)
        layReturn = np.sum(losingResults * (np.array(self.layBets > 0, dtype=np.float32)) * self.layStake)
        layLossesMinusStake = (self.layBets * self.layStake) - self.layStake
        potentialLayLosses = np.where(layLossesMinusStake > 0, layLossesMinusStake, 0)
        layLosses = np.sum(outcomeVector * potentialLayLosses)
        return backReturn + layReturn - backLosses - layLosses

    def calculate_return_for_discounted_rl(self, outcomeVector: np.array) -> float:
        # losses are already in the model as discounted negative rewards
        if len(outcomeVector) != len(self.backBets) or len(outcomeVector) != len(self.layBets):
            raise Exception("outcomeVector is incorrect size")
        if isinstance(outcomeVector, list):
            outcomeVector = np.array(outcomeVector)
        losingResults = np.array(outcomeVector != 1, dtype=np.int32)
        backReturn = np.sum(outcomeVector * (self.backBets * self.backStake))
        layReturn = np.sum(losingResults * (np.array(self.layBets, dtype=np.float32)) * self.layStake)
        return backReturn + layReturn

    @abstractmethod
    def reset(self) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} must implement `reset`")

    @abstractmethod
    def get_state_observation(self) -> np.array:
        raise NotImplementedError(f"{self.__class__.__name__} must implement `get_state_observation`")

    @abstractmethod
    def from_saved_state(self, **kwargs) -> BaseBettingState:
        raise NotImplementedError(f"{self.__class__.__name__} must implement `from_saved_state`")
