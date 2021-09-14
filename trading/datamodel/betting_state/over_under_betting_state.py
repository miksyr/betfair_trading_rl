from __future__ import annotations

import numpy as np

from trading.datamodel.betting_state.base_betting_state import BaseBettingState
from trading.datamodel.discounted_reward import DiscountedReward


class OverUnderBettingState(BaseBettingState):
    def __init__(
        self,
        discountFactor: float,
        onlyPositiveCashout: bool,
        duplicateActionPenalty: float = -100.0,
    ):
        super().__init__(
            duplicateActionPenalty=duplicateActionPenalty,
            onlyPositiveCashout=onlyPositiveCashout,
            discountFactor=discountFactor,
        )
        self.overBackOdds = 0
        self.underBackOdds = 0
        self.overLayOdds = 0
        self.underLayOdds = 0

    @property
    def backBets(self) -> np.array:
        return np.array([self.overBackOdds, self.underBackOdds], dtype=np.float32)

    @property
    def layBets(self) -> np.array:
        return np.array([self.overLayOdds, self.underLayOdds], dtype=np.float32)

    def get_state_observation(self) -> np.array:
        return np.round(
            np.array(
                [
                    self.overBackOdds,
                    self.underBackOdds,
                    self.overLayOdds,
                    self.underLayOdds,
                ],
                dtype=np.float32,
            ),
            decimals=2,
        )

    def place_over_back_bet(self, odds: float) -> DiscountedReward:
        if self.overBackOdds > 0 or odds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        if self.overLayOdds == 0:
            self.overBackOdds = odds
            return DiscountedReward(reward=-self.backStake, discount=self.discountFactor)
        else:
            if self.onlyPositiveCashout and (self.overLayOdds >= odds):
                return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
            discountedReward = self._calculate_back_trade_out_winnings(
                layOdds=self.overLayOdds, layStake=self.layStake, currentBackOdds=odds
            )
            discountedReward.update_reward(
                rewardIncrease=self.overLayOdds - self.layStake
            )  # giving back original liability for discounted lay bet
            self.overLayOdds = 0
            return discountedReward

    def place_under_back_bet(self, odds: float) -> DiscountedReward:
        if self.underBackOdds > 0 or odds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        if self.underLayOdds == 0:
            self.underBackOdds = odds
            return DiscountedReward(reward=-self.backStake, discount=self.discountFactor)
        else:
            if self.onlyPositiveCashout and (self.underLayOdds >= odds):
                return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
            discountedReward = self._calculate_back_trade_out_winnings(
                layOdds=self.underLayOdds, layStake=self.layStake, currentBackOdds=odds
            )
            discountedReward.update_reward(rewardIncrease=self.underLayOdds - self.layStake)
            self.underLayOdds = 0
            return discountedReward

    def place_over_lay_bet(self, odds: float) -> DiscountedReward:
        if self.overLayOdds > 0 or odds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        if self.overBackOdds == 0:
            self.overLayOdds = odds
            return DiscountedReward(
                reward=(-self.layStake * odds) + self.layStake,
                discount=self.discountFactor,
            )
        else:
            if self.onlyPositiveCashout and (self.overBackOdds <= odds):
                return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
            discountedReward = self._calculate_lay_trade_out_winnings(
                backOdds=self.overBackOdds,
                backStake=self.backStake,
                currentLayOdds=odds,
            )
            discountedReward.update_reward(rewardIncrease=self.backStake)
            self.overBackOdds = 0
            return discountedReward

    def place_under_lay_bet(self, odds: float) -> DiscountedReward:
        if self.underLayOdds > 0 or odds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        if self.underBackOdds == 0:
            self.underLayOdds = odds
            return DiscountedReward(
                reward=(-self.layStake * odds) + self.layStake,
                discount=self.discountFactor,
            )
        else:
            if self.onlyPositiveCashout and (self.underBackOdds <= odds):
                return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
            discountedReward = self._calculate_lay_trade_out_winnings(
                backOdds=self.underBackOdds,
                backStake=self.backStake,
                currentLayOdds=odds,
            )
            discountedReward.update_reward(rewardIncrease=self.backStake)
            self.underBackOdds = 0
            return discountedReward

    def reset(self) -> None:
        self.overBackOdds = 0
        self.underBackOdds = 0
        self.overLayOdds = 0
        self.underLayOdds = 0

    def from_saved_state(
        self,
        overBackOdds: float,
        underBackOdds: float,
        overLayOdds: float,
        underLayOdds: float,
    ) -> OverUnderBettingState:
        if overBackOdds > 0:
            self.place_over_back_bet(odds=overBackOdds)
        if underBackOdds > 0:
            self.place_under_back_bet(odds=underBackOdds)
        if overLayOdds > 0:
            self.place_over_lay_bet(odds=overLayOdds)
        if underLayOdds > 0:
            self.place_under_lay_bet(odds=underLayOdds)
        return self
