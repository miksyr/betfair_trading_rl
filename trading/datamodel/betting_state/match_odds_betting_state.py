from __future__ import annotations

import numpy as np

from trading.datamodel.betting_state.base_betting_state import BaseBettingState
from trading.datamodel.discounted_reward import DiscountedReward


class MatchOddsBettingState(BaseBettingState):

    # assumes full liquidity for the sake of ease
    # assumes that can cash out of lay/back bets and then still have the same odds to place more bet
    # back or lay bets attempt to win £1 as a unit stake, to normalise the outcome of potential actions

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
        self.homeBackOdds = 0
        self.awayBackOdds = 0
        self.drawBackOdds = 0
        self.homeLayOdds = 0
        self.awayLayOdds = 0
        self.drawLayOdds = 0

    @property
    def backBets(self) -> np.array:
        return np.array([self.homeBackOdds, self.awayBackOdds, self.drawBackOdds], dtype=np.float32)

    @property
    def layBets(self) -> np.array:
        return np.array([self.homeLayOdds, self.awayLayOdds, self.drawLayOdds], dtype=np.float32)

    def place_home_back_bet(self, odds: float) -> DiscountedReward:
        if self.homeBackOdds > 0 or odds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        if self.homeLayOdds == 0:
            self.homeBackOdds = odds
            return DiscountedReward(reward=-self.backStake, discount=self.discountFactor)
        else:
            if self.onlyPositiveCashout and (self.homeLayOdds >= odds):
                return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
            discountedReward = self._calculate_back_trade_out_winnings(
                layOdds=self.homeLayOdds, layStake=self.layStake, currentBackOdds=odds
            )
            discountedReward.update_reward(
                rewardIncrease=self.homeLayOdds - self.layStake
            )  # giving back original liability for discounted lay bet
            self.homeLayOdds = 0
            return discountedReward

    def place_away_back_bet(self, odds: float) -> DiscountedReward:
        if self.awayBackOdds > 0 or odds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        if self.awayLayOdds == 0:
            self.awayBackOdds = odds
            return DiscountedReward(reward=-self.backStake, discount=self.discountFactor)
        else:
            if self.onlyPositiveCashout and (self.awayLayOdds >= odds):
                return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
            discountedReward = self._calculate_back_trade_out_winnings(
                layOdds=self.awayLayOdds, layStake=self.layStake, currentBackOdds=odds
            )
            discountedReward.update_reward(rewardIncrease=self.awayLayOdds - self.layStake)
            self.awayLayOdds = 0
            return discountedReward

    def place_draw_back_bet(self, odds: float) -> DiscountedReward:
        if self.drawBackOdds > 0 or odds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        if self.drawLayOdds == 0:
            self.drawBackOdds = odds
            return DiscountedReward(reward=-self.backStake, discount=self.discountFactor)
        else:
            if self.onlyPositiveCashout and (self.drawLayOdds >= odds):
                return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
            discountedReward = self._calculate_back_trade_out_winnings(
                layOdds=self.drawLayOdds, layStake=self.layStake, currentBackOdds=odds
            )
            discountedReward.update_reward(rewardIncrease=self.drawLayOdds - self.layStake)
            self.drawLayOdds = 0
            return discountedReward

    def place_home_lay_bet(self, odds: float) -> DiscountedReward:
        if self.homeLayOdds > 0 or odds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        if self.homeBackOdds == 0:
            self.homeLayOdds = odds
            return DiscountedReward(
                reward=(-self.layStake * odds) + self.layStake,
                discount=self.discountFactor,
            )
        else:
            if self.onlyPositiveCashout and (self.homeBackOdds <= odds):
                return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
            discountedReward = self._calculate_lay_trade_out_winnings(
                backOdds=self.homeBackOdds,
                backStake=self.backStake,
                currentLayOdds=odds,
            )
            discountedReward.update_reward(rewardIncrease=self.backStake)
            self.homeBackOdds = 0
            return discountedReward

    def place_away_lay_bet(self, odds: float) -> DiscountedReward:
        if self.awayLayOdds > 0 or odds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        if self.awayBackOdds == 0:
            self.awayLayOdds = odds
            return DiscountedReward(
                reward=(-self.layStake * odds) + self.layStake,
                discount=self.discountFactor,
            )
        else:
            if self.onlyPositiveCashout and (self.awayBackOdds <= odds):
                return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
            discountedReward = self._calculate_lay_trade_out_winnings(
                backOdds=self.awayBackOdds,
                backStake=self.backStake,
                currentLayOdds=odds,
            )
            discountedReward.update_reward(rewardIncrease=self.backStake)
            self.awayBackOdds = 0
            return discountedReward

    def place_draw_lay_bet(self, odds: float) -> DiscountedReward:
        if self.drawLayOdds > 0 or odds == 0:
            return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
        if self.drawBackOdds == 0:
            self.drawLayOdds = odds
            return DiscountedReward(
                reward=(-self.layStake * odds) + self.layStake,
                discount=self.discountFactor,
            )
        else:
            if self.onlyPositiveCashout and (self.drawBackOdds <= odds):
                return DiscountedReward(reward=self.duplicateActionPenalty, discount=0)
            discountedReward = self._calculate_lay_trade_out_winnings(
                backOdds=self.drawBackOdds,
                backStake=self.backStake,
                currentLayOdds=odds,
            )
            discountedReward.update_reward(rewardIncrease=self.backStake)
            self.drawBackOdds = 0
            return discountedReward

    def get_state_observation(self) -> np.array:
        return np.round(
            np.array(
                [
                    self.homeBackOdds,
                    self.awayBackOdds,
                    self.drawBackOdds,
                    self.homeLayOdds,
                    self.awayLayOdds,
                    self.drawLayOdds,
                ],
                dtype=np.float32,
            ),
            decimals=2,
        )

    def reset(self) -> None:
        self.homeBackOdds = 0
        self.awayBackOdds = 0
        self.drawBackOdds = 0
        self.homeLayOdds = 0
        self.awayLayOdds = 0
        self.drawLayOdds = 0

    def from_saved_state(
        self,
        homeBackOdds: float,
        awayBackOdds: float,
        drawBackOdds: float,
        homeLayOdds: float,
        awayLayOdds: float,
        drawLayOdds: float,
    ) -> MatchOddsBettingState:
        if homeBackOdds > 0:
            self.place_home_back_bet(odds=homeBackOdds)
        if awayBackOdds > 0:
            self.place_away_back_bet(odds=awayBackOdds)
        if drawBackOdds > 0:
            self.place_draw_back_bet(odds=drawBackOdds)
        if homeLayOdds > 0:
            self.place_home_lay_bet(odds=homeLayOdds)
        if awayLayOdds > 0:
            self.place_away_lay_bet(odds=awayLayOdds)
        if drawLayOdds > 0:
            self.place_draw_lay_bet(odds=drawLayOdds)
        return self
