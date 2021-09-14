from typing import Optional, Union

import numpy as np
import tensorflow as tf

from trading.datamodel.actions.over_under_actions import OverUnderActions
from trading.datamodel.betting_state.over_under_betting_state import OverUnderBettingState
from trading.datamodel.constants import OVER_UNDER_ODDS_NORMALISATION_CONSTANT
from trading.datamodel.discounted_reward import DiscountedReward
from trading.datamodel.environment.base_environment import BaseEnvironment
from trading.datamodel.odds_series.over_under_odds_series import OverUnderOddsSeries


class OverUnderEnvironment(BaseEnvironment):
    def __init__(
        self,
        oddSeries: OverUnderOddsSeries,
        rewardDiscountFactor: float,
        inactionPenalty: float,
        onlyPositiveCashout: bool,
        actions: Optional[OverUnderActions] = None,
    ):
        super().__init__(
            oddSeries=oddSeries,
            rewardDiscountFactor=rewardDiscountFactor,
            inactionPenalty=inactionPenalty,
        )
        self.actions = actions or OverUnderActions()
        self.actionsNameToIdMap = self.actions.get_action_name_to_id_mapping()
        self._state = OverUnderBettingState(discountFactor=rewardDiscountFactor, onlyPositiveCashout=onlyPositiveCashout)

    def _get_odds_normalisation_constant(self) -> Union[int, float]:
        return OVER_UNDER_ODDS_NORMALISATION_CONSTANT

    def _action_processing(self, action: int, offeredOdds: np.array) -> DiscountedReward:
        if action == self.actionsNameToIdMap[self.actions.OVER_BACK]:
            return self._state.place_over_back_bet(odds=offeredOdds[0])
        elif action == self.actionsNameToIdMap[self.actions.UNDER_BACK]:
            return self._state.place_under_back_bet(odds=offeredOdds[1])
        elif action == self.actionsNameToIdMap[self.actions.OVER_LAY]:
            return self._state.place_over_lay_bet(odds=offeredOdds[2])
        elif action == self.actionsNameToIdMap[self.actions.UNDER_LAY]:
            return self._state.place_under_lay_bet(odds=offeredOdds[3])
        else:
            raise ValueError(f"unknown action: {action}")
