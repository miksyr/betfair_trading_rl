from typing import Optional, Union

import numpy as np

from trading.datamodel.actions.match_odds_actions import MatchOddsActions
from trading.datamodel.betting_state.match_odds_betting_state import MatchOddsBettingState
from trading.datamodel.constants import MATCH_ODDS_NORMALISATION_CONSTANT
from trading.datamodel.discounted_reward import DiscountedReward
from trading.datamodel.environment.base_environment import BaseEnvironment
from trading.datamodel.odds_series.match_odds_series import MatchOddsSeries


class MatchOddsEnvironment(BaseEnvironment):
    def __init__(
        self,
        oddSeries: MatchOddsSeries,
        rewardDiscountFactor: float,
        inactionPenalty: float,
        onlyPositiveCashout: bool,
        actions: Optional[MatchOddsActions] = None,
    ):
        super().__init__(
            oddSeries=oddSeries,
            rewardDiscountFactor=rewardDiscountFactor,
            inactionPenalty=inactionPenalty,
        )
        self.actions = actions or MatchOddsActions()
        self.actionsNameToIdMap = self.actions.get_action_name_to_id_mapping()
        self._state = MatchOddsBettingState(discountFactor=rewardDiscountFactor, onlyPositiveCashout=onlyPositiveCashout)

    def _get_odds_normalisation_constant(self) -> Union[int, float]:
        return MATCH_ODDS_NORMALISATION_CONSTANT

    def _action_processing(self, action: int, offeredOdds: np.array) -> DiscountedReward:
        if action == self.actionsNameToIdMap[self.actions.HOME_BACK]:
            return self._state.place_home_back_bet(odds=offeredOdds[0])
        elif action == self.actionsNameToIdMap[self.actions.AWAY_BACK]:
            return self._state.place_away_back_bet(odds=offeredOdds[1])
        elif action == self.actionsNameToIdMap[self.actions.DRAW_BACK]:
            return self._state.place_draw_back_bet(odds=offeredOdds[2])
        elif action == self.actionsNameToIdMap[self.actions.HOME_LAY]:
            return self._state.place_home_lay_bet(odds=offeredOdds[3])
        elif action == self.actionsNameToIdMap[self.actions.AWAY_LAY]:
            return self._state.place_away_lay_bet(odds=offeredOdds[4])
        elif action == self.actionsNameToIdMap[self.actions.DRAW_LAY]:
            return self._state.place_draw_lay_bet(odds=offeredOdds[5])
        else:
            raise ValueError(f"unknown action: {action}")
