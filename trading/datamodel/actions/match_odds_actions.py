from typing import Tuple

from trading.datamodel.actions.base_actions import BaseActions


class MatchOddsActions(BaseActions):

    DO_NOTHING = "doNothing"
    HOME_BACK = "homeBack"
    AWAY_BACK = "awayBack"
    DRAW_BACK = "drawBack"
    HOME_LAY = "homeLay"
    AWAY_LAY = "awayLay"
    DRAW_LAY = "drawLay"

    def get_all_actions(self) -> Tuple[str, str, str, str, str, str, str]:
        return self.DO_NOTHING, self.HOME_BACK, self.AWAY_BACK, self.DRAW_BACK, self.HOME_LAY, self.AWAY_LAY, self.DRAW_LAY
