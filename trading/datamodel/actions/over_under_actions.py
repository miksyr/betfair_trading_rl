from typing import Tuple

from trading.datamodel.actions.base_actions import BaseActions


class OverUnderActions(BaseActions):

    DO_NOTHING = "doNothing"
    OVER_BACK = "overBack"
    UNDER_BACK = "underBack"
    OVER_LAY = "overLay"
    UNDER_LAY = "underLay"

    def get_all_actions(self) -> Tuple[str, str, str, str, str]:
        return self.DO_NOTHING, self.OVER_BACK, self.UNDER_BACK, self.OVER_LAY, self.UNDER_LAY
