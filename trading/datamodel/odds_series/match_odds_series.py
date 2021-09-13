import numpy as np
import pandas as pd

from trading.datamodel.odds_series.base_odds_series import BaseOddsSeries
from trading.datamodel.outcomes.match_outcome import MatchOutcome


class MatchOddsSeries(BaseOddsSeries):
    def __init__(
        self, oddsDataframe: pd.DataFrame, matchOutcome: MatchOutcome, doCycle: bool = False, backScalingFactor: float = 0.99
    ):
        super().__init__(
            oddsDataframe=oddsDataframe, doCycle=doCycle, endOutcome=matchOutcome, backScalingFactor=backScalingFactor
        )
        self.lastHomeOdds = 0
        self.lastAwayOdds = 0
        self.lastDrawOdds = 0

    def update_step(self, groupedDataframeUpdate: pd.DataFrame) -> None:
        for _, row in groupedDataframeUpdate.iterrows():
            oddsType = row["betHAD"]
            if oddsType == "HOME":
                self.lastHomeOdds = row["price"]
            elif oddsType == "AWAY":
                self.lastAwayOdds = row["price"]
            elif oddsType == "DRAW":
                self.lastDrawOdds = row["price"]

    def _get_odds_vector(self) -> np.array:
        return np.array((self.lastHomeOdds, self.lastAwayOdds, self.lastDrawOdds))

    def _reset(self) -> None:
        if not self.doCycle:
            self.lastHomeOdds = 0
            self.lastAwayOdds = 0
            self.lastDrawOdds = 0
