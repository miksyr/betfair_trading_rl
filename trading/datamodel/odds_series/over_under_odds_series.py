import numpy as np
import pandas as pd

from trading.datamodel.odds_series.base_odds_series import BaseOddsSeries
from trading.datamodel.outcomes.over_under_outcome import OverUnderOutcome


class OverUnderOddsSeries(BaseOddsSeries):
    def __init__(
        self,
        oddsDataframe: pd.DataFrame,
        overUnderOutcome: OverUnderOutcome,
        doCycle: bool = False,
        backScalingFactor: float = 0.99,
    ):
        super().__init__(
            oddsDataframe=oddsDataframe, doCycle=doCycle, endOutcome=overUnderOutcome, backScalingFactor=backScalingFactor
        )
        self.lastOverOdds = 0
        self.lastUnderOdds = 0

    def update_step(self, groupedDataframeUpdate: pd.DataFrame) -> None:
        for _, row in groupedDataframeUpdate.iterrows():
            runnerName = row["runner_name"].lower()
            if "over" in runnerName:
                self.lastOverOdds = row["price"]
            elif "under" in runnerName:
                self.lastUnderOdds = row["price"]
            else:
                raise ValueError(f'Runner name: "{runnerName}" not recognised')

    def _get_odds_vector(self) -> np.array:
        return np.array((self.lastOverOdds, self.lastUnderOdds))

    def _reset_last_odds(self) -> None:
        self.lastOverOdds = 0
        self.lastUnderOdds = 0
