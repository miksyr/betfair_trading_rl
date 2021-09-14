from trading.datamodel.outcomes.base_outcome import BaseOutcome


class MatchOutcome(BaseOutcome):

    HOME_WIN = (1, 0, 0)
    DRAW = (0, 1, 0)
    AWAY_WIN = (0, 0, 1)
