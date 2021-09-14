from trading.datamodel.outcomes.base_outcome import BaseOutcome


class OverUnderOutcome(BaseOutcome):

    OVER = (1, 0)
    UNDER = (0, 1)
