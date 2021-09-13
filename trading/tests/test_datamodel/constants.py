from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class MatchOddsTestData:
    timestamps = [1514806169, 1514806169, 1514806169, 1514790730, 1514806395, 1514806400]
    prices = [1.86, 4.30, 3.85, 1.97, 2.00, 4.00]
    sortedValidTimestamps = sorted(set(timestamps))
    backScalingFactor = 0.99
    testDataframe = pd.DataFrame(
        {
            "betHAD": ["AWAY", "HOME", "DRAW", "AWAY", "AWAY", "HOME"],
            "unix_timestamp": timestamps,
            "price": prices,
        }
    )
    expectedFirstStep = np.concatenate(
        (
            np.array((0, prices[3], 0)) * backScalingFactor,
            np.array((0, prices[3], 0)),
        )
    )
    expectedSecondStep = np.concatenate(
        (
            np.array((prices[1], prices[0], prices[2])) * backScalingFactor,
            np.array((prices[1], prices[0], prices[2])),
        )
    )
    expectedThirdStep = np.concatenate(
        (
            np.array((prices[1], prices[4], prices[2])) * backScalingFactor,
            np.array((prices[1], prices[4], prices[2])),
        )
    )
    expectedLoopedStep = np.concatenate(
        (
            np.array((prices[5], prices[3], prices[2])) * backScalingFactor,
            np.array((prices[5], prices[3], prices[2])),
        )
    )


@dataclass
class OverUnderOddsTestData:
    timestamps = [1514806169, 1514806169, 1514806168, 1514790730, 1514806395, 1514806400]
    prices = [1.86, 4.30, 3.85, 1.97, 2.00, 4.00]
    sortedValidTimestamps = sorted(set(timestamps))
    backScalingFactor = 0.99
    testDataframe = pd.DataFrame(
        {
            "runner_name": ["Over", "UNDER", "Under", "OVER", "under", "over"],
            "unix_timestamp": timestamps,
            "price": prices,
        }
    )
    expectedFirstStep = np.concatenate(
        (
            np.array((prices[3], 0)) * backScalingFactor,
            np.array((prices[3], 0)),
        )
    )
    expectedSecondStep = np.concatenate(
        (
            np.array((prices[3], prices[2])) * backScalingFactor,
            np.array((prices[3], prices[2])),
        )
    )
    expectedThirdStep = np.concatenate(
        (
            np.array((prices[0], prices[1])) * backScalingFactor,
            np.array((prices[0], prices[1])),
        )
    )
    expectedLoopedStep = np.concatenate(
        (
            np.array((prices[3], prices[4])) * backScalingFactor,
            np.array((prices[3], prices[4])),
        )
    )
