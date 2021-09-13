import numpy as np
import numpy.testing as npt
import pandas as pd

from unittest import TestCase

from trading.datamodel.odds_series.over_under_odds_series import OverUnderOddsSeries
from trading.datamodel.outcomes.over_under_outcome import OverUnderOutcome


class TestOverUnderOddsSeries(TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.timestamps = [1514806169, 1514806169, 1514806168, 1514790730, 1514806395, 1514806400]
        self.prices = [1.86, 4.30, 3.85, 1.97, 2.00, 4.00]
        self.sortedValidTimestamps = sorted(set(self.timestamps))
        self.backScalingFactor = 0.99
        self.testDataframe = pd.DataFrame(
            {
                "runner_name": ["Over", "UNDER", "Under", "OVER", "under", "over"],
                "unix_timestamp": self.timestamps,
                "price": self.prices,
            }
        )
        self.expectedFirstStep = np.concatenate(
            (
                np.array((self.prices[3], 0)) * self.backScalingFactor,
                np.array((self.prices[3], 0)),
            )
        )
        self.expectedSecondStep = np.concatenate(
            (
                np.array((self.prices[3], self.prices[2])) * self.backScalingFactor,
                np.array((self.prices[3], self.prices[2])),
            )
        )
        self.expectedThirdStep = np.concatenate(
            (
                np.array((self.prices[0], self.prices[1])) * self.backScalingFactor,
                np.array((self.prices[0], self.prices[1])),
            )
        )
        self.expectedLoopedStep = np.concatenate(
            (
                np.array((self.prices[3], self.prices[4])) * self.backScalingFactor,
                np.array((self.prices[3], self.prices[4])),
            )
        )

    def setUp(self):
        super().setUp()
        self.overUnderOddsSeries = OverUnderOddsSeries(
            oddsDataframe=self.testDataframe,
            doCycle=False,
            overUnderOutcome=OverUnderOutcome.UNDER,
            backScalingFactor=self.backScalingFactor,
        )
        self.overUnderOddsSeries.initialize()

    def test_initialize(self):
        self.assertEqual(len(self.overUnderOddsSeries.groupedOddsUpdates), 5)
        self.assertSequenceEqual(self.overUnderOddsSeries.orderedTimestamps, self.sortedValidTimestamps)
        self.assertEqual(self.overUnderOddsSeries.totalNumSteps, len(self.sortedValidTimestamps))

    def test_isValid(self):
        self.assertTrue(self.overUnderOddsSeries.isValid)
        badDataFrame = pd.DataFrame({"runner_name": [], "unix_timestamp": [], "price": []})
        badOddsSeries = OverUnderOddsSeries(
            oddsDataframe=badDataFrame,
            doCycle=False,
            overUnderOutcome=OverUnderOutcome.OVER,
            backScalingFactor=self.backScalingFactor,
        )
        self.assertFalse(badOddsSeries.isValid)

    def test_get_end_outcome_vector(self):
        outcomeVector = self.overUnderOddsSeries.get_end_outcome_vector()
        self.assertTrue((np.array((0, 1)) == outcomeVector).all())

    def test_update_first_step(self):
        groupedDataframeUpdate = self.overUnderOddsSeries.groupedOddsUpdates[self.sortedValidTimestamps[0]]
        self.overUnderOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.overUnderOddsSeries.lastOverOdds, self.prices[3])
        self.assertEqual(self.overUnderOddsSeries.lastUnderOdds, 0)

    def test_update_second_step(self):
        groupedDataframeUpdate = self.overUnderOddsSeries.groupedOddsUpdates[self.sortedValidTimestamps[1]]
        self.overUnderOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.overUnderOddsSeries.lastOverOdds, 0)
        self.assertEqual(self.overUnderOddsSeries.lastUnderOdds, self.prices[2])

    def test_update_third_step(self):
        # steps are treated independent in these tests, so not full expected step as nothing is carried over from step 2 like in proper processing
        groupedDataframeUpdate = self.overUnderOddsSeries.groupedOddsUpdates[self.sortedValidTimestamps[2]]
        self.overUnderOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.overUnderOddsSeries.lastOverOdds, self.prices[0])
        self.assertEqual(self.overUnderOddsSeries.lastUnderOdds, self.prices[1])

    def test_get_step_without_jitter_odds(self):
        firstStep = self.overUnderOddsSeries.get_step()
        npt.assert_almost_equal(actual=firstStep, desired=self.expectedFirstStep, decimal=4)
        secondStep = self.overUnderOddsSeries.get_step()
        npt.assert_almost_equal(actual=secondStep, desired=self.expectedSecondStep, decimal=4)
        thirdStep = self.overUnderOddsSeries.get_step()
        npt.assert_almost_equal(actual=thirdStep, desired=self.expectedThirdStep, decimal=4)

    @staticmethod
    def _convert_odds_to_probabilities(odds: np.array) -> np.array:
        return np.array([1 / v if v > 0 else 0 for v in odds])

    def test_get_step_with_jitter_odds(self):
        self.overUnderOddsSeries.set_jitter_odds_scale(scale=0.001)
        firstStep = self.overUnderOddsSeries.get_step()
        firstStepProbabilities = self._convert_odds_to_probabilities(odds=firstStep)
        expectedFirstProbabilities = self._convert_odds_to_probabilities(odds=self.expectedFirstStep)
        npt.assert_almost_equal(actual=firstStepProbabilities, desired=expectedFirstProbabilities, decimal=2)
        firstStepOddsDifference = sum(abs(firstStep - self.expectedFirstStep))
        self.assertTrue(firstStepOddsDifference > 0)

        secondStep = self.overUnderOddsSeries.get_step()
        secondStepProbabilities = self._convert_odds_to_probabilities(odds=secondStep)
        expectedSecondStepProbabilities = self._convert_odds_to_probabilities(odds=self.expectedSecondStep)
        npt.assert_almost_equal(actual=secondStepProbabilities, desired=expectedSecondStepProbabilities, decimal=2)
        secondStepOddsDifference = sum(abs(secondStep - self.expectedSecondStep))
        self.assertTrue(secondStepOddsDifference > 0)

        thirdStep = self.overUnderOddsSeries.get_step()
        thirdStepProbabilities = self._convert_odds_to_probabilities(odds=thirdStep)
        expectedThirdStepProbabilities = self._convert_odds_to_probabilities(odds=self.expectedThirdStep)
        npt.assert_almost_equal(actual=thirdStepProbabilities, desired=expectedThirdStepProbabilities, decimal=2)
        thirdStepOddsDifference = sum(abs(thirdStep - self.expectedThirdStep))
        self.assertTrue(thirdStepOddsDifference > 0)

    def test_reset(self):
        self.overUnderOddsSeries.lastUpdateTime = self.timestamps[1] - 1
        self.overUnderOddsSeries.currentStepNumber = 100
        groupedDataframeUpdate = self.overUnderOddsSeries.groupedOddsUpdates[self.timestamps[1]]
        self.overUnderOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.overUnderOddsSeries.episodeEnded = True
        self.overUnderOddsSeries.reset()
        self.assertEqual(self.overUnderOddsSeries.currentStepNumber, 0)
        self.assertFalse(self.overUnderOddsSeries.episodeEnded)
        self.assertEqual(self.overUnderOddsSeries.lastOverOdds, 0)
        self.assertEqual(self.overUnderOddsSeries.lastUnderOdds, 0)

    def test_full_series_no_cycle(self):
        for _ in range(len(self.sortedValidTimestamps)):
            self.overUnderOddsSeries.get_step()
        self.assertTrue(self.overUnderOddsSeries.episodeEnded)
        self.assertRaises(IndexError, self.overUnderOddsSeries.get_step)

    def test_full_series_with_cycle(self):
        overUnderOddsSeries = OverUnderOddsSeries(
            oddsDataframe=self.testDataframe,
            doCycle=True,
            overUnderOutcome=OverUnderOutcome.UNDER,
            backScalingFactor=self.backScalingFactor,
        )
        overUnderOddsSeries.initialize()
        for i in range(len(self.sortedValidTimestamps)):
            self.assertEqual(overUnderOddsSeries.currentStepNumber, i)
            overUnderOddsSeries.get_step()
            self.assertFalse(overUnderOddsSeries.episodeEnded)
        self.assertEqual(overUnderOddsSeries.currentStepNumber, 0)
        cycledStep = overUnderOddsSeries.get_step()
        npt.assert_almost_equal(actual=cycledStep, desired=self.expectedLoopedStep, decimal=4)
