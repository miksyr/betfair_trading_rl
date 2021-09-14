import numpy as np
import numpy.testing as npt
import pandas as pd

from unittest import TestCase

from trading.datamodel.odds_series.over_under_odds_series import OverUnderOddsSeries
from trading.datamodel.outcomes.over_under_outcome import OverUnderOutcome
from trading.tests.test_datamodel.constants import OverUnderOddsTestData


class TestOverUnderOddsSeries(TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)

    def setUp(self):
        super().setUp()
        self.overUnderOddsSeries = OverUnderOddsSeries(
            oddsDataframe=OverUnderOddsTestData.testDataframe,
            doCycle=False,
            overUnderOutcome=OverUnderOutcome.UNDER,
            backScalingFactor=OverUnderOddsTestData.backScalingFactor,
        )
        self.overUnderOddsSeries.initialize()

    def test_initialize(self):
        self.assertEqual(len(self.overUnderOddsSeries.groupedOddsUpdates), 5)
        self.assertSequenceEqual(self.overUnderOddsSeries.orderedTimestamps, OverUnderOddsTestData.sortedValidTimestamps)
        self.assertEqual(self.overUnderOddsSeries.totalNumSteps, len(OverUnderOddsTestData.sortedValidTimestamps))

    def test_isValid(self):
        self.assertTrue(self.overUnderOddsSeries.isValid)
        badDataFrame = pd.DataFrame({"runner_name": [], "unix_timestamp": [], "price": []})
        badOddsSeries = OverUnderOddsSeries(
            oddsDataframe=badDataFrame,
            doCycle=False,
            overUnderOutcome=OverUnderOutcome.OVER,
            backScalingFactor=OverUnderOddsTestData.backScalingFactor,
        )
        self.assertFalse(badOddsSeries.isValid)

    def test_get_end_outcome_vector(self):
        outcomeVector = self.overUnderOddsSeries.get_end_outcome_vector()
        self.assertTrue((np.array((0, 1)) == outcomeVector).all())

    def test_update_first_step(self):
        groupedDataframeUpdate = self.overUnderOddsSeries.groupedOddsUpdates[OverUnderOddsTestData.sortedValidTimestamps[0]]
        self.overUnderOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.overUnderOddsSeries.lastOverOdds, OverUnderOddsTestData.prices[3])
        self.assertEqual(self.overUnderOddsSeries.lastUnderOdds, 0)

    def test_update_second_step(self):
        groupedDataframeUpdate = self.overUnderOddsSeries.groupedOddsUpdates[OverUnderOddsTestData.sortedValidTimestamps[1]]
        self.overUnderOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.overUnderOddsSeries.lastOverOdds, 0)
        self.assertEqual(self.overUnderOddsSeries.lastUnderOdds, OverUnderOddsTestData.prices[2])

    def test_update_third_step(self):
        # steps are treated independent in these tests, so not full expected step as nothing is carried over from step 2 like in proper processing
        groupedDataframeUpdate = self.overUnderOddsSeries.groupedOddsUpdates[OverUnderOddsTestData.sortedValidTimestamps[2]]
        self.overUnderOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.overUnderOddsSeries.lastOverOdds, OverUnderOddsTestData.prices[0])
        self.assertEqual(self.overUnderOddsSeries.lastUnderOdds, OverUnderOddsTestData.prices[1])

    def test_get_step_without_jitter_odds(self):
        firstStep = self.overUnderOddsSeries.get_step()
        npt.assert_almost_equal(actual=firstStep, desired=OverUnderOddsTestData.expectedFirstStep, decimal=4)
        secondStep = self.overUnderOddsSeries.get_step()
        npt.assert_almost_equal(actual=secondStep, desired=OverUnderOddsTestData.expectedSecondStep, decimal=4)
        thirdStep = self.overUnderOddsSeries.get_step()
        npt.assert_almost_equal(actual=thirdStep, desired=OverUnderOddsTestData.expectedThirdStep, decimal=4)

    @staticmethod
    def _convert_odds_to_probabilities(odds: np.array) -> np.array:
        return np.array([1 / v if v > 0 else 0 for v in odds])

    def test_get_step_with_jitter_odds(self):
        self.overUnderOddsSeries.set_jitter_odds_scale(scale=0.001)
        firstStep = self.overUnderOddsSeries.get_step()
        firstStepProbabilities = self._convert_odds_to_probabilities(odds=firstStep)
        expectedFirstProbabilities = self._convert_odds_to_probabilities(odds=OverUnderOddsTestData.expectedFirstStep)
        npt.assert_almost_equal(actual=firstStepProbabilities, desired=expectedFirstProbabilities, decimal=2)
        firstStepOddsDifference = sum(abs(firstStep - OverUnderOddsTestData.expectedFirstStep))
        self.assertTrue(firstStepOddsDifference > 0)

        secondStep = self.overUnderOddsSeries.get_step()
        secondStepProbabilities = self._convert_odds_to_probabilities(odds=secondStep)
        expectedSecondStepProbabilities = self._convert_odds_to_probabilities(odds=OverUnderOddsTestData.expectedSecondStep)
        npt.assert_almost_equal(actual=secondStepProbabilities, desired=expectedSecondStepProbabilities, decimal=2)
        secondStepOddsDifference = sum(abs(secondStep - OverUnderOddsTestData.expectedSecondStep))
        self.assertTrue(secondStepOddsDifference > 0)

        thirdStep = self.overUnderOddsSeries.get_step()
        thirdStepProbabilities = self._convert_odds_to_probabilities(odds=thirdStep)
        expectedThirdStepProbabilities = self._convert_odds_to_probabilities(odds=OverUnderOddsTestData.expectedThirdStep)
        npt.assert_almost_equal(actual=thirdStepProbabilities, desired=expectedThirdStepProbabilities, decimal=2)
        thirdStepOddsDifference = sum(abs(thirdStep - OverUnderOddsTestData.expectedThirdStep))
        self.assertTrue(thirdStepOddsDifference > 0)

    def test_reset(self):
        self.overUnderOddsSeries.lastUpdateTime = OverUnderOddsTestData.timestamps[1] - 1
        self.overUnderOddsSeries.currentStepNumber = 100
        groupedDataframeUpdate = self.overUnderOddsSeries.groupedOddsUpdates[OverUnderOddsTestData.timestamps[1]]
        self.overUnderOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.overUnderOddsSeries.episodeEnded = True
        self.overUnderOddsSeries.reset()
        self.assertEqual(self.overUnderOddsSeries.currentStepNumber, 0)
        self.assertFalse(self.overUnderOddsSeries.episodeEnded)
        self.assertEqual(self.overUnderOddsSeries.lastOverOdds, 0)
        self.assertEqual(self.overUnderOddsSeries.lastUnderOdds, 0)

    def test_full_series_no_cycle(self):
        for _ in range(len(OverUnderOddsTestData.sortedValidTimestamps)):
            self.overUnderOddsSeries.get_step()
        self.assertTrue(self.overUnderOddsSeries.episodeEnded)
        self.assertRaises(IndexError, self.overUnderOddsSeries.get_step)

    def test_full_series_with_cycle(self):
        overUnderOddsSeries = OverUnderOddsSeries(
            oddsDataframe=OverUnderOddsTestData.testDataframe,
            doCycle=True,
            overUnderOutcome=OverUnderOutcome.UNDER,
            backScalingFactor=OverUnderOddsTestData.backScalingFactor,
        )
        overUnderOddsSeries.initialize()
        for i in range(len(OverUnderOddsTestData.sortedValidTimestamps)):
            self.assertEqual(overUnderOddsSeries.currentStepNumber, i)
            overUnderOddsSeries.get_step()
            self.assertFalse(overUnderOddsSeries.episodeEnded)
        self.assertEqual(overUnderOddsSeries.currentStepNumber, 0)
        cycledStep = overUnderOddsSeries.get_step()
        npt.assert_almost_equal(actual=cycledStep, desired=OverUnderOddsTestData.expectedLoopedStep, decimal=4)
