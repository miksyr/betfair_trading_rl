import numpy as np
import numpy.testing as npt
import pandas as pd

from unittest import TestCase

from trading.datamodel.odds_series.match_odds_series import MatchOddsSeries
from trading.datamodel.outcomes.match_outcome import MatchOutcome


class TestMatchOddsBettingState(TestCase):
    def __init__(self, methodName="runTest"):
        super(TestMatchOddsBettingState, self).__init__(methodName=methodName)
        self.timestamps = [1514806169, 1514806169, 1514806169, 1514790730, 1514806395, 1514806400]
        self.prices = [1.86, 4.30, 3.85, 1.97, 2.00, 4.00]
        self.sortedValidTimestamps = sorted(set(self.timestamps))
        self.backScalingFactor = 0.99
        self.testDataframe = pd.DataFrame(
            {
                "betHAD": ["AWAY", "HOME", "DRAW", "AWAY", "AWAY", "HOME"],
                "unix_timestamp": self.timestamps,
                "price": self.prices,
            }
        )
        self.expectedFirstStep = np.concatenate(
            (
                np.array((0, self.prices[3], 0)) * self.backScalingFactor,
                np.array((0, self.prices[3], 0)),
            )
        )
        self.expectedSecondStep = np.concatenate(
            (
                np.array((self.prices[1], self.prices[0], self.prices[2])) * self.backScalingFactor,
                np.array((self.prices[1], self.prices[0], self.prices[2])),
            )
        )
        self.expectedThirdStep = np.concatenate(
            (
                np.array((self.prices[1], self.prices[4], self.prices[2])) * self.backScalingFactor,
                np.array((self.prices[1], self.prices[4], self.prices[2])),
            )
        )
        self.expectedLoopedStep = np.concatenate(
            (
                np.array((self.prices[5], self.prices[3], self.prices[2])) * self.backScalingFactor,
                np.array((self.prices[5], self.prices[3], self.prices[2])),
            )
        )

    def setUp(self):
        super().setUp()
        self.matchOddsSeries = MatchOddsSeries(
            oddsDataframe=self.testDataframe,
            doCycle=False,
            matchOutcome=MatchOutcome.HOME_WIN,
            backScalingFactor=self.backScalingFactor,
        )
        self.matchOddsSeries.initialize()

    def test_initialize(self):
        self.assertEqual(len(self.matchOddsSeries.groupedOddsUpdates), 4)
        self.assertSequenceEqual(self.matchOddsSeries.orderedTimestamps, self.sortedValidTimestamps)
        self.assertEqual(self.matchOddsSeries.totalNumSteps, len(self.sortedValidTimestamps))

    def test_isValid(self):
        self.assertTrue(self.matchOddsSeries.isValid)
        badDataFrame = pd.DataFrame({"betHAD": [], "unix_timestamp": [], "price": []})
        badOddsSeries = MatchOddsSeries(
            oddsDataframe=badDataFrame,
            doCycle=False,
            matchOutcome=MatchOutcome.HOME_WIN,
            backScalingFactor=self.backScalingFactor,
        )
        self.assertFalse(badOddsSeries.isValid)

    def test_get_end_outcome_vector(self):
        outcomeVector = self.matchOddsSeries.get_end_outcome_vector()
        self.assertTrue((np.array((1, 0, 0)) == outcomeVector).all())

    def test_update_first_step(self):
        groupedDataframeUpdate = self.matchOddsSeries.groupedOddsUpdates[self.sortedValidTimestamps[0]]
        self.matchOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.matchOddsSeries.lastHomeOdds, self.expectedFirstStep[3])
        self.assertEqual(self.matchOddsSeries.lastAwayOdds, self.expectedFirstStep[4])
        self.assertEqual(self.matchOddsSeries.lastDrawOdds, self.expectedFirstStep[5])

    def test_update_second_step(self):
        groupedDataframeUpdate = self.matchOddsSeries.groupedOddsUpdates[self.sortedValidTimestamps[1]]
        self.matchOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.matchOddsSeries.lastHomeOdds, self.expectedSecondStep[3])
        self.assertEqual(self.matchOddsSeries.lastAwayOdds, self.expectedSecondStep[4])
        self.assertEqual(self.matchOddsSeries.lastDrawOdds, self.expectedSecondStep[5])

    def test_update_third_step(self):
        # steps are treated independent in these tests, so not full expected step as nothing is carried over from step 2 like in proper processing
        groupedDataframeUpdate = self.matchOddsSeries.groupedOddsUpdates[self.sortedValidTimestamps[2]]
        self.matchOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.matchOddsSeries.lastHomeOdds, 0)
        self.assertEqual(self.matchOddsSeries.lastAwayOdds, self.expectedThirdStep[4])
        self.assertEqual(self.matchOddsSeries.lastDrawOdds, 0)

    def test_get_step_without_jitter_odds(self):
        firstStep = self.matchOddsSeries.get_step()
        npt.assert_almost_equal(actual=firstStep, desired=self.expectedFirstStep, decimal=4)
        secondStep = self.matchOddsSeries.get_step()
        npt.assert_almost_equal(actual=secondStep, desired=self.expectedSecondStep, decimal=4)
        thirdStep = self.matchOddsSeries.get_step()
        npt.assert_almost_equal(actual=thirdStep, desired=self.expectedThirdStep, decimal=4)

    @staticmethod
    def _convert_odds_to_probabilities(odds: np.array) -> np.array:
        return np.array([1 / v if v > 0 else 0 for v in odds])

    def test_get_step_with_jitter_odds(self):
        self.matchOddsSeries.set_jitter_odds_scale(scale=0.001)
        firstStep = self.matchOddsSeries.get_step()
        firstStepProbabilities = self._convert_odds_to_probabilities(odds=firstStep)
        expectedFirstProbabilities = self._convert_odds_to_probabilities(odds=self.expectedFirstStep)
        npt.assert_almost_equal(actual=firstStepProbabilities, desired=expectedFirstProbabilities, decimal=2)
        firstStepOddsDifference = sum(abs(firstStep - self.expectedFirstStep))
        self.assertTrue(firstStepOddsDifference > 0)

        secondStep = self.matchOddsSeries.get_step()
        secondStepProbabilities = self._convert_odds_to_probabilities(odds=secondStep)
        expectedSecondStepProbabilities = self._convert_odds_to_probabilities(odds=self.expectedSecondStep)
        npt.assert_almost_equal(actual=secondStepProbabilities, desired=expectedSecondStepProbabilities, decimal=2)
        secondStepOddsDifference = sum(abs(secondStep - self.expectedSecondStep))
        self.assertTrue(secondStepOddsDifference > 0)

        thirdStep = self.matchOddsSeries.get_step()
        thirdStepProbabilities = self._convert_odds_to_probabilities(odds=thirdStep)
        expectedThirdStepProbabilities = self._convert_odds_to_probabilities(odds=self.expectedThirdStep)
        npt.assert_almost_equal(actual=thirdStepProbabilities, desired=expectedThirdStepProbabilities, decimal=2)
        thirdStepOddsDifference = sum(abs(thirdStep - self.expectedThirdStep))
        self.assertTrue(thirdStepOddsDifference > 0)

    def test_reset(self):
        self.matchOddsSeries.lastUpdateTime = self.timestamps[1] - 1
        self.matchOddsSeries.currentStepNumber = 100
        groupedDataframeUpdate = self.matchOddsSeries.groupedOddsUpdates[self.timestamps[1]]
        self.matchOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.matchOddsSeries.episodeEnded = True
        self.matchOddsSeries.reset()
        self.assertEqual(self.matchOddsSeries.currentStepNumber, 0)
        self.assertFalse(self.matchOddsSeries.episodeEnded)
        self.assertEqual(self.matchOddsSeries.lastHomeOdds, 0)
        self.assertEqual(self.matchOddsSeries.lastAwayOdds, 0)
        self.assertEqual(self.matchOddsSeries.lastDrawOdds, 0)

    def test_full_series_no_cycle(self):
        for _ in range(len(self.sortedValidTimestamps)):
            self.matchOddsSeries.get_step()
        self.assertTrue(self.matchOddsSeries.episodeEnded)
        self.assertRaises(IndexError, self.matchOddsSeries.get_step)

    def test_full_series_with_cycle(self):
        matchOddsSeries = MatchOddsSeries(
            oddsDataframe=self.testDataframe,
            doCycle=True,
            matchOutcome=MatchOutcome.HOME_WIN,
            backScalingFactor=self.backScalingFactor,
        )
        matchOddsSeries.initialize()
        for i in range(len(self.sortedValidTimestamps)):
            self.assertEqual(matchOddsSeries.currentStepNumber, i)
            matchOddsSeries.get_step()
            self.assertFalse(matchOddsSeries.episodeEnded)
        self.assertEqual(matchOddsSeries.currentStepNumber, 0)
        cycledStep = matchOddsSeries.get_step()
        npt.assert_almost_equal(actual=cycledStep, desired=self.expectedLoopedStep, decimal=4)
