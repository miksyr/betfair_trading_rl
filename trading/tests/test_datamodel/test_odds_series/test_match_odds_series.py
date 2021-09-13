import numpy as np
import numpy.testing as npt
import pandas as pd

from unittest import TestCase

from trading.datamodel.odds_series.match_odds_series import MatchOddsSeries
from trading.datamodel.outcomes.match_outcome import MatchOutcome
from trading.tests.test_datamodel.constants import MatchOddsTestData


class TestMatchOddsSeries(TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)

    def setUp(self):
        super().setUp()
        self.matchOddsSeries = MatchOddsSeries(
            oddsDataframe=MatchOddsTestData.testDataframe,
            doCycle=False,
            matchOutcome=MatchOutcome.HOME_WIN,
            backScalingFactor=MatchOddsTestData.backScalingFactor,
        )
        self.matchOddsSeries.initialize()

    def test_initialize(self):
        self.assertEqual(len(self.matchOddsSeries.groupedOddsUpdates), 4)
        self.assertSequenceEqual(self.matchOddsSeries.orderedTimestamps, MatchOddsTestData.sortedValidTimestamps)
        self.assertEqual(self.matchOddsSeries.totalNumSteps, len(MatchOddsTestData.sortedValidTimestamps))

    def test_isValid(self):
        self.assertTrue(self.matchOddsSeries.isValid)
        badDataFrame = pd.DataFrame({"betHAD": [], "unix_timestamp": [], "price": []})
        badOddsSeries = MatchOddsSeries(
            oddsDataframe=badDataFrame,
            doCycle=False,
            matchOutcome=MatchOutcome.HOME_WIN,
            backScalingFactor=MatchOddsTestData.backScalingFactor,
        )
        self.assertFalse(badOddsSeries.isValid)

    def test_get_end_outcome_vector(self):
        outcomeVector = self.matchOddsSeries.get_end_outcome_vector()
        self.assertTrue((np.array((1, 0, 0)) == outcomeVector).all())

    def test_update_first_step(self):
        groupedDataframeUpdate = self.matchOddsSeries.groupedOddsUpdates[MatchOddsTestData.sortedValidTimestamps[0]]
        self.matchOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.matchOddsSeries.lastHomeOdds, MatchOddsTestData.expectedFirstStep[3])
        self.assertEqual(self.matchOddsSeries.lastAwayOdds, MatchOddsTestData.expectedFirstStep[4])
        self.assertEqual(self.matchOddsSeries.lastDrawOdds, MatchOddsTestData.expectedFirstStep[5])

    def test_update_second_step(self):
        groupedDataframeUpdate = self.matchOddsSeries.groupedOddsUpdates[MatchOddsTestData.sortedValidTimestamps[1]]
        self.matchOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.matchOddsSeries.lastHomeOdds, MatchOddsTestData.expectedSecondStep[3])
        self.assertEqual(self.matchOddsSeries.lastAwayOdds, MatchOddsTestData.expectedSecondStep[4])
        self.assertEqual(self.matchOddsSeries.lastDrawOdds, MatchOddsTestData.expectedSecondStep[5])

    def test_update_third_step(self):
        # steps are treated independent in these tests, so not full expected step as nothing is carried over from step 2 like in proper processing
        groupedDataframeUpdate = self.matchOddsSeries.groupedOddsUpdates[MatchOddsTestData.sortedValidTimestamps[2]]
        self.matchOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.assertEqual(self.matchOddsSeries.lastHomeOdds, 0)
        self.assertEqual(self.matchOddsSeries.lastAwayOdds, MatchOddsTestData.expectedThirdStep[4])
        self.assertEqual(self.matchOddsSeries.lastDrawOdds, 0)

    def test_get_step_without_jitter_odds(self):
        firstStep = self.matchOddsSeries.get_step()
        npt.assert_almost_equal(actual=firstStep, desired=MatchOddsTestData.expectedFirstStep, decimal=4)
        secondStep = self.matchOddsSeries.get_step()
        npt.assert_almost_equal(actual=secondStep, desired=MatchOddsTestData.expectedSecondStep, decimal=4)
        thirdStep = self.matchOddsSeries.get_step()
        npt.assert_almost_equal(actual=thirdStep, desired=MatchOddsTestData.expectedThirdStep, decimal=4)

    @staticmethod
    def _convert_odds_to_probabilities(odds: np.array) -> np.array:
        return np.array([1 / v if v > 0 else 0 for v in odds])

    def test_get_step_with_jitter_odds(self):
        self.matchOddsSeries.set_jitter_odds_scale(scale=0.001)
        firstStep = self.matchOddsSeries.get_step()
        firstStepProbabilities = self._convert_odds_to_probabilities(odds=firstStep)
        expectedFirstProbabilities = self._convert_odds_to_probabilities(odds=MatchOddsTestData.expectedFirstStep)
        npt.assert_almost_equal(actual=firstStepProbabilities, desired=expectedFirstProbabilities, decimal=2)
        firstStepOddsDifference = sum(abs(firstStep - MatchOddsTestData.expectedFirstStep))
        self.assertTrue(firstStepOddsDifference > 0)

        secondStep = self.matchOddsSeries.get_step()
        secondStepProbabilities = self._convert_odds_to_probabilities(odds=secondStep)
        expectedSecondStepProbabilities = self._convert_odds_to_probabilities(odds=MatchOddsTestData.expectedSecondStep)
        npt.assert_almost_equal(actual=secondStepProbabilities, desired=expectedSecondStepProbabilities, decimal=2)
        secondStepOddsDifference = sum(abs(secondStep - MatchOddsTestData.expectedSecondStep))
        self.assertTrue(secondStepOddsDifference > 0)

        thirdStep = self.matchOddsSeries.get_step()
        thirdStepProbabilities = self._convert_odds_to_probabilities(odds=thirdStep)
        expectedThirdStepProbabilities = self._convert_odds_to_probabilities(odds=MatchOddsTestData.expectedThirdStep)
        npt.assert_almost_equal(actual=thirdStepProbabilities, desired=expectedThirdStepProbabilities, decimal=2)
        thirdStepOddsDifference = sum(abs(thirdStep - MatchOddsTestData.expectedThirdStep))
        self.assertTrue(thirdStepOddsDifference > 0)

    def test_reset(self):
        self.matchOddsSeries.lastUpdateTime = MatchOddsTestData.timestamps[1] - 1
        self.matchOddsSeries.currentStepNumber = 100
        groupedDataframeUpdate = self.matchOddsSeries.groupedOddsUpdates[MatchOddsTestData.timestamps[1]]
        self.matchOddsSeries.update_step(groupedDataframeUpdate=groupedDataframeUpdate)
        self.matchOddsSeries.episodeEnded = True
        self.matchOddsSeries.reset()
        self.assertEqual(self.matchOddsSeries.currentStepNumber, 0)
        self.assertFalse(self.matchOddsSeries.episodeEnded)
        self.assertEqual(self.matchOddsSeries.lastHomeOdds, 0)
        self.assertEqual(self.matchOddsSeries.lastAwayOdds, 0)
        self.assertEqual(self.matchOddsSeries.lastDrawOdds, 0)

    def test_full_series_no_cycle(self):
        for _ in range(len(MatchOddsTestData.sortedValidTimestamps)):
            self.matchOddsSeries.get_step()
        self.assertTrue(self.matchOddsSeries.episodeEnded)
        self.assertRaises(IndexError, self.matchOddsSeries.get_step)

    def test_full_series_with_cycle(self):
        matchOddsSeries = MatchOddsSeries(
            oddsDataframe=MatchOddsTestData.testDataframe,
            doCycle=True,
            matchOutcome=MatchOutcome.HOME_WIN,
            backScalingFactor=MatchOddsTestData.backScalingFactor,
        )
        matchOddsSeries.initialize()
        for i in range(len(MatchOddsTestData.sortedValidTimestamps)):
            self.assertEqual(matchOddsSeries.currentStepNumber, i)
            matchOddsSeries.get_step()
            self.assertFalse(matchOddsSeries.episodeEnded)
        self.assertEqual(matchOddsSeries.currentStepNumber, 0)
        cycledStep = matchOddsSeries.get_step()
        npt.assert_almost_equal(actual=cycledStep, desired=MatchOddsTestData.expectedLoopedStep, decimal=4)
