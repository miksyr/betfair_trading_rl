import numpy as np

from unittest import TestCase

from trading.datamodel.environment.match_odds_environment import MatchOddsEnvironment
from trading.datamodel.odds_series.match_odds_series import MatchOddsSeries
from trading.datamodel.outcomes.match_outcome import MatchOutcome
from trading.tests.test_datamodel.constants import MatchOddsTestData


class TestMatchOddsEnvironment(TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.oddsDimensionality = 6
        self.inactionPenalty = -1
        self.firstStepOddsSum = 0.196
        self.secondStepOddsSum = 0.996
        self.thirdStepOddsSum = 1.010

    def setUp(self):
        super().setUp()
        self.exampleMatchOddsSeries = MatchOddsSeries(
            oddsDataframe=MatchOddsTestData.testDataframe,
            doCycle=False,
            matchOutcome=MatchOutcome.HOME_WIN,
            backScalingFactor=MatchOddsTestData.backScalingFactor,
        )
        self.matchOddsEnvironment = MatchOddsEnvironment(
            oddSeries=self.exampleMatchOddsSeries,
            rewardDiscountFactor=0.1,
            inactionPenalty=self.inactionPenalty,
            onlyPositiveCashout=False,
        )

    def test_initialise(self):
        self.assertEqual(
            len(self.matchOddsEnvironment.oddSeries.groupedOddsUpdates), len(MatchOddsTestData.sortedValidTimestamps)
        )
        self.assertSequenceEqual(
            self.matchOddsEnvironment.oddSeries.orderedTimestamps, MatchOddsTestData.sortedValidTimestamps
        )
        self.assertEqual(self.matchOddsEnvironment.oddSeries.totalNumSteps, len(MatchOddsTestData.sortedValidTimestamps))

    def test_get_blank_state_observation(self):
        blankState = self.matchOddsEnvironment.get_state_observation()
        self.assertEqual(sum(blankState), 0)

    def test_action_spec(self):
        actionSpec = self.matchOddsEnvironment.action_spec()
        self.assertEqual(actionSpec.maximum, self.oddsDimensionality)

    def test_observation_spec(self):
        observationSpec = self.matchOddsEnvironment.observation_spec()
        self.assertEqual(observationSpec.shape, (2 * self.oddsDimensionality,))

    def test_bad_inaction_penalty(self):
        self.assertRaises(ValueError, MatchOddsEnvironment, self.exampleMatchOddsSeries, 0.1, 10, False)

    def test_step_inaction_penalty(self):
        firstStep = self.matchOddsEnvironment.step(action=0)
        self.assertEqual(firstStep.discount, 1)
        self.assertEqual(firstStep.reward, 0)
        self.assertEqual(firstStep.step_type, 0)
        self.assertEqual(round(sum(firstStep.observation[-self.oddsDimensionality :]), 3), self.firstStepOddsSum)
        self.assertEqual(sum(firstStep.observation[: self.oddsDimensionality]), 0)
        secondStep = self.matchOddsEnvironment.step(action=0)
        self.assertEqual(secondStep.discount, 0)
        self.assertEqual(secondStep.reward, self.inactionPenalty)
        self.assertEqual(secondStep.step_type, 1)
        self.assertEqual(round(sum(secondStep.observation[-self.oddsDimensionality :]), 3), self.secondStepOddsSum)
        self.assertEqual(sum(secondStep.observation[: self.oddsDimensionality]), 0)

    def test_step_invalid(self):
        firstStep = self.matchOddsEnvironment.step(action=0)
        self.assertEqual(firstStep.discount, 1)
        self.assertEqual(firstStep.reward, 0)
        self.assertEqual(firstStep.step_type, 0)
        self.assertEqual(round(sum(firstStep.observation[-self.oddsDimensionality :]), 3), self.firstStepOddsSum)
        self.assertEqual(sum(firstStep.observation[: self.oddsDimensionality]), 0)
        secondStep = self.matchOddsEnvironment.step(action=1)  # invalid because no odds are here (zeroes)
        self.assertEqual(secondStep.discount, 0)
        self.assertEqual(secondStep.reward, self.matchOddsEnvironment._state.duplicateActionPenalty)
        self.assertEqual(secondStep.step_type, 1)
        self.assertEqual(round(sum(secondStep.observation[-self.oddsDimensionality :]), 3), self.secondStepOddsSum)
        self.assertEqual(sum(secondStep.observation[: self.oddsDimensionality]), 0)

    def test_step_valid_1(self):
        firstStep = self.matchOddsEnvironment.step(action=0)
        self.assertEqual(firstStep.discount, 1)
        self.assertEqual(firstStep.reward, 0)
        self.assertEqual(firstStep.step_type, 0)
        self.assertEqual(round(sum(firstStep.observation[-self.oddsDimensionality :]), 3), self.firstStepOddsSum)
        self.assertEqual(sum(firstStep.observation[: self.oddsDimensionality]), 0)

        secondStep = self.matchOddsEnvironment.step(action=2)
        self.assertEqual(secondStep.discount, np.array(0.1, dtype="float32"))
        self.assertEqual(secondStep.reward, -1)
        self.assertEqual(secondStep.step_type, 1)
        self.assertEqual(round(sum(secondStep.observation[-self.oddsDimensionality :]), 3), self.secondStepOddsSum)
        self.assertEqual(round(sum(secondStep.observation[: self.oddsDimensionality]), 3), 0.098)

        thirdStep = self.matchOddsEnvironment.step(action=5)
        self.assertEqual(thirdStep.discount, 0)
        self.assertEqual(round(float(thirdStep.reward), 3), 1.049)
        self.assertEqual(thirdStep.step_type, 1)
        self.assertEqual(round(sum(thirdStep.observation[-self.oddsDimensionality :]), 3), self.thirdStepOddsSum)
        self.assertEqual(sum(thirdStep.observation[: self.oddsDimensionality]), 0)

    def test_step_valid_2(self):
        firstStep = self.matchOddsEnvironment.step(action=0)
        self.assertEqual(firstStep.discount, 1)
        self.assertEqual(firstStep.reward, 0)
        self.assertEqual(firstStep.step_type, 0)
        self.assertEqual(round(sum(firstStep.observation[-self.oddsDimensionality :]), 3), self.firstStepOddsSum)
        self.assertEqual(sum(firstStep.observation[: self.oddsDimensionality]), 0)

        secondStep = self.matchOddsEnvironment.step(action=2)
        self.assertEqual(secondStep.discount, np.array(0.1, dtype="float32"))
        self.assertEqual(secondStep.reward, -1)
        self.assertEqual(secondStep.step_type, 1)
        self.assertEqual(round(sum(secondStep.observation[-self.oddsDimensionality :]), 3), self.secondStepOddsSum)
        self.assertEqual(round(sum(secondStep.observation[: self.oddsDimensionality]), 3), 0.098)

        thirdStep = self.matchOddsEnvironment.step(action=3)
        self.assertEqual(thirdStep.discount, np.array(0.1, dtype="float32"))
        self.assertEqual(round(float(thirdStep.reward), 3), -1)
        self.assertEqual(thirdStep.step_type, 1)
        self.assertEqual(round(sum(thirdStep.observation[-self.oddsDimensionality :]), 3), self.thirdStepOddsSum)
        self.assertEqual(round(sum(thirdStep.observation[: self.oddsDimensionality]), 3), 0.288)

    def test_no_cycle(self):
        actions = [0, 0, 1, 0, 0, 0]
        rewards = [
            0,
            self.inactionPenalty,
            -1,
            MatchOddsTestData.prices[1] * MatchOddsTestData.backScalingFactor,
            self.inactionPenalty,
            self.inactionPenalty,
        ]
        stepTypes = [0, 1, 1, 2, 1, 1]
        for action, reward, stepType in zip(actions, rewards, stepTypes):
            timestep = self.matchOddsEnvironment.step(action=action)
            self.assertEqual(timestep.reward, np.array(reward, dtype="float32"))
            self.assertEqual(timestep.step_type, stepType)

    def test_cycle(self):
        matchOddsSeries = MatchOddsSeries(
            oddsDataframe=MatchOddsTestData.testDataframe,
            doCycle=True,
            matchOutcome=MatchOutcome.HOME_WIN,
            backScalingFactor=MatchOddsTestData.backScalingFactor,
        )
        matchOddsEnvironment = MatchOddsEnvironment(
            oddSeries=matchOddsSeries,
            rewardDiscountFactor=0.1,
            inactionPenalty=self.inactionPenalty,
            onlyPositiveCashout=False,
        )
        actions = [0, 0, 1, 0, 0, 0]
        rewards = [0, self.inactionPenalty, -1, self.inactionPenalty, self.inactionPenalty, self.inactionPenalty]
        stepTypes = [0, 1, 1, 1, 1, 1]
        for action, reward, stepType in zip(actions, rewards, stepTypes):
            timestep = matchOddsEnvironment.step(action=action)
            self.assertEqual(timestep.reward, np.array(reward, dtype="float32"))
            self.assertEqual(timestep.step_type, stepType)
