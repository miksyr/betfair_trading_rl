import numpy as np

from unittest import TestCase

from trading.datamodel.environment.over_under_environment import OverUnderEnvironment
from trading.datamodel.odds_series.over_under_odds_series import OverUnderOddsSeries
from trading.datamodel.outcomes.over_under_outcome import OverUnderOutcome
from trading.tests.test_datamodel.constants import OverUnderOddsTestData


class TestOverUnderOddsEnvironment(TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.oddsDimensionality = 4
        self.inactionPenalty = -1
        self.firstStepOddsSum = 0.784
        self.secondStepOddsSum = 2.316
        self.thirdStepOddsSum = 2.452

    def setUp(self):
        super().setUp()
        self.exampleOverUnderOddsSeries = OverUnderOddsSeries(
            oddsDataframe=OverUnderOddsTestData.testDataframe,
            doCycle=False,
            overUnderOutcome=OverUnderOutcome.UNDER,
            backScalingFactor=OverUnderOddsTestData.backScalingFactor,
        )
        self.overUnderEnvironment = OverUnderEnvironment(
            oddSeries=self.exampleOverUnderOddsSeries,
            rewardDiscountFactor=0.1,
            inactionPenalty=self.inactionPenalty,
            onlyPositiveCashout=False,
        )

    def test_initialise(self):
        self.assertEqual(
            len(self.overUnderEnvironment.oddSeries.groupedOddsUpdates), len(OverUnderOddsTestData.sortedValidTimestamps)
        )
        self.assertSequenceEqual(
            self.overUnderEnvironment.oddSeries.orderedTimestamps, OverUnderOddsTestData.sortedValidTimestamps
        )
        self.assertEqual(self.overUnderEnvironment.oddSeries.totalNumSteps, len(OverUnderOddsTestData.sortedValidTimestamps))

    def test_get_blank_state_observation(self):
        blankState = self.overUnderEnvironment.get_state_observation()
        self.assertEqual(sum(blankState), 0)

    def test_action_spec(self):
        actionSpec = self.overUnderEnvironment.action_spec()
        self.assertEqual(actionSpec.maximum, self.oddsDimensionality)

    def test_observation_spec(self):
        observationSpec = self.overUnderEnvironment.observation_spec()
        self.assertEqual(observationSpec.shape, (2 * self.oddsDimensionality,))

    def test_bad_inaction_penalty(self):
        self.assertRaises(ValueError, OverUnderEnvironment, self.exampleOverUnderOddsSeries, 0.1, 10, False)

    def test_step_inaction_penalty(self):
        firstStep = self.overUnderEnvironment.step(action=0)
        self.assertEqual(firstStep.discount, 1)
        self.assertEqual(firstStep.reward, 0)
        self.assertEqual(firstStep.step_type, 0)
        self.assertEqual(round(sum(firstStep.observation[-self.oddsDimensionality :]), 3), self.firstStepOddsSum)
        self.assertEqual(sum(firstStep.observation[: self.oddsDimensionality]), 0)
        secondStep = self.overUnderEnvironment.step(action=0)
        self.assertEqual(secondStep.discount, 0)
        self.assertEqual(secondStep.reward, self.inactionPenalty)
        self.assertEqual(secondStep.step_type, 1)
        self.assertEqual(round(sum(secondStep.observation[-self.oddsDimensionality :]), 3), self.secondStepOddsSum)
        self.assertEqual(sum(secondStep.observation[: self.oddsDimensionality]), 0)

    def test_step_valid_1(self):
        firstStep = self.overUnderEnvironment.step(action=0)
        self.assertEqual(firstStep.discount, 1)
        self.assertEqual(firstStep.reward, 0)
        self.assertEqual(firstStep.step_type, 0)
        self.assertEqual(round(sum(firstStep.observation[-self.oddsDimensionality :]), 3), self.firstStepOddsSum)
        self.assertEqual(sum(firstStep.observation[: self.oddsDimensionality]), 0)

        secondStep = self.overUnderEnvironment.step(action=1)
        self.assertEqual(secondStep.discount, np.array(0.1, dtype="float32"))
        self.assertEqual(secondStep.reward, -1)
        self.assertEqual(secondStep.step_type, 1)
        self.assertEqual(round(sum(secondStep.observation[-self.oddsDimensionality :]), 3), self.secondStepOddsSum)
        self.assertEqual(round(sum(secondStep.observation[: self.oddsDimensionality]), 3), 0.390)

        thirdStep = self.overUnderEnvironment.step(action=3)
        self.assertEqual(thirdStep.discount, 0)
        self.assertEqual(round(float(thirdStep.reward), 3), 0.990)
        self.assertEqual(thirdStep.step_type, 1)
        self.assertEqual(round(sum(thirdStep.observation[-self.oddsDimensionality :]), 3), self.thirdStepOddsSum)
        self.assertEqual(sum(thirdStep.observation[: self.oddsDimensionality]), 0)

    def test_step_valid_2(self):
        firstStep = self.overUnderEnvironment.step(action=0)
        self.assertEqual(firstStep.discount, 1)
        self.assertEqual(firstStep.reward, 0)
        self.assertEqual(firstStep.step_type, 0)
        self.assertEqual(round(sum(firstStep.observation[-self.oddsDimensionality :]), 3), self.firstStepOddsSum)
        self.assertEqual(sum(firstStep.observation[: self.oddsDimensionality]), 0)

        secondStep = self.overUnderEnvironment.step(action=1)
        self.assertEqual(secondStep.discount, np.array(0.1, dtype="float32"))
        self.assertEqual(secondStep.reward, -1)
        self.assertEqual(secondStep.step_type, 1)
        self.assertEqual(round(sum(secondStep.observation[-self.oddsDimensionality :]), 3), self.secondStepOddsSum)
        self.assertEqual(round(sum(secondStep.observation[: self.oddsDimensionality]), 3), 0.390)

        thirdStep = self.overUnderEnvironment.step(action=4)
        self.assertEqual(thirdStep.discount, np.array(0.1, dtype="float32"))
        self.assertEqual(round(float(thirdStep.reward), 3), -2.85)
        self.assertEqual(thirdStep.step_type, 1)
        self.assertEqual(round(sum(thirdStep.observation[-self.oddsDimensionality :]), 3), self.thirdStepOddsSum)
        self.assertEqual(round(sum(thirdStep.observation[: self.oddsDimensionality]), 3), 1.160)

    def test_no_cycle(self):
        actions = [0, 0, 2, 0, 0, 0]
        rewards = [
            0,
            self.inactionPenalty,
            -1,
            self.inactionPenalty,
            OverUnderOddsTestData.prices[2] * OverUnderOddsTestData.backScalingFactor,
            self.inactionPenalty,
        ]
        stepTypes = [0, 1, 1, 1, 2, 1]
        for action, reward, stepType in zip(actions, rewards, stepTypes):
            timestep = self.overUnderEnvironment.step(action=action)
            self.assertEqual(timestep.reward, np.array(reward, dtype="float32"))
            self.assertEqual(timestep.step_type, stepType)

    def test_cycle(self):
        overUnderOddsSeries = OverUnderOddsSeries(
            oddsDataframe=OverUnderOddsTestData.testDataframe,
            doCycle=True,
            overUnderOutcome=OverUnderOutcome.OVER,
            backScalingFactor=OverUnderOddsTestData.backScalingFactor,
        )
        overUnderEnvironment = OverUnderEnvironment(
            oddSeries=overUnderOddsSeries,
            rewardDiscountFactor=0.1,
            inactionPenalty=self.inactionPenalty,
            onlyPositiveCashout=False,
        )
        actions = [0, 0, 1, 0, 0, 0]
        rewards = [0, self.inactionPenalty, -1, self.inactionPenalty, self.inactionPenalty, self.inactionPenalty]
        stepTypes = [0, 1, 1, 1, 1, 1]
        for action, reward, stepType in zip(actions, rewards, stepTypes):
            timestep = overUnderEnvironment.step(action=action)
            self.assertEqual(timestep.reward, np.array(reward, dtype="float32"))
            self.assertEqual(timestep.step_type, stepType)
