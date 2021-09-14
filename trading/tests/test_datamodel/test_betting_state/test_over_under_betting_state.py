import numpy as np
import pandas as pd

from unittest import TestCase

from trading.datamodel.betting_state.over_under_betting_state import (
    OverUnderBettingState,
)


class TestOverUnderBettingState(TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.defaultOdds = [2.0, 2.5]
        self.updatedOdds = [1.66, 10.0]

    def _get_backed_betting_state(self):
        state = OverUnderBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        state.place_over_back_bet(odds=self.defaultOdds[0])
        state.place_under_back_bet(odds=self.defaultOdds[1])
        return state

    def _get_layed_betting_state(self):
        state = OverUnderBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        state.place_over_lay_bet(odds=self.defaultOdds[0])
        state.place_under_lay_bet(odds=self.defaultOdds[1])
        return state

    def setUp(self):
        super().setUp()
        self.emptyBettingState = OverUnderBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        self.restrictedEmptyBettingState = OverUnderBettingState(discountFactor=0.9, onlyPositiveCashout=True)
        self.backedBettingState = self._get_backed_betting_state()
        self.layedBettingState = self._get_layed_betting_state()

    # BACK/LAY tests
    def test_empty_over_back_placing(self):
        self.emptyBettingState.place_over_back_bet(odds=self.defaultOdds[0])
        self.assertEqual(self.emptyBettingState.overBackOdds, self.defaultOdds[0])

    def test_empty_under_back_placing(self):
        self.emptyBettingState.place_under_back_bet(odds=self.defaultOdds[1])
        self.assertEqual(self.emptyBettingState.underBackOdds, self.defaultOdds[1])

    def test_empty_over_lay_placing(self):
        self.emptyBettingState.place_over_lay_bet(odds=self.defaultOdds[0])
        self.assertEqual(self.emptyBettingState.overLayOdds, self.defaultOdds[0])

    def test_empty_under_lay_placing(self):
        self.emptyBettingState.place_under_lay_bet(odds=self.defaultOdds[1])
        self.assertEqual(self.emptyBettingState.underLayOdds, self.defaultOdds[1])

    # DUPLICATE ACTION TESTS
    def test_duplicate_over_back_placing(self):
        rewardClass = self.backedBettingState.place_over_back_bet(odds=self.defaultOdds[0])
        self.assertEqual(rewardClass.reward, self.backedBettingState.duplicateActionPenalty)

    def test_duplicate_under_back_placing(self):
        rewardClass = self.backedBettingState.place_under_back_bet(odds=self.defaultOdds[1])
        self.assertEqual(rewardClass.reward, self.backedBettingState.duplicateActionPenalty)

    def test_duplicate_over_lay_placing(self):
        rewardClass = self.layedBettingState.place_over_lay_bet(odds=self.defaultOdds[0])
        self.assertEqual(rewardClass.reward, self.layedBettingState.duplicateActionPenalty)

    def test_duplicate_under_lay_placing(self):
        rewardClass = self.layedBettingState.place_under_lay_bet(odds=self.defaultOdds[1])
        self.assertEqual(rewardClass.reward, self.layedBettingState.duplicateActionPenalty)

    # ZERO ODDS BETS
    def test_zero_odds_over_placing(self):
        backRewardClass = self.backedBettingState.place_over_back_bet(odds=0)
        self.assertEqual(backRewardClass.reward, self.backedBettingState.duplicateActionPenalty)
        layRewardClass = self.layedBettingState.place_over_lay_bet(odds=0)
        self.assertEqual(layRewardClass.reward, self.layedBettingState.duplicateActionPenalty)
        emptyBackRewardClass = self.emptyBettingState.place_over_back_bet(odds=0)
        self.assertEqual(emptyBackRewardClass.reward, self.emptyBettingState.duplicateActionPenalty)
        emptyLayRewardClass = self.emptyBettingState.place_over_lay_bet(odds=0)
        self.assertEqual(emptyLayRewardClass.reward, self.emptyBettingState.duplicateActionPenalty)

    def test_zero_odds_under_placing(self):
        backRewardClass = self.backedBettingState.place_under_back_bet(odds=0)
        self.assertEqual(backRewardClass.reward, self.backedBettingState.duplicateActionPenalty)
        layRewardClass = self.layedBettingState.place_under_lay_bet(odds=0)
        self.assertEqual(layRewardClass.reward, self.layedBettingState.duplicateActionPenalty)
        emptyBackRewardClass = self.emptyBettingState.place_under_back_bet(odds=0)
        self.assertEqual(emptyBackRewardClass.reward, self.emptyBettingState.duplicateActionPenalty)
        emptyLayRewardClass = self.emptyBettingState.place_under_lay_bet(odds=0)
        self.assertEqual(emptyLayRewardClass.reward, self.emptyBettingState.duplicateActionPenalty)

    # ONLY POSITIVE CASHOUT TESTS
    def test_restricted_state_valid_over_back_then_lay(self):
        backReward = self.restrictedEmptyBettingState.place_over_back_bet(odds=self.defaultOdds[0])
        layReward = self.restrictedEmptyBettingState.place_over_lay_bet(odds=self.updatedOdds[0])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.20)
        self.assertEqual(round(layReward.reward, 2), 1.20)
        self.assertEqual(self.restrictedEmptyBettingState.overBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.overLayOdds, 0.0)

    def test_restricted_state_invalid_over_back_then_lay(self):
        self.restrictedEmptyBettingState.place_over_back_bet(odds=self.updatedOdds[0])
        layReward = self.restrictedEmptyBettingState.place_over_lay_bet(odds=self.defaultOdds[0])
        self.assertEqual(layReward.reward, self.restrictedEmptyBettingState.duplicateActionPenalty)
        self.assertEqual(self.restrictedEmptyBettingState.overBackOdds, self.updatedOdds[0])
        self.assertEqual(self.restrictedEmptyBettingState.overLayOdds, 0.0)

    def test_restricted_state_valid_under_back_then_lay(self):
        backReward = self.restrictedEmptyBettingState.place_under_back_bet(odds=self.updatedOdds[1])
        layReward = self.restrictedEmptyBettingState.place_under_lay_bet(odds=self.defaultOdds[1])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 3.00)
        self.assertEqual(layReward.reward, 4.0)
        self.assertEqual(self.restrictedEmptyBettingState.underBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.underLayOdds, 0.0)

    def test_restricted_state_invalid_under_back_then_lay(self):
        self.restrictedEmptyBettingState.place_under_back_bet(odds=self.defaultOdds[1])
        layReward = self.restrictedEmptyBettingState.place_under_lay_bet(odds=self.updatedOdds[1])
        self.assertEqual(layReward.reward, self.restrictedEmptyBettingState.duplicateActionPenalty)
        self.assertEqual(self.restrictedEmptyBettingState.underBackOdds, self.defaultOdds[1])
        self.assertEqual(self.restrictedEmptyBettingState.underLayOdds, 0.0)

    def test_restricted_state_valid_over_lay_then_back(self):
        layReward = self.restrictedEmptyBettingState.place_over_lay_bet(odds=self.updatedOdds[0])
        backReward = self.restrictedEmptyBettingState.place_over_back_bet(odds=self.defaultOdds[0])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.17)
        self.assertEqual(round(backReward.reward, 2), 0.83)
        self.assertEqual(self.restrictedEmptyBettingState.overBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.overLayOdds, 0.0)

    def test_restricted_invalid_state_over_lay_then_back(self):
        self.restrictedEmptyBettingState.place_over_lay_bet(odds=self.defaultOdds[0])
        backReward = self.restrictedEmptyBettingState.place_over_back_bet(odds=self.updatedOdds[0])
        self.assertEqual(
            round(backReward.reward, 2),
            self.restrictedEmptyBettingState.duplicateActionPenalty,
        )
        self.assertEqual(self.restrictedEmptyBettingState.overBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.overLayOdds, self.defaultOdds[0])

    def test_restricted_state_valid_under_lay_then_back(self):
        layReward = self.restrictedEmptyBettingState.place_under_lay_bet(odds=self.defaultOdds[1])
        backReward = self.restrictedEmptyBettingState.place_under_back_bet(odds=self.updatedOdds[1])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.75)
        self.assertEqual(round(backReward.reward, 2), 2.25)
        self.assertEqual(self.restrictedEmptyBettingState.underBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.underLayOdds, 0.0)

    def test_restricted_invalid_state_under_lay_then_back(self):
        self.restrictedEmptyBettingState.place_under_lay_bet(odds=self.updatedOdds[1])
        backReward = self.restrictedEmptyBettingState.place_under_back_bet(odds=self.defaultOdds[1])
        self.assertEqual(
            round(backReward.reward, 2),
            self.restrictedEmptyBettingState.duplicateActionPenalty,
        )
        self.assertEqual(self.restrictedEmptyBettingState.underBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.underLayOdds, self.updatedOdds[1])

    # CASHING OUT TESTS (written when back + lay stakes were £1)
    def test_laying_over_after_backing_over(self):
        backReward = self.emptyBettingState.place_over_back_bet(odds=self.defaultOdds[0])
        layReward = self.emptyBettingState.place_over_lay_bet(odds=self.updatedOdds[0])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.20)
        self.assertEqual(self.emptyBettingState.overBackOdds, 0.0)
        self.assertEqual(self.emptyBettingState.overLayOdds, 0.0)

    def test_laying_under_after_backing_under(self):
        backReward = self.emptyBettingState.place_under_back_bet(odds=self.defaultOdds[1])
        layReward = self.emptyBettingState.place_under_lay_bet(odds=self.updatedOdds[1])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), -0.75)
        self.assertEqual(self.emptyBettingState.underBackOdds, 0.0)
        self.assertEqual(self.emptyBettingState.underLayOdds, 0.0)

    def test_backing_over_after_laying_over(self):
        layReward = self.emptyBettingState.place_over_lay_bet(odds=self.defaultOdds[0])
        backReward = self.emptyBettingState.place_over_back_bet(odds=self.updatedOdds[0])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), -0.20)
        self.assertEqual(self.emptyBettingState.overBackOdds, 0.0)
        self.assertEqual(self.emptyBettingState.overLayOdds, 0.0)

    def test_backing_under_after_laying_under(self):
        layReward = self.emptyBettingState.place_under_lay_bet(odds=self.defaultOdds[1])
        backReward = self.layedBettingState.place_under_back_bet(odds=self.updatedOdds[1])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.75)
        self.assertEqual(self.layedBettingState.underBackOdds, 0.0)
        self.assertEqual(self.layedBettingState.underLayOdds, 0.0)

    # OVERALL RETURN FROM STATE
    def test_for_incorrect_outcome_vector_size_1(self):
        self.assertRaises(Exception, self.backedBettingState.calculate_return, [1, 0, 0])

    def test_calculated_return_1(self):
        state = OverUnderBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        state.place_over_back_bet(odds=self.defaultOdds[0])
        state.place_under_back_bet(odds=self.defaultOdds[1])
        overWinCalculatedReturn = state.calculate_return(outcomeVector=[1, 0])
        self.assertEqual(overWinCalculatedReturn, 0)
        underWinCalculatedReturn = state.calculate_return(outcomeVector=[0, 1])
        self.assertEqual(underWinCalculatedReturn, 0.5)

    def test_calculated_return_2(self):
        state = OverUnderBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        state.place_over_lay_bet(odds=self.defaultOdds[0])
        state.place_under_back_bet(odds=self.defaultOdds[1])
        overWinCalculatedReturn = state.calculate_return(outcomeVector=[1, 0])
        self.assertEqual(overWinCalculatedReturn, -2.0)
        underWinCalculatedReturn = state.calculate_return(outcomeVector=[0, 1])
        self.assertEqual(underWinCalculatedReturn, 2.5)

    def test_calculated_return_3(self):
        state = OverUnderBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        state.place_over_lay_bet(odds=self.defaultOdds[0])
        state.place_under_lay_bet(odds=self.defaultOdds[1])
        overWinCalculatedReturn = state.calculate_return(outcomeVector=[1, 0])
        self.assertEqual(overWinCalculatedReturn, 0)
        underWinCalculatedReturn = state.calculate_return(outcomeVector=[0, 1])
        self.assertEqual(underWinCalculatedReturn, -0.5)

    # RETURN WHEN DISCOUNTING IS USED
    def test_for_incorrect_outcome_vector_size_2(self):
        self.assertRaises(
            Exception,
            self.backedBettingState.calculate_return_for_discounted_rl,
            [1, 0, 0],
        )

    def test_calculated_return_for_discounted_rl_1(self):
        state = OverUnderBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        discountedOverReward = state.place_over_back_bet(odds=self.defaultOdds[0])
        discountedUnderReward = state.place_under_back_bet(odds=self.defaultOdds[1])
        discountedRewards = sum((discountedOverReward.reward, discountedUnderReward.reward))
        overWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[1, 0])
        self.assertEqual(overWinCalculatedReturn, 2.0)
        self.assertEqual(
            overWinCalculatedReturn + discountedRewards, 0
        )  # this is same example as non-discounted, so this checks results are equal without discounting
        underWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[0, 1])
        self.assertEqual(underWinCalculatedReturn, 2.5)
        self.assertEqual(underWinCalculatedReturn + discountedRewards, 0.5)

    def test_calculated_return_for_discounted_rl_2(self):
        state = OverUnderBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        discountedOverReward = state.place_over_lay_bet(odds=self.defaultOdds[0])
        discountedUnderReward = state.place_under_back_bet(odds=self.defaultOdds[1])
        discountedRewards = sum((discountedOverReward.reward, discountedUnderReward.reward))
        overWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[1, 0])
        self.assertEqual(overWinCalculatedReturn, 0)
        self.assertEqual(overWinCalculatedReturn + discountedRewards, -2.0)
        underWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[0, 1])
        self.assertEqual(underWinCalculatedReturn, 4.5)
        self.assertEqual(underWinCalculatedReturn + discountedRewards, 2.5)

    def test_calculated_return_for_discounted_rl_3(self):
        state = OverUnderBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        discountedOverReward = state.place_over_back_bet(odds=self.defaultOdds[0])
        discountedUnderReward = state.place_under_lay_bet(odds=self.defaultOdds[1])
        discountedRewards = sum((discountedOverReward.reward, discountedUnderReward.reward))
        overWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[1, 0])
        self.assertEqual(overWinCalculatedReturn, 4.5)
        self.assertEqual(overWinCalculatedReturn + discountedRewards, 2.0)
        underWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[0, 1])
        self.assertEqual(underWinCalculatedReturn, 0)
        self.assertEqual(underWinCalculatedReturn + discountedRewards, -2.5)

    # RESETS
    def test_reset_1(self):
        self.backedBettingState.reset()
        self.assertEqual(self.backedBettingState.overBackOdds, 0)
        self.assertEqual(self.backedBettingState.underBackOdds, 0)
        self.assertEqual(self.backedBettingState.overLayOdds, 0)
        self.assertEqual(self.backedBettingState.underLayOdds, 0)

    def test_reset_2(self):
        self.layedBettingState.reset()
        self.assertEqual(self.layedBettingState.overBackOdds, 0)
        self.assertEqual(self.layedBettingState.underBackOdds, 0)
        self.assertEqual(self.layedBettingState.overLayOdds, 0)
        self.assertEqual(self.layedBettingState.underLayOdds, 0)

    # INITIALISING FROM SAVED STATE
    def test_initialize_saved_backed_state(self):
        testFrame = pd.DataFrame(
            [
                {
                    "over_backed_odds": self.defaultOdds[0],
                    "under_backed_odds": self.defaultOdds[1],
                    "over_layed_odds": 0,
                    "under_layed_odds": 0,
                }
            ]
        )
        self.emptyBettingState.from_saved_state(
            overBackOdds=float(testFrame["over_backed_odds"]),
            underBackOdds=float(testFrame["under_backed_odds"]),
            overLayOdds=float(testFrame["over_layed_odds"]),
            underLayOdds=float(testFrame["under_layed_odds"]),
        )
        truthArray = self.emptyBettingState.get_state_observation() == np.array(
            [self.defaultOdds[0], self.defaultOdds[1], 0, 0]
        )
        self.assertTrue(truthArray.all())

    def test_initialize_saved_layed_state(self):
        testFrame = pd.DataFrame(
            [
                {
                    "over_backed_odds": 0,
                    "under_backed_odds": 0,
                    "over_layed_odds": self.defaultOdds[0],
                    "under_layed_odds": self.defaultOdds[1],
                }
            ]
        )
        self.emptyBettingState.from_saved_state(
            overBackOdds=float(testFrame["over_backed_odds"]),
            underBackOdds=float(testFrame["under_backed_odds"]),
            overLayOdds=float(testFrame["over_layed_odds"]),
            underLayOdds=float(testFrame["under_layed_odds"]),
        )
        truthArray = self.emptyBettingState.get_state_observation() == np.array(
            [0, 0, self.defaultOdds[0], self.defaultOdds[1]]
        )
        self.assertTrue(truthArray.all())

    def test_initialize_saved_mixed_state_1(self):
        testFrame = pd.DataFrame(
            [
                {
                    "over_backed_odds": 0,
                    "under_backed_odds": self.defaultOdds[1],
                    "over_layed_odds": self.defaultOdds[0],
                    "under_layed_odds": 0,
                }
            ]
        )
        self.emptyBettingState.from_saved_state(
            overBackOdds=float(testFrame["over_backed_odds"]),
            underBackOdds=float(testFrame["under_backed_odds"]),
            overLayOdds=float(testFrame["over_layed_odds"]),
            underLayOdds=float(testFrame["under_layed_odds"]),
        )
        truthArray = self.emptyBettingState.get_state_observation() == np.array(
            [0, self.defaultOdds[1], self.defaultOdds[0], 0]
        )
        self.assertTrue(truthArray.all())
