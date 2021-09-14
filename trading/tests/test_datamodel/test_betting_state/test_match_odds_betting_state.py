import numpy as np
import pandas as pd

from unittest import TestCase

from trading.datamodel.betting_state.match_odds_betting_state import (
    MatchOddsBettingState,
)


class TestMatchOddsBettingState(TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.defaultHADOdds = [2.0, 2.5, 10.0]
        self.updatedHADOdds = [1.66, 10.0, 3.33]

    def _get_backed_betting_state(self):
        state = MatchOddsBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        state.place_home_back_bet(odds=self.defaultHADOdds[0])
        state.place_away_back_bet(odds=self.defaultHADOdds[1])
        state.place_draw_back_bet(odds=self.defaultHADOdds[2])
        return state

    def _get_layed_betting_state(self):
        state = MatchOddsBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        state.place_home_lay_bet(odds=self.defaultHADOdds[0])
        state.place_away_lay_bet(odds=self.defaultHADOdds[1])
        state.place_draw_lay_bet(odds=self.defaultHADOdds[2])
        return state

    def setUp(self):
        super().setUp()
        self.emptyBettingState = MatchOddsBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        self.restrictedEmptyBettingState = MatchOddsBettingState(discountFactor=0.9, onlyPositiveCashout=True)
        self.backedBettingState = self._get_backed_betting_state()
        self.layedBettingState = self._get_layed_betting_state()

    # BACK/LAY tests
    def test_empty_home_back_placing(self):
        self.emptyBettingState.place_home_back_bet(odds=self.defaultHADOdds[0])
        self.assertEqual(self.emptyBettingState.homeBackOdds, self.defaultHADOdds[0])
        self.assertEqual(self.emptyBettingState.awayBackOdds, 0)
        self.assertEqual(self.emptyBettingState.drawBackOdds, 0)

    def test_empty_away_back_placing(self):
        self.emptyBettingState.place_away_back_bet(odds=self.defaultHADOdds[1])
        self.assertEqual(self.emptyBettingState.homeBackOdds, 0)
        self.assertEqual(self.emptyBettingState.awayBackOdds, self.defaultHADOdds[1])
        self.assertEqual(self.emptyBettingState.drawBackOdds, 0)

    def test_empty_draw_back_placing(self):
        self.emptyBettingState.place_draw_back_bet(odds=self.defaultHADOdds[2])
        self.assertEqual(self.emptyBettingState.homeBackOdds, 0)
        self.assertEqual(self.emptyBettingState.awayBackOdds, 0)
        self.assertEqual(self.emptyBettingState.drawBackOdds, self.defaultHADOdds[2])

    def test_empty_home_lay_placing(self):
        self.emptyBettingState.place_home_lay_bet(odds=self.defaultHADOdds[0])
        self.assertEqual(self.emptyBettingState.homeLayOdds, self.defaultHADOdds[0])
        self.assertEqual(self.emptyBettingState.awayLayOdds, 0)
        self.assertEqual(self.emptyBettingState.drawLayOdds, 0)

    def test_empty_away_lay_placing(self):
        self.emptyBettingState.place_away_lay_bet(odds=self.defaultHADOdds[1])
        self.assertEqual(self.emptyBettingState.homeLayOdds, 0)
        self.assertEqual(self.emptyBettingState.awayLayOdds, self.defaultHADOdds[1])
        self.assertEqual(self.emptyBettingState.drawLayOdds, 0)

    def test_empty_draw_lay_placing(self):
        self.emptyBettingState.place_draw_lay_bet(odds=self.defaultHADOdds[2])
        self.assertEqual(self.emptyBettingState.homeLayOdds, 0)
        self.assertEqual(self.emptyBettingState.awayLayOdds, 0)
        self.assertEqual(self.emptyBettingState.drawLayOdds, self.defaultHADOdds[2])

    # DUPLICATE ACTION TESTS
    def test_duplicate_home_back_placing(self):
        rewardClass = self.backedBettingState.place_home_back_bet(odds=self.defaultHADOdds[0])
        self.assertEqual(rewardClass.reward, self.backedBettingState.duplicateActionPenalty)

    def test_duplicate_away_back_placing(self):
        rewardClass = self.backedBettingState.place_away_back_bet(odds=self.defaultHADOdds[1])
        self.assertEqual(rewardClass.reward, self.backedBettingState.duplicateActionPenalty)

    def test_duplicate_draw_back_placing(self):
        rewardClass = self.backedBettingState.place_draw_back_bet(odds=self.defaultHADOdds[2])
        self.assertEqual(rewardClass.reward, self.backedBettingState.duplicateActionPenalty)

    def test_duplicate_home_lay_placing(self):
        rewardClass = self.layedBettingState.place_home_lay_bet(odds=self.defaultHADOdds[0])
        self.assertEqual(rewardClass.reward, self.layedBettingState.duplicateActionPenalty)

    def test_duplicate_away_lay_placing(self):
        rewardClass = self.layedBettingState.place_away_lay_bet(odds=self.defaultHADOdds[1])
        self.assertEqual(rewardClass.reward, self.layedBettingState.duplicateActionPenalty)

    def test_duplicate_draw_lay_placing(self):
        rewardClass = self.layedBettingState.place_draw_lay_bet(odds=self.defaultHADOdds[2])
        self.assertEqual(rewardClass.reward, self.layedBettingState.duplicateActionPenalty)

    # ZERO ODDS BETS
    def test_zero_odds_home_placing(self):
        backRewardClass = self.backedBettingState.place_home_back_bet(odds=0)
        self.assertEqual(backRewardClass.reward, self.backedBettingState.duplicateActionPenalty)
        layRewardClass = self.layedBettingState.place_home_lay_bet(odds=0)
        self.assertEqual(layRewardClass.reward, self.layedBettingState.duplicateActionPenalty)
        emptyBackRewardClass = self.emptyBettingState.place_home_back_bet(odds=0)
        self.assertEqual(emptyBackRewardClass.reward, self.emptyBettingState.duplicateActionPenalty)
        emptyLayRewardClass = self.emptyBettingState.place_home_lay_bet(odds=0)
        self.assertEqual(emptyLayRewardClass.reward, self.emptyBettingState.duplicateActionPenalty)

    def test_zero_odds_away_placing(self):
        backRewardClass = self.backedBettingState.place_away_back_bet(odds=0)
        self.assertEqual(backRewardClass.reward, self.backedBettingState.duplicateActionPenalty)
        layRewardClass = self.layedBettingState.place_away_lay_bet(odds=0)
        self.assertEqual(layRewardClass.reward, self.layedBettingState.duplicateActionPenalty)
        emptyBackRewardClass = self.emptyBettingState.place_away_back_bet(odds=0)
        self.assertEqual(emptyBackRewardClass.reward, self.emptyBettingState.duplicateActionPenalty)
        emptyLayRewardClass = self.emptyBettingState.place_away_lay_bet(odds=0)
        self.assertEqual(emptyLayRewardClass.reward, self.emptyBettingState.duplicateActionPenalty)

    def test_zero_odds_draw_placing(self):
        backRewardClass = self.backedBettingState.place_draw_back_bet(odds=0)
        self.assertEqual(backRewardClass.reward, self.backedBettingState.duplicateActionPenalty)
        layRewardClass = self.layedBettingState.place_draw_lay_bet(odds=0)
        self.assertEqual(layRewardClass.reward, self.layedBettingState.duplicateActionPenalty)
        emptyBackRewardClass = self.emptyBettingState.place_draw_back_bet(odds=0)
        self.assertEqual(emptyBackRewardClass.reward, self.emptyBettingState.duplicateActionPenalty)
        emptyLayRewardClass = self.emptyBettingState.place_draw_lay_bet(odds=0)
        self.assertEqual(emptyLayRewardClass.reward, self.emptyBettingState.duplicateActionPenalty)

    # CASHING OUT TESTS (written when back + lay stakes were £1)
    def test_laying_home_after_backing_home(self):
        backReward = self.emptyBettingState.place_home_back_bet(odds=self.defaultHADOdds[0])
        layReward = self.emptyBettingState.place_home_lay_bet(odds=self.updatedHADOdds[0])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.2)
        self.assertEqual(round(layReward.reward, 2), 1.2)
        self.assertEqual(self.emptyBettingState.homeBackOdds, 0.0)
        self.assertEqual(self.emptyBettingState.homeLayOdds, 0.0)

    def test_laying_away_after_backing_away(self):
        backReward = self.emptyBettingState.place_away_back_bet(odds=self.defaultHADOdds[1])
        layReward = self.emptyBettingState.place_away_lay_bet(odds=self.updatedHADOdds[1])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), -0.75)
        self.assertEqual(round(layReward.reward, 3), 0.25)
        self.assertEqual(self.emptyBettingState.awayBackOdds, 0.0)
        self.assertEqual(self.emptyBettingState.awayLayOdds, 0.0)

    def test_laying_draw_after_backing_draw(self):
        backReward = self.emptyBettingState.place_draw_back_bet(odds=self.defaultHADOdds[2])
        layReward = self.emptyBettingState.place_draw_lay_bet(odds=self.updatedHADOdds[2])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 2.00)
        self.assertEqual(round(layReward.reward, 2), 3.00)
        self.assertEqual(self.emptyBettingState.drawBackOdds, 0.0)
        self.assertEqual(self.emptyBettingState.drawLayOdds, 0.0)

    def test_backing_home_after_laying_home(self):
        layReward = self.emptyBettingState.place_home_lay_bet(odds=self.defaultHADOdds[0])
        backReward = self.emptyBettingState.place_home_back_bet(odds=self.updatedHADOdds[0])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), -0.20)
        self.assertEqual(round(backReward.reward, 2), 0.80)
        self.assertEqual(self.emptyBettingState.homeBackOdds, 0.0)
        self.assertEqual(self.emptyBettingState.homeLayOdds, 0.0)

    def test_backing_away_after_laying_away(self):
        layReward = self.emptyBettingState.place_away_lay_bet(odds=self.defaultHADOdds[1])
        backReward = self.emptyBettingState.place_away_back_bet(odds=self.updatedHADOdds[1])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.75)
        self.assertEqual(round(backReward.reward, 2), 2.25)
        self.assertEqual(self.emptyBettingState.awayBackOdds, 0.0)
        self.assertEqual(self.emptyBettingState.awayLayOdds, 0.0)

    def test_backing_draw_after_laying_draw(self):
        layReward = self.emptyBettingState.place_draw_lay_bet(odds=self.defaultHADOdds[2])
        backReward = self.emptyBettingState.place_draw_back_bet(odds=self.updatedHADOdds[2])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), -2.00)
        self.assertEqual(round(backReward.reward, 2), 7.00)
        self.assertEqual(self.emptyBettingState.drawBackOdds, 0.0)
        self.assertEqual(self.emptyBettingState.drawLayOdds, 0.0)

    # ONLY POSITIVE CASHOUT TESTS
    def test_restricted_state_valid_home_back_then_lay(self):
        backReward = self.restrictedEmptyBettingState.place_home_back_bet(odds=self.defaultHADOdds[0])
        layReward = self.restrictedEmptyBettingState.place_home_lay_bet(odds=self.updatedHADOdds[0])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.20)
        self.assertEqual(round(layReward.reward, 2), 1.20)
        self.assertEqual(self.restrictedEmptyBettingState.homeBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.homeLayOdds, 0.0)

    def test_restricted_state_invalid_home_back_then_lay(self):
        self.restrictedEmptyBettingState.place_home_back_bet(odds=self.updatedHADOdds[0])
        layReward = self.restrictedEmptyBettingState.place_home_lay_bet(odds=self.defaultHADOdds[0])
        self.assertEqual(layReward.reward, self.restrictedEmptyBettingState.duplicateActionPenalty)
        self.assertEqual(self.restrictedEmptyBettingState.homeBackOdds, self.updatedHADOdds[0])
        self.assertEqual(self.restrictedEmptyBettingState.homeLayOdds, 0.0)

    def test_restricted_state_valid_away_back_then_lay(self):
        backReward = self.restrictedEmptyBettingState.place_away_back_bet(odds=self.updatedHADOdds[1])
        layReward = self.restrictedEmptyBettingState.place_away_lay_bet(odds=self.defaultHADOdds[1])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 3.00)
        self.assertEqual(layReward.reward, 4.0)
        self.assertEqual(self.restrictedEmptyBettingState.awayBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.awayLayOdds, 0.0)

    def test_restricted_state_invalid_away_back_then_lay(self):
        self.restrictedEmptyBettingState.place_away_back_bet(odds=self.defaultHADOdds[1])
        layReward = self.restrictedEmptyBettingState.place_away_lay_bet(odds=self.updatedHADOdds[1])
        self.assertEqual(layReward.reward, self.restrictedEmptyBettingState.duplicateActionPenalty)
        self.assertEqual(self.restrictedEmptyBettingState.awayBackOdds, self.defaultHADOdds[1])
        self.assertEqual(self.restrictedEmptyBettingState.awayLayOdds, 0.0)

    def test_restricted_state_valid_draw_back_then_lay(self):
        backReward = self.restrictedEmptyBettingState.place_draw_back_bet(odds=self.defaultHADOdds[2])
        layReward = self.restrictedEmptyBettingState.place_draw_lay_bet(odds=self.updatedHADOdds[2])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 2.00)
        self.assertEqual(round(layReward.reward, 2), 3.00)
        self.assertEqual(self.restrictedEmptyBettingState.drawBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.drawLayOdds, 0.0)

    def test_restricted_state_invalid_draw_back_then_lay(self):
        self.restrictedEmptyBettingState.place_draw_back_bet(odds=self.updatedHADOdds[2])
        layReward = self.restrictedEmptyBettingState.place_draw_lay_bet(odds=self.defaultHADOdds[2])
        self.assertEqual(layReward.reward, self.restrictedEmptyBettingState.duplicateActionPenalty)
        self.assertEqual(self.restrictedEmptyBettingState.drawBackOdds, self.updatedHADOdds[2])
        self.assertEqual(self.restrictedEmptyBettingState.drawLayOdds, 0.0)

    def test_restricted_state_valid_home_lay_then_back(self):
        layReward = self.restrictedEmptyBettingState.place_home_lay_bet(odds=self.updatedHADOdds[0])
        backReward = self.restrictedEmptyBettingState.place_home_back_bet(odds=self.defaultHADOdds[0])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.17)
        self.assertEqual(round(backReward.reward, 2), 0.83)
        self.assertEqual(self.restrictedEmptyBettingState.homeBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.homeLayOdds, 0.0)

    def test_restricted_invalid_state_home_lay_then_back(self):
        self.restrictedEmptyBettingState.place_home_lay_bet(odds=self.defaultHADOdds[0])
        backReward = self.restrictedEmptyBettingState.place_home_back_bet(odds=self.updatedHADOdds[0])
        self.assertEqual(
            round(backReward.reward, 2),
            self.restrictedEmptyBettingState.duplicateActionPenalty,
        )
        self.assertEqual(self.restrictedEmptyBettingState.homeBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.homeLayOdds, self.defaultHADOdds[0])

    def test_restricted_state_valid_away_lay_then_back(self):
        layReward = self.restrictedEmptyBettingState.place_away_lay_bet(odds=self.defaultHADOdds[1])
        backReward = self.restrictedEmptyBettingState.place_away_back_bet(odds=self.updatedHADOdds[1])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.75)
        self.assertEqual(round(backReward.reward, 2), 2.25)
        self.assertEqual(self.restrictedEmptyBettingState.awayBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.awayLayOdds, 0.0)

    def test_restricted_invalid_state_away_lay_then_back(self):
        self.restrictedEmptyBettingState.place_away_lay_bet(odds=self.updatedHADOdds[1])
        backReward = self.restrictedEmptyBettingState.place_away_back_bet(odds=self.defaultHADOdds[1])
        self.assertEqual(
            round(backReward.reward, 2),
            self.restrictedEmptyBettingState.duplicateActionPenalty,
        )
        self.assertEqual(self.restrictedEmptyBettingState.awayBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.awayLayOdds, self.updatedHADOdds[1])

    def test_restricted_state_valid_draw_lay_then_back(self):
        layReward = self.restrictedEmptyBettingState.place_draw_lay_bet(odds=self.updatedHADOdds[2])
        backReward = self.restrictedEmptyBettingState.place_draw_back_bet(odds=self.defaultHADOdds[2])
        self.assertEqual(round(sum((backReward.reward, layReward.reward)), 2), 0.67)
        self.assertEqual(round(backReward.reward, 2), 3)
        self.assertEqual(self.restrictedEmptyBettingState.drawBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.drawLayOdds, 0.0)

    def test_restricted_invalid_state_draw_lay_then_back(self):
        self.restrictedEmptyBettingState.place_draw_lay_bet(odds=self.defaultHADOdds[2])
        backReward = self.restrictedEmptyBettingState.place_draw_back_bet(odds=self.updatedHADOdds[2])
        self.assertEqual(
            round(backReward.reward, 2),
            self.restrictedEmptyBettingState.duplicateActionPenalty,
        )
        self.assertEqual(self.restrictedEmptyBettingState.drawBackOdds, 0.0)
        self.assertEqual(self.restrictedEmptyBettingState.drawLayOdds, self.defaultHADOdds[2])

    # OVERALL RETURN FROM STATE
    def test_for_incorrect_outcome_vector_size_1(self):
        self.assertRaises(Exception, self.backedBettingState.calculate_return, [1, 0])

    def test_calculated_return_1(self):
        state = MatchOddsBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        state.place_home_back_bet(odds=self.defaultHADOdds[0])
        state.place_away_back_bet(odds=self.defaultHADOdds[1])
        state.place_draw_lay_bet(odds=self.defaultHADOdds[2])
        homeWinCalculatedReturn = state.calculate_return(outcomeVector=[1, 0, 0])
        self.assertEqual(homeWinCalculatedReturn, 1.0)
        awayWinCalculatedReturn = state.calculate_return(outcomeVector=[0, 1, 0])
        self.assertEqual(awayWinCalculatedReturn, 1.5)
        drawCalculatedReturn = state.calculate_return(outcomeVector=[0, 0, 1])
        self.assertEqual(drawCalculatedReturn, -11.0)

    def test_calculated_return_2(self):
        state = MatchOddsBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        state.place_home_lay_bet(odds=self.defaultHADOdds[0])
        state.place_away_back_bet(odds=self.defaultHADOdds[1])
        state.place_draw_lay_bet(odds=self.defaultHADOdds[2])
        homeWinCalculatedReturn = state.calculate_return(outcomeVector=[1, 0, 0])
        self.assertEqual(homeWinCalculatedReturn, -1.0)
        awayWinCalculatedReturn = state.calculate_return(outcomeVector=[0, 1, 0])
        self.assertEqual(awayWinCalculatedReturn, 3.5)
        drawCalculatedReturn = state.calculate_return(outcomeVector=[0, 0, 1])
        self.assertEqual(drawCalculatedReturn, -9.0)

    def test_calculated_return_3(self):
        state = MatchOddsBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        state.place_home_lay_bet(odds=self.defaultHADOdds[0])
        state.place_away_lay_bet(odds=self.defaultHADOdds[1])
        state.place_draw_back_bet(odds=self.defaultHADOdds[2])
        homeWinCalculatedReturn = state.calculate_return(outcomeVector=[1, 0, 0])
        self.assertEqual(homeWinCalculatedReturn, -1.0)
        awayWinCalculatedReturn = state.calculate_return(outcomeVector=[0, 1, 0])
        self.assertEqual(awayWinCalculatedReturn, -1.5)
        drawCalculatedReturn = state.calculate_return(outcomeVector=[0, 0, 1])
        self.assertEqual(drawCalculatedReturn, 11.0)

    # RETURN WHEN DISCOUNTING IS USED
    def test_for_incorrect_outcome_vector_size_2(self):
        self.assertRaises(
            Exception,
            self.backedBettingState.calculate_return_for_discounted_rl,
            [1, 0],
        )

    def test_calculated_return_for_discounted_rl_1(self):
        state = MatchOddsBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        discountedHomeReward = state.place_home_back_bet(odds=self.defaultHADOdds[0])
        discountedAwayReward = state.place_away_back_bet(odds=self.defaultHADOdds[1])
        discountedDrawReward = state.place_draw_lay_bet(odds=self.defaultHADOdds[2])
        discountedRewards = sum(
            (
                discountedHomeReward.reward,
                discountedAwayReward.reward,
                discountedDrawReward.reward,
            )
        )
        homeWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[1, 0, 0])
        self.assertEqual(homeWinCalculatedReturn, 12.0)
        self.assertEqual(
            homeWinCalculatedReturn + discountedRewards, 1.0
        )  # this is same example as non-discounted, so this checks results are equal without discounting
        awayWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[0, 1, 0])
        self.assertEqual(awayWinCalculatedReturn, 12.5)
        self.assertEqual(awayWinCalculatedReturn + discountedRewards, 1.5)
        drawCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[0, 0, 1])
        self.assertEqual(drawCalculatedReturn, 0)
        self.assertEqual(drawCalculatedReturn + discountedRewards, -11.0)

    def test_calculated_return_for_discounted_rl_2(self):
        state = MatchOddsBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        discountedHomeReward = state.place_home_lay_bet(odds=self.defaultHADOdds[0])
        discountedAwayReward = state.place_away_back_bet(odds=self.defaultHADOdds[1])
        discountedDrawReward = state.place_draw_lay_bet(odds=self.defaultHADOdds[2])
        discountedRewards = sum(
            (
                discountedHomeReward.reward,
                discountedAwayReward.reward,
                discountedDrawReward.reward,
            )
        )
        homeWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[1, 0, 0])
        self.assertEqual(homeWinCalculatedReturn, 10.0)
        self.assertEqual(homeWinCalculatedReturn + discountedRewards, -1.0)
        awayWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[0, 1, 0])
        self.assertEqual(awayWinCalculatedReturn, 14.5)
        self.assertEqual(awayWinCalculatedReturn + discountedRewards, 3.5)
        drawCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[0, 0, 1])
        self.assertEqual(drawCalculatedReturn, 2.0)
        self.assertEqual(drawCalculatedReturn + discountedRewards, -9.0)

    def test_calculated_return_for_discounted_rl_3(self):
        state = MatchOddsBettingState(discountFactor=0.9, onlyPositiveCashout=False)
        discountedHomeReward = state.place_home_lay_bet(odds=self.defaultHADOdds[0])
        discountedAwayReward = state.place_away_lay_bet(odds=self.defaultHADOdds[1])
        discountedDrawReward = state.place_draw_back_bet(odds=self.defaultHADOdds[2])
        discountedRewards = sum(
            (
                discountedHomeReward.reward,
                discountedAwayReward.reward,
                discountedDrawReward.reward,
            )
        )
        homeWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[1, 0, 0])
        self.assertEqual(homeWinCalculatedReturn, 2.5)
        self.assertEqual(homeWinCalculatedReturn + discountedRewards, -1.0)
        awayWinCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[0, 1, 0])
        self.assertEqual(awayWinCalculatedReturn, 2.0)
        self.assertEqual(awayWinCalculatedReturn + discountedRewards, -1.5)
        drawCalculatedReturn = state.calculate_return_for_discounted_rl(outcomeVector=[0, 0, 1])
        self.assertEqual(drawCalculatedReturn, 14.5)
        self.assertEqual(drawCalculatedReturn + discountedRewards, 11.0)

    # RESETS
    def test_reset_1(self):
        self.backedBettingState.reset()
        self.assertEqual(self.backedBettingState.homeBackOdds, 0)
        self.assertEqual(self.backedBettingState.awayBackOdds, 0)
        self.assertEqual(self.backedBettingState.drawBackOdds, 0)
        self.assertEqual(self.backedBettingState.homeLayOdds, 0)
        self.assertEqual(self.backedBettingState.awayLayOdds, 0)
        self.assertEqual(self.backedBettingState.drawLayOdds, 0)

    def test_reset_2(self):
        self.layedBettingState.reset()
        self.assertEqual(self.layedBettingState.homeBackOdds, 0)
        self.assertEqual(self.layedBettingState.awayBackOdds, 0)
        self.assertEqual(self.layedBettingState.drawBackOdds, 0)
        self.assertEqual(self.layedBettingState.homeLayOdds, 0)
        self.assertEqual(self.layedBettingState.awayLayOdds, 0)
        self.assertEqual(self.layedBettingState.drawLayOdds, 0)

    # INITIALISING FROM SAVED STATE
    def test_initialize_saved_backed_state(self):
        testFrame = pd.DataFrame(
            [
                {
                    "home_backed_odds": self.defaultHADOdds[0],
                    "away_backed_odds": self.defaultHADOdds[1],
                    "draw_backed_odds": self.defaultHADOdds[2],
                    "home_layed_odds": 0,
                    "away_layed_odds": 0,
                    "draw_layed_odds": 0,
                }
            ]
        )
        self.emptyBettingState.from_saved_state(
            homeBackOdds=float(testFrame["home_backed_odds"]),
            awayBackOdds=float(testFrame["away_backed_odds"]),
            drawBackOdds=float(testFrame["draw_backed_odds"]),
            homeLayOdds=float(testFrame["home_layed_odds"]),
            awayLayOdds=float(testFrame["away_layed_odds"]),
            drawLayOdds=float(testFrame["draw_layed_odds"]),
        )
        truthArray = self.emptyBettingState.get_state_observation() == np.array(
            [
                self.defaultHADOdds[0],
                self.defaultHADOdds[1],
                self.defaultHADOdds[2],
                0,
                0,
                0,
            ]
        )
        self.assertTrue(truthArray.all())

    def test_initialize_saved_layed_state(self):
        testFrame = pd.DataFrame(
            [
                {
                    "home_backed_odds": 0,
                    "away_backed_odds": 0,
                    "draw_backed_odds": 0,
                    "home_layed_odds": self.defaultHADOdds[0],
                    "away_layed_odds": self.defaultHADOdds[1],
                    "draw_layed_odds": self.defaultHADOdds[2],
                }
            ]
        )
        self.emptyBettingState.from_saved_state(
            homeBackOdds=float(testFrame["home_backed_odds"]),
            awayBackOdds=float(testFrame["away_backed_odds"]),
            drawBackOdds=float(testFrame["draw_backed_odds"]),
            homeLayOdds=float(testFrame["home_layed_odds"]),
            awayLayOdds=float(testFrame["away_layed_odds"]),
            drawLayOdds=float(testFrame["draw_layed_odds"]),
        )
        truthArray = self.emptyBettingState.get_state_observation() == np.array(
            [
                0,
                0,
                0,
                self.defaultHADOdds[0],
                self.defaultHADOdds[1],
                self.defaultHADOdds[2],
            ]
        )
        self.assertTrue(truthArray.all())

    def test_initialize_saved_mixed_state_1(self):
        testFrame = pd.DataFrame(
            [
                {
                    "home_backed_odds": 0,
                    "away_backed_odds": self.defaultHADOdds[1],
                    "draw_backed_odds": 0,
                    "home_layed_odds": self.defaultHADOdds[0],
                    "away_layed_odds": 0,
                    "draw_layed_odds": self.defaultHADOdds[2],
                }
            ]
        )
        self.emptyBettingState.from_saved_state(
            homeBackOdds=float(testFrame["home_backed_odds"]),
            awayBackOdds=float(testFrame["away_backed_odds"]),
            drawBackOdds=float(testFrame["draw_backed_odds"]),
            homeLayOdds=float(testFrame["home_layed_odds"]),
            awayLayOdds=float(testFrame["away_layed_odds"]),
            drawLayOdds=float(testFrame["draw_layed_odds"]),
        )
        truthArray = self.emptyBettingState.get_state_observation() == np.array(
            [
                0,
                self.defaultHADOdds[1],
                0,
                self.defaultHADOdds[0],
                0,
                self.defaultHADOdds[2],
            ]
        )
        self.assertTrue(truthArray.all())

    def test_initialize_saved_mixed_state_2(self):
        testFrame = pd.DataFrame(
            [
                {
                    "home_backed_odds": 0,
                    "away_backed_odds": self.defaultHADOdds[1],
                    "draw_backed_odds": self.defaultHADOdds[2],
                    "home_layed_odds": self.defaultHADOdds[0],
                    "away_layed_odds": 0,
                    "draw_layed_odds": 0,
                }
            ]
        )
        self.emptyBettingState.from_saved_state(
            homeBackOdds=float(testFrame["home_backed_odds"]),
            awayBackOdds=float(testFrame["away_backed_odds"]),
            drawBackOdds=float(testFrame["draw_backed_odds"]),
            homeLayOdds=float(testFrame["home_layed_odds"]),
            awayLayOdds=float(testFrame["away_layed_odds"]),
            drawLayOdds=float(testFrame["draw_layed_odds"]),
        )
        truthArray = self.emptyBettingState.get_state_observation() == np.array(
            [
                0,
                self.defaultHADOdds[1],
                self.defaultHADOdds[2],
                self.defaultHADOdds[0],
                0,
                0,
            ]
        )
        self.assertTrue(truthArray.all())
