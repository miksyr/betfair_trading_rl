from abc import ABC, abstractmethod
from typing import Union

import numpy as np
from tf_agents.environments.py_environment import PyEnvironment
from tf_agents.specs.array_spec import BoundedArraySpec
from tf_agents.trajectories import time_step, TimeStep

from trading.datamodel.discounted_reward import DiscountedReward
from trading.datamodel.odds_series.base_odds_series import BaseOddsSeries


class BaseEnvironment(PyEnvironment, ABC):
    def __init__(
        self,
        oddSeries: BaseOddsSeries,
        rewardDiscountFactor: float,
        inactionPenalty: float,
    ):
        super().__init__()
        if inactionPenalty > 0:
            raise ValueError("inactionPenalty must be <= 0")
        self.oddSeries = oddSeries
        self.oddSeries.initialize()
        self.rewardDiscountFactor = rewardDiscountFactor
        self.inactionPenalty = inactionPenalty
        self.observationDimensionality = len(self.oddSeries.get_step())
        self.oddSeries.reset()
        self.actions = None
        self.actionsNameToIdMap = None
        self._state = None
        self.oddsNormalisationConstant = self._get_odds_normalisation_constant()

    @abstractmethod
    def _get_odds_normalisation_constant(self) -> Union[int, float]:
        pass

    def get_state_observation(self) -> np.array:
        return self._state.get_state_observation()

    def action_spec(self) -> BoundedArraySpec:
        return BoundedArraySpec(
            shape=(),
            dtype=np.int32,
            minimum=0,
            maximum=len(self.actionsNameToIdMap) - 1,
            name="action",
        )

    def observation_spec(self) -> BoundedArraySpec:
        totalLength = len(self.get_state_observation()) + self.observationDimensionality
        return BoundedArraySpec(shape=(totalLength,), dtype=np.float32, minimum=0, name="observation")

    def _reset(self) -> TimeStep:
        self._state.reset()
        self.oddSeries.reset()
        return time_step.restart(
            observation=np.concatenate(
                [self._state.get_state_observation(), self.oddSeries.get_step()],
                axis=-1,
            )
            / self.oddsNormalisationConstant
        )

    def _step(self, action: int) -> TimeStep:
        nextStep = self.oddSeries.get_step()
        currentTimeStep = self.current_time_step()
        numOdds = len(self.actionsNameToIdMap) - 1
        offeredOdds = currentTimeStep.observation[-numOdds:] * self.oddsNormalisationConstant

        if self.oddSeries.episodeEnded:
            reward = self._state.calculate_return_for_discounted_rl(outcomeVector=self.oddSeries.get_end_outcome_vector())
            self.reset()
            return time_step.termination(
                observation=np.concatenate(
                    [
                        self.get_state_observation(),
                        currentTimeStep.observation[-self.observationDimensionality :],
                    ],
                    axis=-1,
                )
                / self.oddsNormalisationConstant,
                reward=reward,
            )

        if action == self.actionsNameToIdMap[self.actions.DO_NOTHING]:
            return time_step.transition(
                observation=np.concatenate([self.get_state_observation(), nextStep], axis=-1) / self.oddsNormalisationConstant,
                reward=self.inactionPenalty,
                discount=0.0,
            )

        else:
            rewardClass = self._action_processing(action=action, offeredOdds=offeredOdds)

        return time_step.transition(
            observation=np.concatenate([self.get_state_observation(), nextStep], axis=-1) / self.oddsNormalisationConstant,
            reward=rewardClass.reward,
            discount=rewardClass.discount,
        )

    @abstractmethod
    def _action_processing(self, action: int, offeredOdds: np.array) -> DiscountedReward:
        raise NotImplementedError(f"{self.__class__.__name__} must implement `_action_processing` function")
