import numpy as np


class DiscountedReward:
    def __init__(self, reward: float, discount: float):
        self._reward = reward
        self._discount = discount

    @property
    def reward(self) -> float:
        return self._reward

    @property
    def logReward(self) -> float:
        return np.log(self._reward)

    @property
    def discount(self) -> float:
        return self._discount

    def update_reward(self, rewardIncrease: float) -> None:
        self._reward += rewardIncrease
