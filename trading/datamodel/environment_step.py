from typing import Dict, Optional

import numpy as np


class EnvironmentStep:
    def __init__(self, offeredOdds: np.array, additionalFeatures: Optional[Dict[str, np.array]] = None):
        self.offeredOdds = offeredOdds
        self.additionalFeatures = additionalFeatures or {}
        self.sortedFeatureKeys = sorted(list(self.additionalFeatures.keys()))

    def update_offered_odds(self, offeredOdds: np.array) -> None:
        self.offeredOdds = offeredOdds

    def get_numpy_array(self) -> np.array:
        # odds need to be last, because of the way environment gets them from `current_time_step`
        if len(self.sortedFeatureKeys) > 0:
            additionalFeatureVectors = np.concatenate(
                (self.additionalFeatures[key] for key in self.sortedFeatureKeys), axis=-1
            )
            vector = np.concatenate([additionalFeatureVectors, self.offeredOdds], axis=-1)
            return vector.astype(dtype=np.float32)
        return self.offeredOdds.astype(dtype=np.float32)
