from abc import ABC, abstractmethod
from typing import Dict, Tuple


class BaseActions(ABC):
    @abstractmethod
    def get_all_actions(self) -> Tuple[str]:
        pass

    def get_action_name_to_id_mapping(self) -> Dict[str, int]:
        return {name: i for i, name in enumerate(self.get_all_actions())}

    def get_action_id_to_name_mapping(self) -> Dict[str, int]:
        return {name: i for i, name in enumerate(self.get_all_actions())}
