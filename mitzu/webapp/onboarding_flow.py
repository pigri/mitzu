from __future__ import annotations

from typing import List
from dataclasses import dataclass

import mitzu.webapp.storage as S


@dataclass(frozen=True)
class OnboardingFlow:
    _states: List[str]

    @classmethod
    def flow_id(self) -> str:
        raise NotImplementedError()

    @classmethod
    def initial_state(self) -> str:
        raise NotImplementedError()

    @classmethod
    def terminating_state(self) -> str:
        raise NotImplementedError()

    def next_state(self, current_state: str) -> str:
        index = self._states.index(current_state)
        return self._states[index + 1]

    def get_implicitly_completed_states(self, storage: S.MitzuStorage) -> List[str]:
        return []


CONNECT_WAREHOUSE = "connect_warehouse"
EXPLORE_DATA = "explore_data"
INVITE_TEAM = "invite_team"
WAITING_FOR_DISMISS = "waiting_for_dismiss"
DISMISSED = "dismissed"


@dataclass(frozen=True)
class ConfigureMitzuOnboardingFlow(OnboardingFlow):
    def __init__(self):
        super().__init__(
            _states=[
                CONNECT_WAREHOUSE,
                EXPLORE_DATA,
                INVITE_TEAM,
                WAITING_FOR_DISMISS,
                DISMISSED,
            ]
        )

    @classmethod
    def flow_id(self) -> str:
        return "configure_mitzu"

    @classmethod
    def initial_state(cls) -> str:
        return CONNECT_WAREHOUSE

    @classmethod
    def terminating_state(cls) -> str:
        return DISMISSED

    def get_implicitly_completed_states(self, storage: S.MitzuStorage) -> List[str]:
        if len(storage.list_users()) > 1:
            return [INVITE_TEAM]
        return []
