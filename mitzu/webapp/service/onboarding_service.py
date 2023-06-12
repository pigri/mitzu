from __future__ import annotations

from typing import Dict
import mitzu.webapp.storage as S
import mitzu.webapp.model as WM
import mitzu.webapp.onboarding_flow as OF


class OnboardingService:
    def __init__(self, storage: S.MitzuStorage):
        self._storage = storage
        self._onboarding_flows: Dict[str, OF.OnboardingFlow] = {}
        for flow in [OF.ConfigureMitzuOnboardingFlow()]:
            self._onboarding_flows[flow.flow_id()] = flow

    def get_current_state(self, flow_id: str) -> WM.OnboardingFlowState:
        for flow in self._storage.get_onboarding_flows():
            if flow.flow_id == flow_id:
                return flow

        initial_state = self._onboarding_flows[flow_id].initial_state()
        self._storage.set_onboarding_flow_state(
            state=WM.OnboardingFlowState(flow_id=flow_id, current_state=initial_state),
        )

        return WM.OnboardingFlowState(
            flow_id=flow_id,
            current_state=initial_state,
        )

    def mark_state_complete(self, flow_id: str, completed_state: str):
        flow = self._onboarding_flows[flow_id]

        def store_state_change(completed_state: str):
            next_state = flow.next_state(completed_state)
            self._storage.set_onboarding_flow_state(
                state=WM.OnboardingFlowState(flow_id=flow_id, current_state=next_state),
                when_current_state=completed_state,
            )

        store_state_change(completed_state)
        for other_completed_state in flow.get_implicitly_completed_states(
            self._storage
        ):
            store_state_change(other_completed_state)
