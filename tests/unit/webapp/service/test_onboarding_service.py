import pytest

from mitzu.webapp.service.onboarding_service import OnboardingService
from mitzu.webapp.onboarding_flow import (
    ConfigureMitzuOnboardingFlow,
    CONNECT_WAREHOUSE,
    EXPLORE_DATA,
    INVITE_TEAM,
    WAITING_FOR_DISMISS,
)
import mitzu.webapp.storage as S
import mitzu.webapp.configs as configs
import mitzu.webapp.model as WM


@pytest.fixture(autouse=True)
def run_around_tests():
    yield
    configs.SECRET_ENCRYPTION_KEY = None


def get_onboarding_service() -> OnboardingService:
    storage = S.MitzuStorage()
    storage.init_db_schema()
    return OnboardingService(storage)


def test_onboarding_service_starts_the_flow_when_its_not_stored():
    service = get_onboarding_service()

    state = service.get_current_state(ConfigureMitzuOnboardingFlow.flow_id())
    assert state.flow_id == ConfigureMitzuOnboardingFlow.flow_id()
    assert state.current_state == ConfigureMitzuOnboardingFlow.initial_state()


def test_onboarding_service_progressing_flow():
    service = get_onboarding_service()

    flow_id = ConfigureMitzuOnboardingFlow.flow_id()
    service.mark_state_complete(flow_id, CONNECT_WAREHOUSE)

    state = service.get_current_state(flow_id)
    assert state.flow_id == flow_id
    assert state.current_state == EXPLORE_DATA

    # one can't jump two steps
    service.mark_state_complete(flow_id, INVITE_TEAM)
    state = service.get_current_state(flow_id)
    assert state.flow_id == flow_id
    assert state.current_state == EXPLORE_DATA

    # one can't rollback to a previous state
    service.mark_state_complete(flow_id, CONNECT_WAREHOUSE)

    state = service.get_current_state(flow_id)
    assert state.flow_id == flow_id
    assert state.current_state == EXPLORE_DATA

    # one can jump the invite team step
    service._storage.set_user(
        WM.User(email="a@b.cd", password_hash="a", password_salt="b")
    )
    service._storage.set_user(
        WM.User(email="b@b.cd", password_hash="a", password_salt="b")
    )
    service.mark_state_complete(flow_id, EXPLORE_DATA)

    state = service.get_current_state(flow_id)
    assert state.flow_id == flow_id
    assert state.current_state == WAITING_FOR_DISMISS
