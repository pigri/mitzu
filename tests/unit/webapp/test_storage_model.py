from hypothesis import given, settings, HealthCheck
from mitzu.webapp.storage_model import (
    SavedMetricStorageRecord,
    ConnectionStorageRecord,
    ProjectStorageRecord,
    EventDataTableStorageRecord,
    DiscoverySettingsStorageRecord,
    WebappSettingsStorageRecord,
    DashboardMetricStorageRecord,
    DashboardStorageRecord,
    OnboardingFlowStateStorageRecord,
)
from tests.unit.webapp.generators import (
    connection,
    project,
    event_data_table,
    saved_metric,
    dashboard_metric,
    dashboard,
    onboarding_flow_state,
    MAX_EXAMPLES,
)


@given(connection())
@settings(max_examples=MAX_EXAMPLES)
def test_connection_storage_record(connection):
    sm = ConnectionStorageRecord.from_model_instance(connection)
    assert connection == sm.as_model_instance()


@given(project())
@settings(suppress_health_check=(HealthCheck.too_slow,), max_examples=MAX_EXAMPLES)
def test_project_storage_record(project):
    sm = ProjectStorageRecord.from_model_instance(project)
    connection = ConnectionStorageRecord.from_model_instance(project.connection)
    event_data_tables = []
    for edt in project.event_data_tables:
        event_data_tables.append(
            EventDataTableStorageRecord.from_model_instance(project.id, edt)
        )
    discovery_settings = DiscoverySettingsStorageRecord.from_model_instance(
        project.discovery_settings
    )
    webapp_settings = WebappSettingsStorageRecord.from_model_instance(
        project.webapp_settings
    )
    assert project == sm.as_model_instance(
        connection=connection.as_model_instance(),
        event_data_tables=[
            edt.as_model_instance(project.id) for edt in event_data_tables
        ],
        discovery_settings=discovery_settings.as_model_instance(),
        webapp_settings=webapp_settings.as_model_instance(),
    )


@given(event_data_table())
@settings(max_examples=MAX_EXAMPLES)
def test_event_data_table_storage_record(edt):
    project_id = "project_id"
    sm = EventDataTableStorageRecord.from_model_instance(project_id, edt)
    assert edt.__dict__ == sm.as_model_instance(edt.discovery_settings).__dict__


@given(saved_metric())
@settings(suppress_health_check=(HealthCheck.too_slow,), max_examples=MAX_EXAMPLES)
def test_saved_metric_storage_record(saved_metric):

    sm = SavedMetricStorageRecord().from_model_instance(saved_metric)

    parsed_dict = sm.as_model_instance(saved_metric.project).__dict__
    for key, value in saved_metric.__dict__.items():
        if key != "chart" and not key.startswith("_"):
            assert value == parsed_dict[key]


@given(dashboard_metric())
@settings(suppress_health_check=(HealthCheck.too_slow,), max_examples=MAX_EXAMPLES)
def test_dashboard_metric_storage_record(dashboard_metric):

    dashboard_id = "dashboard_id"
    saved_metric = dashboard_metric.saved_metric
    dm = DashboardMetricStorageRecord().from_model_instance(
        dashboard_id, dashboard_metric
    )

    assert dm.as_model_instance(saved_metric) == dashboard_metric


@given(dashboard())
@settings(suppress_health_check=(HealthCheck.too_slow,), max_examples=MAX_EXAMPLES)
def test_dashboard_storage_record(dashboard):
    dashboard_metrics = dashboard.dashboard_metrics
    dm = DashboardStorageRecord().from_model_instance(dashboard)

    assert dm.as_model_instance(dashboard_metrics) == dashboard


@given(onboarding_flow_state())
@settings(max_examples=MAX_EXAMPLES)
def test_onboarding_flow_state(flow_state):
    state = OnboardingFlowStateStorageRecord().from_model_instance(flow_state)
    assert state.as_model_instance() == flow_state
