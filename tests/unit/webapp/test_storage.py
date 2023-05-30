import pytest
from hypothesis import HealthCheck, given, settings
import mitzu.webapp.storage as S
import mitzu.webapp.model as WM
from tests.unit.webapp.generators import (
    connection,
    project,
    discovered_project,
    saved_metric,
    user,
    dashboard,
    MAX_EXAMPLES,
)


def create_storage() -> S.MitzuStorage:
    storage = S.MitzuStorage()
    storage.init_db_schema()
    return storage


@given(connection(), connection())
@settings(deadline=None, max_examples=MAX_EXAMPLES)
def test_storing_connections(connection, updated_connection):
    object.__setattr__(updated_connection, "id", connection.id)

    storage = create_storage()

    assert len(storage.list_connections()) == 0

    storage.set_connection(connection.id, connection)

    assert storage.get_connection(connection.id) == connection
    assert storage.list_connections() == [connection.id]

    storage.set_connection(connection.id, updated_connection)
    assert storage.get_connection(connection.id) == updated_connection

    storage.delete_connection(connection.id)
    assert len(storage.list_connections()) == 0
    with pytest.raises(ValueError):
        storage.get_connection(connection.id)


@given(project(), project())
@settings(
    suppress_health_check=(HealthCheck.too_slow,),
    deadline=None,
    max_examples=MAX_EXAMPLES,
)
def test_storing_projects(project, updated_project):
    object.__setattr__(updated_project, "id", project.id)
    storage = create_storage()

    assert len(storage.list_projects()) == 0

    storage.set_project(project.id, project)

    assert storage.get_project(project.id) == project
    with storage._new_db_session() as session:
        assert len(
            storage._get_event_data_tables_for_project(project.id, session)
        ) == len(project.event_data_tables)
    assert storage.list_projects() == [WM.ProjectInfo(project.id, project.project_name)]

    storage.set_project(project.id, updated_project)

    assert storage.get_project(project.id) == updated_project
    with storage._new_db_session() as session:
        assert len(
            storage._get_event_data_tables_for_project(project.id, session)
        ) == len(updated_project.event_data_tables)

    storage.delete_project(project.id)
    assert len(storage.list_projects()) == 0
    with pytest.raises(ValueError):
        storage.get_project(project.id)

    with storage._new_db_session() as session:
        assert len(storage._get_event_data_tables_for_project(project.id, session)) == 0


@given(discovered_project())
@settings(
    suppress_health_check=(HealthCheck.too_slow,),
    deadline=None,
    max_examples=MAX_EXAMPLES,
)
def test_storing_discovered_projects(project):
    storage = create_storage()

    assert len(storage.list_projects()) == 0
    storage.set_project(project.id, project)

    stored_project = storage.get_project(project.id)
    assert stored_project is not None

    for edt in project.event_data_tables:
        assert edt in stored_project._discovered_project.get_value().definitions.keys()
        edt_definitions = stored_project._discovered_project.get_value().definitions[
            edt
        ]
        expected_definitions = project._discovered_project.get_value().definitions[edt]
        assert len(edt_definitions) == len(expected_definitions)

        for event_name, event_def in expected_definitions.items():
            assert event_name in edt_definitions.keys()
            assert isinstance(edt_definitions[event_name], S.StorageEventDefReference)

            stored_event_def = storage.get_event_definition(
                edt, edt_definitions[event_name]._id
            )
            assert stored_event_def == event_def.get_value()


@given(user(), user())
@settings(deadline=None, max_examples=MAX_EXAMPLES)
def test_storing_users(user, updated_user):
    object.__setattr__(updated_user, "id", user.id)
    storage = create_storage()

    assert len(storage.list_users()) == 0

    storage.set_user(user)

    assert storage.get_user_by_id(user.id) == user
    assert storage.list_users() == [user]

    storage.set_user(updated_user)
    assert storage.get_user_by_id(user.id) == updated_user

    storage.clear_user(user.id)
    assert len(storage.list_users()) == 0
    assert storage.get_user_by_id(user.id) is None


@given(saved_metric(), saved_metric())
@settings(
    suppress_health_check=(HealthCheck.too_slow,),
    deadline=None,
    max_examples=MAX_EXAMPLES,
)
def test_storing_saved_metrics(saved_metric, updated_saved_metric):
    object.__setattr__(updated_saved_metric, "id", saved_metric.id)
    object.__setattr__(updated_saved_metric, "_project_ref", saved_metric._project_ref)

    storage = create_storage()
    storage.set_project(saved_metric.project.id, saved_metric.project)

    assert len(storage.list_saved_metrics()) == 0

    storage.set_saved_metric(saved_metric.id, saved_metric)

    assert storage.get_saved_metric(saved_metric.id) == saved_metric
    assert storage.list_saved_metrics() == [saved_metric.id]

    storage.set_saved_metric(saved_metric.id, updated_saved_metric)

    assert storage.get_saved_metric(saved_metric.id) == updated_saved_metric

    storage.clear_saved_metric(saved_metric.id)
    assert len(storage.list_saved_metrics()) == 0
    with pytest.raises(ValueError):
        storage.get_saved_metric(saved_metric.id)


@given(dashboard(), dashboard())
@settings(
    suppress_health_check=(HealthCheck.too_slow,),
    deadline=None,
    max_examples=MAX_EXAMPLES,
)
def test_storing_dashboards(dashboard, updated_dashboard):
    object.__setattr__(updated_dashboard, "id", dashboard.id)
    object.__setattr__(
        updated_dashboard, "dashboard_metrics", dashboard.dashboard_metrics
    )
    storage = create_storage()
    for saved_dashboard_metric in [
        dm.saved_metric
        for dm in dashboard.dashboard_metrics + updated_dashboard.dashboard_metrics
    ]:
        storage.set_project(
            saved_dashboard_metric.project.id, saved_dashboard_metric.project
        )
        storage.set_saved_metric(saved_dashboard_metric.id, saved_dashboard_metric)

    assert len(storage.list_dashboards()) == 0

    storage.set_dashboard(dashboard.id, dashboard)

    d = storage.get_dashboard(dashboard.id)
    assert d.dashboard_metrics[0] == dashboard.dashboard_metrics[0]
    assert d.dashboard_metrics == dashboard.dashboard_metrics

    assert storage.get_dashboard(dashboard.id) == dashboard
    assert storage.list_dashboards() == [dashboard.id]

    storage.clear_dashboard(dashboard.id)
    assert len(storage.list_dashboards()) == 0
    with pytest.raises(ValueError):
        storage.get_dashboard(dashboard.id)
