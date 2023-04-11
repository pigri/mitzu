from hypothesis import given, settings, HealthCheck
import mitzu.webapp.storage as S
from tests.unit.webapp.fixtures import InMemoryCache
from tests.unit.webapp.generators import connection, project, user


@given(connection())
def test_storing_connections(connection):
    storage = S.MitzuStorage(InMemoryCache())

    assert len(storage.list_connections()) == 0

    storage.set_connection(connection.id, connection)

    assert storage.get_connection(connection.id) == connection
    assert storage.list_connections() == [connection.id]

    storage.delete_connection(connection.id)
    assert len(storage.list_connections()) == 0
    assert storage.get_connection(connection.id) is None


@given(project())
@settings(suppress_health_check=(HealthCheck.too_slow,))
def test_storing_projects(project):
    storage = S.MitzuStorage(InMemoryCache())

    assert len(storage.list_projects()) == 0

    storage.set_project(project.id, project)

    assert storage.get_project(project.id) == project
    assert storage.list_projects() == [project.id]

    storage.delete_project(project.id)
    assert len(storage.list_projects()) == 0
    assert storage.get_project(project.id) is None


@given(user())
def test_storing_users(user):
    storage = S.MitzuStorage(InMemoryCache())

    assert len(storage.list_users()) == 0

    storage.set_user(user)

    assert storage.get_user_by_id(user.id) == user
    assert storage.list_users() == [user]

    storage.clear_user(user.id)
    assert len(storage.list_users()) == 0
    assert storage.get_user_by_id(user.id) is None
