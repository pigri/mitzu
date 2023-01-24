from pytest import fixture


import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.storage as S
import mitzu.webapp.cache as C
import mitzu.model as M
import flask
from mitzu.samples.data_ingestion import create_and_ingest_sample_project


@fixture(scope="function")
def dependencies(discovered_project: M.DiscoveredProject) -> DEPS.Dependencies:
    cache = C.InMemoryCache()
    storage = S.MitzuStorage(cache)

    project = discovered_project.project

    storage.set_connection(project.connection.id, project.connection)
    storage.set_project(project_id=project.id, project=project)
    for edt, defs in discovered_project.definitions.items():
        storage.set_event_data_table_definition(
            project_id=project.id, definitions=defs, edt_full_name=edt.get_full_name()
        )

    return DEPS.Dependencies(authorizer=None, storage=storage, cache=cache)


@fixture(scope="session")
def discovered_project() -> M.DiscoveredProject:
    connection = M.Connection(
        id=S.SAMPLE_PROJECT_ID,
        connection_name="Sample connection",
        connection_type=M.ConnectionType.SQLITE,
        host="sample_project",
    )
    project = create_and_ingest_sample_project(
        connection,
        event_count=200000,
        number_of_users=300,
        schema="main",
        overwrite_records=False,
        project_id=S.SAMPLE_PROJECT_ID,
        seed=1000,
    )
    return project.discover_project()


@fixture(scope="function")
def server(dependencies: DEPS.Dependencies) -> flask.Flask:
    app = flask.Flask(__name__)
    with app.test_request_context():
        flask.current_app.config[DEPS.CONFIG_KEY] = dependencies
    return app
