from pytest import fixture
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.storage as S
import mitzu.webapp.service.events_service as E
import mitzu.model as M
from mitzu.webapp.cache import MitzuCache
import flask
from mitzu.samples.data_ingestion import create_and_ingest_sample_project


@dataclass(frozen=True)
class InMemoryCache(MitzuCache):
    """For testing only"""

    _cache: Dict[str, Any] = field(default_factory=dict)

    def put(self, key: str, val: Any, expire: Optional[float] = None):
        self._cache[key] = val

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        res = self._cache.get(key)
        if res is None:
            return default
        return res

    def clear(self, key: str) -> None:
        if key in self._cache:
            self._cache.pop(key)

    def list_keys(
        self, prefix: Optional[str] = None, strip_prefix: bool = True
    ) -> List[str]:
        keys = self._cache.keys()
        start_pos = len(prefix) if strip_prefix and prefix is not None else 0
        return [k[start_pos:] for k in keys if prefix is None or k.startswith(prefix)]


@fixture(scope="function")
def dependencies() -> DEPS.Dependencies:
    cache = InMemoryCache()
    queue = InMemoryCache()
    storage = S.MitzuStorage(cache)

    evt_service = E.EventsService(storage)
    return DEPS.Dependencies(
        authorizer=None,
        storage=storage,
        cache=cache,
        queue=queue,
        events_service=evt_service,
        user_service=None,
    )


@fixture(scope="session")
def discovered_project() -> M.DiscoveredProject:
    connection = M.Connection(
        id=S.SAMPLE_CONNECTION_ID,
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
def server(
    dependencies: DEPS.Dependencies, discovered_project: M.DiscoveredProject
) -> flask.Flask:
    flask_app = flask.Flask(__name__)
    with flask_app.app_context():
        storage = dependencies.storage
        project = discovered_project.project
        flask_app.config[DEPS.CONFIG_KEY] = dependencies
        with flask_app.test_request_context():
            storage.set_connection(project.connection.id, project.connection)
            storage.set_project(project_id=project.id, project=project)
            for edt, defs in discovered_project.definitions.items():
                storage.set_event_data_table_definition(
                    project_id=project.id,
                    definitions=defs,
                    edt_full_name=edt.get_full_name(),
                )

    return flask_app
