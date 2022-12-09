from __future__ import annotations

import os

from typing import Optional, List, Dict, Any
import logging
import dash_bootstrap_components as dbc
import diskcache
import mitzu.model as M
import mitzu.webapp.authorizer as AUTH
import mitzu.webapp.persistence as PE
import mitzu.webapp.webapp as MWA
import mitzu.helper as H
from dash import DiskcacheManager, Dash
import threading
import warnings


class SingleProjectPersistancyProvider(PE.PersistencyProvider):
    def __init__(self, project: M.DiscoveredProject) -> None:
        super().__init__()
        self.sample_project = project

    def list_projects(self) -> List[str]:
        return [PE.SAMPLE_PROJECT_NAME]

    def get_project(self, key: str) -> Optional[M.DiscoveredProject]:
        return self.sample_project


def external_dashboard(
    discovered_project: M.DiscoveredProject,
    port: Optional[int] = 8080,
    host: Optional[str] = "0.0.0.0",
    logging_level: int = logging.WARN,
    results: Optional[Dict[str, Any]] = None,
    new_thread: bool = False,
):

    warnings.filterwarnings("ignore")
    H.LOGGER.setLevel(logging_level)
    log = logging.getLogger("werkzeug")
    log.setLevel(logging_level)
    callback_manager = DiskcacheManager(diskcache.Cache("./"))
    app = Dash(
        __name__,
        compress=True,
        external_stylesheets=[
            dbc.themes.ZEPHYR,
            dbc.icons.BOOTSTRAP,
            "/assets/components.css",
        ],
        update_title=None,
        suppress_callback_exceptions=True,
        long_callback_manager=callback_manager,
    )

    webapp = MWA.MitzuWebApp(
        persistency_provider=SingleProjectPersistancyProvider(discovered_project),
        app=app,
        authorizer=AUTH.GuestMitzuAuthorizer(),
        discovered_project_cache={PE.SAMPLE_PROJECT_NAME: discovered_project},
        fixed_project_name=PE.SAMPLE_PROJECT_NAME,
        results=results,
    )

    os.environ["BACKGROUND_CALLBACK"] = str(results is None)

    webapp.init_app()
    if new_thread:
        t = threading.Thread(target=app.run_server, kwargs={"port": port, "host": host})
        t.start()
    else:
        app.run_server(port=port, host=host)
