from __future__ import annotations

import os
from random import random

from typing import Optional, List, Dict, Any
import logging
import dash_bootstrap_components as dbc
import diskcache
import mitzu.model as M
import mitzu.webapp.authorizer as AUTH
import mitzu.webapp.persistence as PE
import mitzu.webapp.webapp as MWA
import mitzu.helper as H
from dash import DiskcacheManager
from jupyter_dash import JupyterDash


class SingleProjectPersistancyProvider(PE.PersistencyProvider):
    def __init__(self, project: M.DiscoveredProject) -> None:
        super().__init__()
        self.sample_project = project

    def list_projects(self) -> List[str]:
        return [PE.SAMPLE_PROJECT_NAME]

    def get_project(self, key: str) -> Optional[M.DiscoveredProject]:
        return self.sample_project


def dashboard(
    discovered_project: M.DiscoveredProject,
    mode: str = "inline",
    port: Optional[int] = None,
    host: Optional[str] = None,
    logging_level: int = logging.WARN,
    results: Optional[Dict[str, Any]] = None,
):
    H.LOGGER.setLevel(logging_level)
    callback_manager = DiskcacheManager(diskcache.Cache("./"))
    app = JupyterDash(
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
    if port:
        os.environ["PORT"] = str(port)
    else:
        os.environ["PORT"] = str(18000 + int(random() * 10000))

    if host:
        os.environ["HOST"] = host
    else:
        os.environ["HOST"] = "0.0.0.0"

    os.environ["BACKGROUND_CALLBACK"] = str(results is None)

    webapp.init_app()
    app.run_server(mode=mode)
