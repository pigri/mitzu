from __future__ import annotations

import os

from typing import Optional, List, Dict, Any
import logging
import dash_bootstrap_components as dbc
import diskcache
import mitzu.model as M
import mitzu.webapp.storage as S
import mitzu.webapp.cache as C
import mitzu.helper as H
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.pages.explore.explore_page as EXP
import mitzu.webapp.service.events_service as ES
from dash import DiskcacheManager, Dash
import threading
import flask
import warnings


class SingleProjectMitzuStorage(S.MitzuStorage):
    def __init__(self, project: M.DiscoveredProject) -> None:
        self.sample_project = project

    def list_discovered_projects(self) -> List[str]:
        return [S.SAMPLE_PROJECT_NAME]

    def get_project(self, key: str) -> M.Project:
        return self.sample_project.project


def external_dashboard(
    discovered_project: M.DiscoveredProject,
    port: Optional[int] = 8080,
    host: Optional[str] = "0.0.0.0",
    logging_level: int = logging.WARN,
    results: Optional[Dict[str, Any]] = None,
    new_thread: bool = False,
    interactive_shell_only: bool = True,
):
    warnings.filterwarnings("ignore")

    H.LOGGER.setLevel(logging_level)
    log = logging.getLogger("werkzeug")
    log.setLevel(logging_level)
    storage = SingleProjectMitzuStorage(discovered_project)
    callback_manager = DiskcacheManager(diskcache.Cache("./"))
    dependencies = DEPS.Dependencies(
        authorizer=None,
        storage=storage,
        cache=C.DiskMitzuCache("cache"),
        queue=C.DiskMitzuCache("queue"),
        events_service=ES.EventsService(storage),
    )

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
        prevent_initial_callbacks=True,
        long_callback_manager=callback_manager,
    )

    app.layout = EXP.create_explore_page(
        query_params={},
        discovered_project=discovered_project,
        storage=dependencies.storage,
        notebook_mode=True,
    )
    EXP.create_callbacks()

    os.environ["BACKGROUND_CALLBACK"] = str(results is None)
    with app.server.app_context():
        flask.current_app.config[DEPS.CONFIG_KEY] = dependencies

    if interactive_shell_only:
        import __main__ as main

        if hasattr(main, "__file__"):
            return

    if new_thread:
        t = threading.Thread(target=app.run_server, kwargs={"port": port, "host": host})
        t.start()
    else:
        app.run_server(port=port, host=host)
