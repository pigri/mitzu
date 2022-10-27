from __future__ import annotations

import os
from random import random
from typing import Optional

import dash_bootstrap_components as dbc
import diskcache
import mitzu.webapp.authorizer as AUTH
import mitzu.webapp.persistence as PE
import mitzu.webapp.webapp as MWA
from dash import DiskcacheManager
from jupyter_dash import JupyterDash


def dashboard(
    mode: str = "inline", port: Optional[int] = None, host: Optional[str] = None
):
    cache = diskcache.Cache("./")
    callback_manager = DiskcacheManager(cache)
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
        persistency_provider=PE.FileSystemPersistencyProvider(projects_path="./"),
        app=app,
        authorizer=AUTH.GuestMitzuAuthorizer(),
    )
    if port:
        os.environ["PORT"] = str(port)
    else:
        os.environ["PORT"] = str(18000 + int(random() * 10000))

    if host:
        os.environ["HOST"] = host
    else:
        os.environ["HOST"] = "0.0.0.0"
    os.environ["BACKGROUND_CALLBACK"] = "false"

    webapp.init_app()
    app.run_server(mode=mode)
