from __future__ import annotations

import os
import sys
from random import random
from typing import Optional

import dash_bootstrap_components as dbc
import flask
import mitzu.webapp.authorizer as AUTH
import mitzu.webapp.persistence as PE
import mitzu.webapp.webapp as MWA
from jupyter_dash import JupyterDash
from mitzu.notebook.component import CSS

LOG_HANDLER = sys.stdout


def dashboard(
    mode: str = "inline", port: Optional[int] = None, host: Optional[str] = None
):
    app = JupyterDash(
        __name__,
        compress=True,
        external_stylesheets=[
            dbc.themes.ZEPHYR,
            dbc.icons.BOOTSTRAP,
            "/components.css",
        ],
        update_title=None,
        suppress_callback_exceptions=True,
    )

    @app.server.route("/components.css", methods=["GET"])
    def css():
        resp = flask.Response(CSS)
        resp.content_type = "text/css"
        return resp

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

    webapp.init_app()
    app.run_server(mode=mode)
