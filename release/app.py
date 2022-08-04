from __future__ import annotations

import logging
import os

import awsgi
import dash_bootstrap_components as dbc
import mitzu.webapp.persistence as P
import mitzu.webapp.webapp as MWA
from dash import Dash

logging.getLogger().setLevel(logging.INFO)
MITZU_BASEPATH = os.getenv("BASEPATH", "mitzu-webapp")


def create_app():
    if MITZU_BASEPATH.startswith("s3://"):
        pers_provider = P.S3PersistencyProvider(MITZU_BASEPATH[5:])
    else:
        pers_provider = P.FileSystemPersistencyProvider(MITZU_BASEPATH)

    app = Dash(
        __name__,
        compress=False,
        external_stylesheets=[
            dbc.themes.ZEPHYR,
            dbc.icons.BOOTSTRAP,
            "assets/components.css",
        ],
        title="Mitzu",
        update_title=None,
        suppress_callback_exceptions=True,
    )

    webapp = MWA.MitzuWebApp(pers_provider, app)
    webapp.init_app()
    return app


app = create_app()
server = app.server


def handler(event, context):
    base64_types = [
        "image/*",
        "image/x-icon",
        "image/png",
        "image/vnd.microsoft.icon",
        "image/ico",
    ]
    return awsgi.response(server, event, context, base64_types)
