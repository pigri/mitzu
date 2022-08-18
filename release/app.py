from __future__ import annotations

import os

import awsgi
import dash_bootstrap_components as dbc
import flask
import mitzu.webapp.authorizer as AUTH
import mitzu.webapp.persistence as P
import mitzu.webapp.webapp as MWA
from dash import Dash

MITZU_BASEPATH = os.getenv("BASEPATH", "mitzu-webapp")
COMPRESS_RESPONSES = bool(os.getenv("COMPRESS_RESPONSES", False))
URL_BASE_PATHNAME = os.getenv("URL_BASE_PATHNAME", None)
ROUTES_PATHNAME_PREFIX = os.getenv("ROUTES_PATHNAME_PREFIX", None)
REQUESTS_PATHNAME_PREFIX = os.getenv("REQUESTS_PATHNAME_PREFIX", None)


def create_app():
    server = flask.Flask(__name__)
    if MITZU_BASEPATH.startswith("s3://"):
        pers_provider = P.S3PersistencyProvider(MITZU_BASEPATH[5:])
    else:
        pers_provider = P.FileSystemPersistencyProvider(MITZU_BASEPATH)

    app = Dash(
        __name__,
        compress=COMPRESS_RESPONSES,
        server=server,
        external_stylesheets=[
            dbc.themes.ZEPHYR,
            dbc.icons.BOOTSTRAP,
            "assets/components.css",
        ],
        url_base_pathname=URL_BASE_PATHNAME,
        routes_pathname_prefix=ROUTES_PATHNAME_PREFIX,
        requests_pathname_prefix=REQUESTS_PATHNAME_PREFIX,
        title="Mitzu",
        update_title=None,
        suppress_callback_exceptions=True,
    )
    authorizer: AUTH.MitzuAuthorizer
    try:
        authorizer = AUTH.JWTMitzuAuthorizer.from_env_vars(server=server)
    except Exception as exc:
        print(exc)
        authorizer = AUTH.GuestMitzuAuthorizer()

    webapp = MWA.MitzuWebApp(
        persistency_provider=pers_provider, app=app, authorizer=authorizer
    )
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
