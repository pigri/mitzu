from __future__ import annotations

import logging
import os
import sys

import dash_bootstrap_components as dbc
import flask
import mitzu.webapp.authorizer as AUTH
import mitzu.webapp.persistence as P
import mitzu.webapp.webapp as MWA
import serverless_wsgi
from dash import Dash
from mitzu.webapp.helper import LOGGER

MITZU_BASEPATH = os.getenv("BASEPATH", "mitzu-webapp")
DASH_ASSETS_FOLDER = os.getenv("DASH_ASSETS_FOLDER", "assets")
DASH_ASSETS_URL_PATH = os.getenv("DASH_ASSETS_URL_PATH", "assets")
DASH_SERVER_LOCALLY = bool(os.getenv("DASH_SERVER_LOCALLY", True))
DASH_TITLE = os.getenv("DASH_TITLE", "Mitzu")
DASH_FAVICON_PATH = os.getenv("DASH_FAVICON_PATH", "assets/favicon.ico")
DASH_LOGO_PATH = os.getenv("DASH_LOGO_PATH", "assets/logo.png")
DASH_COMPONENTS_CSS = os.getenv("DASH_COMPONENTS_CSS", "assets/components.css")
DASH_COMPRESS_RESPONSES = bool(os.getenv("DASH_COMPRESS_RESPONSES", True))
LOG_HANDLER = sys.stderr if os.getenv("LOG_HANDLER") == "stderr" else sys.stdout


def setup_logger(server: flask.Flask):

    if LOGGER.getEffectiveLevel() >= logging.DEBUG:
        LOGGER.debug("Logging Enabled")

        @server.after_request
        def log_response(resp: flask.Response) -> flask.Response:
            try:
                LOGGER.debug(f"REQ URL: {flask.request.url}")
                LOGGER.debug(f"REQ PATH: {flask.request.path}")
                LOGGER.debug(f"REQ Headers: {flask.request.headers}")
                LOGGER.debug(f"RESP StatusCode: {resp.status_code}")
                LOGGER.debug(f"RESP Headers: {resp.headers}")
                LOGGER.debug(f"RESP Content: {resp.get_json()}")
            except Exception as exc:
                print(exc)
            return resp


def create_app():
    server = flask.Flask(__name__)
    if MITZU_BASEPATH.startswith("s3://"):
        pers_provider = P.S3PersistencyProvider(MITZU_BASEPATH[5:])
    else:
        pers_provider = P.FileSystemPersistencyProvider(MITZU_BASEPATH)

    setup_logger(server)

    app = Dash(
        __name__,
        compress=DASH_COMPRESS_RESPONSES,
        server=server,
        external_stylesheets=[
            dbc.themes.ZEPHYR,
            dbc.icons.BOOTSTRAP,
            DASH_COMPONENTS_CSS,
        ],
        assets_folder=DASH_ASSETS_FOLDER,
        assets_url_path=DASH_ASSETS_URL_PATH,
        serve_locally=DASH_SERVER_LOCALLY,
        title=DASH_TITLE,
        update_title=None,
        suppress_callback_exceptions=True,
    )
    app._favicon = DASH_FAVICON_PATH

    authorizer: AUTH.MitzuAuthorizer
    unauthorized_url = os.getenv(AUTH.UNAUTHORIZED_URL)
    if unauthorized_url:
        try:
            LOGGER.info("Setting up OAuth")
            authorizer = AUTH.JWTMitzuAuthorizer.from_env_vars(server=server)
        except Exception as exc:
            LOGGER.error(exc)
            authorizer = AUTH.GuestMitzuAuthorizer()
    else:
        authorizer = AUTH.GuestMitzuAuthorizer()

    webapp = MWA.MitzuWebApp(
        persistency_provider=pers_provider,
        app=app,
        authorizer=authorizer,
        logo_path=DASH_LOGO_PATH,
    )
    webapp.init_app()
    return app


app = create_app()
server = app.server


def handler(event, context):
    try:
        return serverless_wsgi.handle_request(server, event, context)
    except Exception as exc:
        LOGGER.error(f"Failed Lambda Request: {event} -> {exc}")
        raise exc
