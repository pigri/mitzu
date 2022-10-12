from __future__ import annotations

import os
import sys

import dash_bootstrap_components as dbc
import flask
import mitzu.webapp.authorizer as AUTH
import mitzu.webapp.persistence as P
import mitzu.webapp.webapp as MWA
import serverless_wsgi
from dash import CeleryManager, Dash, DiskcacheManager
from dash.long_callback.managers import BaseLongCallbackManager
from mitzu.webapp.helper import LOGGER

OAUTH_SIGN_IN_URL = os.getenv("OAUTH_SIGN_IN_URL")
MITZU_BASEPATH = os.getenv("BASEPATH", "mitzu-webapp")
DASH_ASSETS_FOLDER = os.getenv("DASH_ASSETS_FOLDER", "assets")
DASH_ASSETS_URL_PATH = os.getenv("DASH_ASSETS_URL_PATH", "assets")
DASH_SERVE_LOCALLY = bool(os.getenv("DASH_SERVE_LOCALLY", True))
DASH_TITLE = os.getenv("DASH_TITLE", "Mitzu")
DASH_FAVICON_PATH = os.getenv("DASH_FAVICON_PATH", "assets/favicon.ico")
DASH_COMPONENTS_CSS = os.getenv("DASH_COMPONENTS_CSS", "assets/components.css")
DASH_COMPRESS_RESPONSES = bool(os.getenv("DASH_COMPRESS_RESPONSES", True))

LOG_HANDLER = sys.stderr if os.getenv("LOG_HANDLER") == "stderr" else sys.stdout
REDIS_URL = os.getenv("REDIS_URL")


def get_callback_manager() -> BaseLongCallbackManager:
    if REDIS_URL is not None:
        # Use Redis & Celery if REDIS_URL set as an env variable
        from celery import Celery

        celery_app = Celery(
            __name__, broker=os.environ["REDIS_URL"], backend=os.environ["REDIS_URL"]
        )
        return CeleryManager(celery_app)

    else:
        # Diskcache for non-production apps when developing locally
        import diskcache

        cache = diskcache.Cache("./cache")
        return DiskcacheManager(cache)


def create_app():
    server = flask.Flask(__name__)
    if MITZU_BASEPATH.startswith("s3://"):
        pers_provider = P.S3PersistencyProvider(MITZU_BASEPATH[5:])
    else:
        pers_provider = P.FileSystemPersistencyProvider(MITZU_BASEPATH)

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
        serve_locally=DASH_SERVE_LOCALLY,
        title=DASH_TITLE,
        update_title=None,
        suppress_callback_exceptions=True,
        long_callback_manager=get_callback_manager(),
    )
    app._favicon = DASH_FAVICON_PATH

    if OAUTH_SIGN_IN_URL is not None:
        authorizer = AUTH.JWTMitzuAuthorizer.from_env_vars(server=server)
    else:
        authorizer = AUTH.GuestMitzuAuthorizer()

    webapp = MWA.MitzuWebApp(
        persistency_provider=pers_provider,
        app=app,
        authorizer=authorizer,
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
