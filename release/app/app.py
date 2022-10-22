from __future__ import annotations

import os
from multiprocessing import set_start_method
from uuid import uuid4

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
REDIS_URL = os.getenv("REDIS_URL")
DISK_CACHE_PATH = os.getenv("DISK_CACHE_PATH", "./cache")
CACHE_EXPIRATION = int(os.getenv("CACHE_EXPIRATION", "600"))
MULTIPROCESSING_START_METHOD = os.getenv("MULTIPROCESSING_START_METHOD", "forkserver")


HEALTH_CHECK_PATH = os.getenv("HEALTH_CHECK_PATH", "/_health")
LAUNCH_UID = uuid4()


def get_callback_manager() -> BaseLongCallbackManager:

    if REDIS_URL is not None:
        from celery import Celery

        celery_app = Celery(__name__, broker=REDIS_URL, backend=REDIS_URL)
        LOGGER.info(f"Setting up Celery and Redis Cache: {REDIS_URL}")
        return CeleryManager(
            celery_app, cache_by=[lambda: LAUNCH_UID], expire=CACHE_EXPIRATION
        )
    else:
        import diskcache

        LOGGER.info(f"Setting up diskcache: {DISK_CACHE_PATH}")
        cache = diskcache.Cache(DISK_CACHE_PATH)
        return DiskcacheManager(
            cache, cache_by=[lambda: LAUNCH_UID], expire=CACHE_EXPIRATION
        )


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

    @server.route(HEALTH_CHECK_PATH)
    def healthcheck():
        return flask.Response("ok", status=200)

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


set_start_method(MULTIPROCESSING_START_METHOD)
app = create_app()
server = app.server


def handler(event, context):
    try:
        return serverless_wsgi.handle_request(server, event, context)
    except Exception as exc:
        LOGGER.error(f"Failed Lambda Request: {event} -> {exc}")
        raise exc
