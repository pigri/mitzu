from __future__ import annotations

from typing import Optional, cast

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import flask
from dash import CeleryManager, Dash, DiskcacheManager, dcc, html, page_container
from dash.long_callback.managers import BaseLongCallbackManager

import mitzu.webapp.configs as configs
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.offcanvas as OC
from mitzu.helper import LOGGER
from mitzu.webapp.helper import MITZU_LOCATION

MAIN = "main"

MDB_CSS = "https://cdnjs.cloudflare.com/ajax/libs/mdb-ui-kit/6.0.1/mdb.min.css"
DCC_DBC_CSS = (
    "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
)


def create_webapp_layout(dependencies: DEPS.Dependencies) -> bc.Component:
    LOGGER.debug("Initializing WebApp")
    offcanvas = OC.create_offcanvas(dependencies)
    location = dcc.Location(id=MITZU_LOCATION, refresh=False)
    return html.Div(
        children=[location, offcanvas, page_container],
        className=MAIN,
        id=MAIN,
    )


def get_callback_manager(dependencies: DEPS.Dependencies) -> BaseLongCallbackManager:
    if configs.QUEUE_REDIS_HOST is not None:
        from celery import Celery

        celery_app = Celery(
            __name__, broker=configs.QUEUE_REDIS_HOST, backend=configs.QUEUE_REDIS_HOST
        )
        return CeleryManager(
            celery_app,
            cache_by=[lambda: configs.LAUNCH_UID],
            expire=configs.CACHE_EXPIRATION,
        )
    else:
        import mitzu.webapp.cache as C

        return DiskcacheManager(
            cast(C.DiskMitzuCache, dependencies.queue).get_disk_cache(),
            cache_by=[lambda: configs.LAUNCH_UID],
            expire=configs.CACHE_EXPIRATION,
        )


def create_dash_app(dependencies: Optional[DEPS.Dependencies] = None) -> Dash:
    server = flask.Flask(__name__)
    if dependencies is None:
        dependencies = DEPS.Dependencies.from_configs(server)

    with server.app_context():
        flask.current_app.config[DEPS.CONFIG_KEY] = dependencies

    app = Dash(
        __name__,
        compress=configs.DASH_COMPRESS_RESPONSES,
        server=server,
        external_stylesheets=[
            MDB_CSS,
            dbc.icons.BOOTSTRAP,
            "/assets/explore_page.css",
        ],
        assets_folder=configs.DASH_ASSETS_FOLDER,
        assets_url_path=configs.DASH_ASSETS_URL_PATH,
        serve_locally=configs.DASH_SERVE_LOCALLY,
        title=configs.DASH_TITLE,
        update_title=None,
        suppress_callback_exceptions=True,
        use_pages=True,
        background_callback_manager=get_callback_manager(dependencies),
        external_scripts=[
            "https://cdnjs.cloudflare.com/ajax/libs/dragula/3.7.2/dragula.min.js"
        ],
    )
    app._favicon = configs.DASH_FAVICON_PATH
    app.layout = create_webapp_layout(dependencies)

    @server.route(configs.HEALTH_CHECK_PATH)
    def healthcheck():
        return flask.Response("ok", status=200)

    return app


if __name__ == "__main__":
    app = create_dash_app()

    app.run(debug=True)
