from __future__ import annotations


import dash_bootstrap_components as dbc
import mitzu.webapp.offcanvas as OC
import dash.development.base_component as bc
import mitzu.webapp.dependencies as DEPS
from dash import CeleryManager, Dash, DiskcacheManager, html, page_container, dcc
from dash.long_callback.managers import BaseLongCallbackManager
from typing import cast, Optional
from mitzu.helper import LOGGER
from mitzu.webapp.helper import MITZU_LOCATION

import flask
import mitzu.webapp.configs as configs


MAIN = "main"

MDB_CSS = "https://cdnjs.cloudflare.com/ajax/libs/mdb-ui-kit/6.0.1/mdb.min.css"
DCC_DBC_CSS = (
    "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
)


def create_webapp_layout() -> bc.Component:
    LOGGER.debug("Initializing WebApp")
    offcanvas = OC.create_offcanvas()
    location = dcc.Location(id=MITZU_LOCATION, refresh=False)
    return html.Div(
        children=[location, offcanvas, page_container],
        className=MAIN,
        id=MAIN,
    )


def get_callback_manager(dependencies: DEPS.Dependencies) -> BaseLongCallbackManager:
    if configs.REDIS_URL is not None:
        from celery import Celery

        celery_app = Celery(
            __name__, broker=configs.REDIS_URL, backend=configs.REDIS_URL
        )
        return CeleryManager(
            celery_app,
            cache_by=[lambda: configs.LAUNCH_UID],
            expire=configs.CACHE_EXPIRATION,
        )
    else:
        import mitzu.webapp.cache as C

        return DiskcacheManager(
            cast(C.DiskMitzuCache, dependencies.cache).get_disk_cache(),
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
            # dbc.themes.ZEPHRY,
            "assets/explore_page.css",
            "assets/dropdown.css",
            "assets/date_input.css",
        ],
        assets_folder=configs.DASH_ASSETS_FOLDER,
        assets_url_path=configs.DASH_ASSETS_URL_PATH,
        serve_locally=configs.DASH_SERVE_LOCALLY,
        title=configs.DASH_TITLE,
        update_title=None,
        suppress_callback_exceptions=True,
        use_pages=True,
        long_callback_manager=get_callback_manager(dependencies),
    )
    app._favicon = configs.DASH_FAVICON_PATH
    app.layout = create_webapp_layout()

    @server.route(configs.HEALTH_CHECK_PATH)
    def healthcheck():
        return flask.Response("ok", status=200)

    return app


if __name__ == "__main__":
    app = create_dash_app()

    app.run(debug=True)
