from __future__ import annotations

from typing import Optional, cast

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import flask
from dash import Dash, DiskcacheManager, dcc, html, page_container
from dash.long_callback.managers import BaseLongCallbackManager
import mitzu.webapp.cache as C
import mitzu.webapp.model as WM
import mitzu.helper as H

import mitzu.webapp.configs as configs
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.storage as S
import mitzu.webapp.offcanvas as OC
import mitzu.webapp.pages.explore.explore_page as EXP
import mitzu.webapp.pages.paths as P
import mitzu.webapp.service.user_service as US
from mitzu.helper import LOGGER
import json
import traceback
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
    return DiskcacheManager(cast(C.DiskMitzuCache, dependencies.queue).get_disk_cache())


def create_dash_app(dependencies: Optional[DEPS.Dependencies] = None) -> Dash:
    server = flask.Flask(__name__)
    if dependencies is None:
        dependencies = DEPS.Dependencies.from_configs()

    @server.before_request
    def before_request():
        request = flask.request
        return dependencies.authorizer.authorize_request(request)

    @server.after_request
    def after_request(response: flask.Response):
        request = flask.request
        if dependencies is not None:
            return dependencies.authorizer.refresh_auth_token(request, response)
        return response

    with server.app_context():
        dependencies.storage.init_db_schema()
        try:
            if configs.AUTH_ROOT_USER_EMAIL:
                dependencies.user_service.new_user(
                    configs.AUTH_ROOT_USER_EMAIL,
                    configs.AUTH_ROOT_PASSWORD,
                    configs.AUTH_ROOT_PASSWORD,
                    role=WM.Role.ADMIN,
                )
                H.LOGGER.info(f"Root user {configs.AUTH_ROOT_USER_EMAIL} is created.")
        except US.UserAlreadyExists:
            H.LOGGER.info(f"Root user {configs.AUTH_ROOT_USER_EMAIL} already exists.")

        flask.current_app.config[DEPS.CONFIG_KEY] = dependencies
        if configs.SETUP_SAMPLE_PROJECT:
            S.setup_sample_project(dependencies.storage)

    dependencies.navbar_service.register_navbar_item_provider(
        "left",
        EXP.metric_type_navbar_provider,
        priority=20,
    )
    dependencies.navbar_service.register_navbar_item_provider(
        "left",
        EXP.save_metric_navbar_provider,
        priority=40,
    )
    dependencies.navbar_service.register_navbar_item_provider(
        "left",
        EXP.share_button_navbar_provider,
        priority=30,
    )

    app = Dash(
        __name__,
        compress=True,
        server=server,
        external_stylesheets=[
            MDB_CSS,
            dbc.icons.BOOTSTRAP,
            "/assets/explore_page.css",
        ],
        title=configs.DASH_TITLE,
        update_title=None,
        suppress_callback_exceptions=True,
        use_pages=True,
        background_callback_manager=get_callback_manager(dependencies),
    )
    app._favicon = configs.DASH_FAVICON_PATH
    app.layout = create_webapp_layout(dependencies)

    @server.route(P.HEALTHCHECK_PATH)
    def healthcheck():
        dependencies: DEPS.Dependencies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )
        try:
            # These will fail with exception if there is an underlying issue
            dependencies.queue.health_check()
            dependencies.cache.health_check()
            dependencies.storage.health_check()
        except Exception as exc:
            traceback.print_exc()
            return flask.Response(
                json.dumps({"status": "fail", "message": str(exc)}), status=500
            )
        return flask.Response('{"status": "ok"}', status=200)

    return app


if __name__ == "__main__":
    app = create_dash_app()

    app.run(debug=True)
