from typing import Optional, cast

import dash.development.base_component as bc
import flask
from dash import html, register_page

import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.model as WM
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.dashboards.manage_dashboards_component as MDC
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted_layout


@restricted_layout
def no_connection_layout():
    return layout(None)


@restricted_layout
def layout(dashboard_id: Optional[str] = None, **query_params) -> bc.Component:
    dashboard: Optional[WM.Dashboard] = None
    if dashboard_id is not None:
        depenednecies: DEPS.Dependencies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )
        dashboard = depenednecies.storage.get_dashboard(dashboard_id)

    return html.Div(
        [
            NB.create_mitzu_navbar("create-dashboard-navbar", []),
            MDC.create_manage_dashboard_container(dashboard),
        ]
    )


register_page(
    __name__ + "_create",
    path=P.DASHBOARDS_CREATE_PATH,
    title="Mitzu - Create dashboard",
    layout=no_connection_layout,
)

register_page(
    __name__,
    path_template=P.DASHBOARDS_EDIT_PATH,
    title="Mitzu - Edit dashboard",
    layout=layout,
)
