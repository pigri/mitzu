from __future__ import annotations

import mitzu.webapp.app as WA
from dash import Dash, Input, Output, State, dcc, html
from mitzu.webapp.helper import value_to_label

CHOOSE_PROJECT_DROPDOWN = "choose-project-dropdown"

DEF_STYLE = {"font-size": 15, "padding-left": 10}
PROJECTS = ["test project"]


def create_project_dropdown(web_app: WA.MitzuWebApp):
    projects = web_app.persistency_provider.list_keys(WA.PATH_PROJECTS)
    return dcc.Dropdown(
        options=[
            {
                "label": html.Div(value_to_label(val), style=DEF_STYLE),
                "value": val,
            }
            for val in projects
        ],
        id=CHOOSE_PROJECT_DROPDOWN,
        className=CHOOSE_PROJECT_DROPDOWN,
        clearable=False,
    )


def create_callbacks(app: Dash):

    # add callback for toggling the collapse on small screens
    @app.callback(
        Output("navbar-collapse", "is_open"),
        [Input("navbar-toggler", "n_clicks")],
        [State("navbar-collapse", "is_open")],
    )
    def toggle_navbar_collapse(n, is_open):
        if n:
            return not is_open
        return is_open
