from __future__ import annotations

import dash_bootstrap_components as dbc
import mitzu.webapp.webapp as MWA
from dash import Dash


def create_app(persistency_provider: MWA.PersistencyProvider):
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

    webapp = MWA.MitzuWebApp(persistency_provider, app)
    webapp.init_app()
    return app
