from __future__ import annotations


import dash_bootstrap_components as dbc
from typing import cast
import flask
import mitzu.webapp.dependencies as DEPS


def create_mitzu_navbar(
    id: str,
    **kwargs,
) -> dbc.Navbar:
    navbar_service = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).navbar_service
    return navbar_service.get_navbar_component(id, **kwargs)
