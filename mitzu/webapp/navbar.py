from __future__ import annotations

import dash_bootstrap_components as dbc
import mitzu.webapp.dependencies as DEPS


def create_mitzu_navbar(
    id: str,
    **kwargs,
) -> dbc.Navbar:
    navbar_service = DEPS.Dependencies.get().navbar_service
    return navbar_service.get_navbar_component(id, **kwargs)
