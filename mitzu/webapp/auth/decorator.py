import flask
from typing import cast
import functools
from dash.exceptions import PreventUpdate
from dash.dcc import Location
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.pages.paths as P


def restricted(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        dependencies: DEPS.Dependencies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )

        if dependencies.authorizer is not None:
            if dependencies.authorizer.is_request_authorized(flask.request):
                return func(*args, **kwargs)
            else:
                raise PreventUpdate
        else:
            return func(*args, **kwargs)

    return wrapper


def restricted_layout(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        dependencies: DEPS.Dependencies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )

        if dependencies.authorizer is not None:
            if dependencies.authorizer.is_request_authorized(flask.request):
                return func(*args, **kwargs)
            else:
                return Location("url", href=P.UNAUTHORIZED_URL)
        else:
            return func(*args, **kwargs)

    return wrapper
