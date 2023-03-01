import flask
from typing import cast
import functools
from dash.exceptions import PreventUpdate
from dash.dcc import Location
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.pages.paths as P
import mitzu.webapp.service.user_service as US


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


def restricted_for_admin(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        dependencies: DEPS.Dependencies = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        )

        if dependencies.authorizer is not None:
            user_role = dependencies.authorizer.get_current_user_role(flask.request)
            if user_role is None:
                raise PreventUpdate

            if user_role != US.Role.ADMIN:
                raise PreventUpdate
            else:
                return func(*args, **kwargs)
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
