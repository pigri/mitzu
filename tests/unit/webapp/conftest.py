from tests.unit.webapp.fixtures import (  # noqa: F401
    dependencies,
    discovered_project,
    server,
)
import dash
import functools


# We make sure that dash.register_page is not called. It throws an exception without the app running
def register_page(*args, **kwargs):
    pass


# We make sure that dash.callback is not called. It throws an exception
def callback(
    *_args,
    **_kwargs,
):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator


dash.register_page = register_page
dash.callback = callback
