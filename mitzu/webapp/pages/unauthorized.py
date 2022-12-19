from dash import register_page, html
import dash.development.base_component as bc

from mitzu.webapp.auth.authorizer import UNAUTHORIZED_URL
from typing import cast


register_page(
    __name__,
    path_template=UNAUTHORIZED_URL,
    title="Mitzu - Login",
)


def layout() -> bc.Component:
    return html.Div(
        [
            "Login to mitzu",
            html.A("login", href="/auth/login-start")
        ],
        className="d-flex text-center lead" 
    )
