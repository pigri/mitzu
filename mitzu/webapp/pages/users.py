from dash import register_page
import dash.development.base_component as bc
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted_layout

register_page(__name__, path_template=P.USER_PATH_PART, title="Mitzu - Users")


@restricted_layout
def layout(**query_params) -> bc.Component:
    return "Users"
