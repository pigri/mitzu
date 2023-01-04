from dash import register_page
import dash.development.base_component as bc
import mitzu.webapp.pages.paths as P


register_page(__name__, path_template=P.DASHBOARDS_PATH, title="Mitzu - Dashboards")


def layout() -> bc.Component:
    return "Dashboards"
