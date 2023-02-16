PROJECT_ID_PATH_PART = "project_id"
CONNECTION_ID_PATH_PART = "connection_id"
USER_PATH_PART = "user_id"
DASHBOARD_ID = "dashboard_id"

ADMIN_PAGE = "/admin"

EVENTS_AND_PROPERTIES_PATH = "/events"
EVENTS_AND_PROPERTIES_PROJECT_PATH = f"/events/<{PROJECT_ID_PATH_PART}>"

PROJECTS_PATH = "/projects"
PROJECTS_CREATE_PATH = "/projects/create/"
PROJECTS_EXPLORE_PATH = f"/projects/<{PROJECT_ID_PATH_PART}>/explore"
PROJECTS_MANAGE_PATH = f"/projects/<{PROJECT_ID_PATH_PART}>/manage"
PROJECTS_EXPLORE_METRIC_QUERY = "m"
PROJECTS_EXPLORE_SAVED_METRIC_QUERY = "sm"

CONNECTIONS_PATH = "/connections"
CONNECTIONS_CREATE_PATH = "/connections/create/"
CONNECTIONS_MANAGE_PATH = f"/connections/<{CONNECTION_ID_PATH_PART}>/manage"

USERS_PATH = "/users"
USERS_HOME_PATH = f"/users/<{USER_PATH_PART}>"

DASHBOARDS_PATH = "/dashboards"
DASHBOARDS_CREATE_PATH = "/dashboards/create"
DASHBOARDS_EDIT_PATH = f"/dashboards/<{DASHBOARD_ID}>/edit"
DASHBOARDS_VIEW_PATH = f"/dashboards/<{DASHBOARD_ID}>/view"

HOME_PATH = "/"

SAVED_METRICS = "/saved_metrics"

SIGN_OUT_URL = "/auth/logout"
UNAUTHORIZED_URL = "/auth/unauthorized"
REDIRECT_TO_LOGIN_URL = "/auth/redirect-to-login"
OAUTH_CODE_URL = "/auth/oauth"


class PathException(Exception):
    pass


def create_path(template: str, **kwargs) -> str:
    for k, v in kwargs.items():
        template = template.replace(f"<{k}>", v)
    return template


def get_path_value(template: str, path: str, key: str) -> str:
    for i, part in enumerate(template.split("/")):
        if part == f"<{key}>":
            path_parts = path.split("/")
            if len(path_parts) >= i:
                return path_parts[i]

    raise PathException(f"{key} is not found in {path} with template {template}")
