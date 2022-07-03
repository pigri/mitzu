from dash import dcc, html

CHOOSE_PROJECT_DROPDOWN = "choose-project-dropdown"


DEF_STYLE = {"font-size": 15, "padding-left": 10}

PROJECTS = ["test project"]


def create_project_dropdown():
    return dcc.Dropdown(
        options=[
            {
                "label": html.Div(val, style=DEF_STYLE),
                "value": val,
            }
            for val in PROJECTS
        ],
        id=CHOOSE_PROJECT_DROPDOWN,
        className=CHOOSE_PROJECT_DROPDOWN,
        clearable=False,
        value=PROJECTS[0],
    )
