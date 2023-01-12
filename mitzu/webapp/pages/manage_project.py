import traceback
from typing import Any, Dict, List, Optional, cast

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import flask
from dash import ALL, Input, Output, State, callback, ctx, html, register_page

import mitzu.helper as H
import mitzu.model as M
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.helper as WH
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.paths as P
import mitzu.webapp.pages.projects.event_tables_tab as ET
import mitzu.webapp.pages.projects.helper as MPH
import mitzu.webapp.pages.projects.manage_project_component as MPC

CREATE_PROJECT_DOCS_LINK = "https://github.com/mitzu-io/mitzu/blob/main/DOCS.md"
SAVE_BUTTON = "project_save_button"
CLOSE_BUTTON = "project_close_button"
MANAGE_PROJECT_INFO = "manage_project_info"


def layout_create() -> bc.Component:
    return layout(None)


def layout(project_id: Optional[str] = None) -> bc.Component:
    project: Optional[M.Project] = None
    dependencies: DEPS.Dependencies = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    )
    if project_id is not None:
        dp = dependencies.storage.get_discovered_project(project_id)
        if dp is not None:
            project = dp.project

    title = (
        "Create new project"
        if project is None
        else f"{H.value_to_label(project.project_name)}"
    )

    return html.Div(
        [
            NB.create_mitzu_navbar("create-project-navbar", []),
            dbc.Container(
                children=[
                    html.H4(title),
                    html.Hr(),
                    dbc.Tabs(
                        children=[
                            dbc.Tab(
                                children=[
                                    MPC.create_project_settings(project, dependencies),
                                ],
                                label="Settings",
                            ),
                            dbc.Tab(
                                children=[
                                    ET.create_event_tables(project),
                                ],
                                label="Tables",
                            ),
                        ],
                        active_tab="tab-0",
                    ),
                    html.Hr(),
                    html.Div(
                        [
                            dbc.Button(
                                [html.B(className="bi bi-x"), " Close"],
                                color="secondary",
                                class_name="me-3",
                                href=P.PROJECTS_PATH,
                                external_link=True,
                                id=CLOSE_BUTTON,
                            ),
                            dbc.Button(
                                [html.B(className="bi bi-check-circle"), " Save"],
                                color="success",
                                id=SAVE_BUTTON,
                            ),
                        ],
                        className="mb-3",
                    ),
                    html.Div(
                        children="",
                        className="mb-3 lead",
                        id=MANAGE_PROJECT_INFO,
                    ),
                ],
                class_name="mb-3",
            ),
        ]
    )


@callback(
    Output(MANAGE_PROJECT_INFO, "children"),
    Input(SAVE_BUTTON, "n_clicks"),
    State(MPH.EDT_TBL_BODY, "children"),
    State({"type": MPH.PROJECT_INDEX_TYPE, "index": ALL}, "value"),
    State(WH.MITZU_LOCATION, "pathname"),
    prevent_initial_call=True,
)
def save_button_clicked(
    save_clicks: int, edt_table_rows: List, prop_values: List, pathname: str
):
    try:

        storage = cast(
            DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
        ).storage

        project_props: Dict[str, Any] = {}

        for prop in ctx.args_grouping[2]:
            id_val = prop["id"]
            if id_val.get("type") == MPH.PROJECT_INDEX_TYPE:
                project_props[id_val.get("index")] = prop["value"]

        project_id = cast(str, project_props.get(MPC.PROP_PROJECT_ID))
        connection_id = cast(str, project_props.get(MPC.PROP_CONNECTION))
        project_name = cast(str, project_props.get(MPC.PROP_PROJECT_NAME))
        description = cast(str, project_props.get(MPC.PROP_DESCRIPTION))
        disc_lookback_days = cast(int, project_props.get(MPC.PROP_DISC_LOOKBACK_DAYS))
        min_sample_size = cast(int, project_props.get(MPC.PROP_DISC_SAMPLE_SIZE))
        autorefresh_enabled = cast(
            bool, project_props.get(MPC.PROP_EXPLORE_AUTO_REFRESH)
        )

        connection = storage.get_connection(connection_id)
        event_data_tables = []
        for tr in edt_table_rows:
            full_table_name = MPH.get_value_from_row(tr, 1)
            user_id_column = MPH.get_value_from_row(tr, 2)
            event_time_column = MPH.get_value_from_row(tr, 3)
            event_name_column = MPH.get_value_from_row(tr, 4)
            date_partition_col = MPH.get_value_from_row(tr, 5)
            ignore_cols = MPH.get_value_from_row(tr, 6)

            schema, table_name = tuple(full_table_name.split("."))

            event_data_tables.append(
                M.EventDataTable.create(
                    table_name=table_name,
                    schema=schema,
                    event_time_field=event_time_column,
                    event_name_field=event_name_column,
                    ignored_fields=(
                        ignore_cols.split(",") if ignore_cols is not None else []
                    ),
                    user_id_field=user_id_column,
                    date_partition_field=date_partition_col,
                )
            )

        project = M.Project(
            project_name=project_name,
            project_id=project_id,
            connection=connection,
            description=description,
            webapp_settings=M.WebappSettings(auto_refresh_enabled=autorefresh_enabled),
            discovery_settings=M.DiscoverySettings(
                min_property_sample_size=min_sample_size,
                lookback_days=disc_lookback_days,
            ),
            event_data_tables=event_data_tables,
        )
        storage.delete_project(project_id)
        storage.set_project(project_id, project)

        return "Project succesfully saved"
    except Exception as exc:
        traceback.print_exc()
        return f"Something went wrong: {str(exc)}"


register_page(
    __name__ + "_create",
    path=P.PROJECTS_CREATE_PATH,
    title="Mitzu - Create Project",
    layout=layout_create,
)


register_page(
    __name__,
    path_template=P.PROJECTS_MANAGE_PATH,
    title="Mitzu - Manage Project",
    layout=layout,
)
