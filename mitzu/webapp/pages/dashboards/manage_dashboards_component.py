from typing import Dict, Optional, cast, List

import dash.development.base_component as bc
import dash_bootstrap_components as dbc
import dash_draggable as dd
from dash import ALL, Input, Output, callback, ctx, html, State, no_update, dcc
import mitzu.webapp.model as WM
import mitzu.webapp.dependencies as DEPS
import flask
import dash_mantine_components as dmc
import mitzu.visualization.plot as PLT
import mitzu.visualization.charts as CHRT
from dash_iconify import DashIconify
import mitzu.serialization as SE
import mitzu.webapp.pages.paths as P
from mitzu.webapp.auth.decorator import restricted
from urllib.parse import quote
from mitzu.webapp.helper import MISSING_RESOURCE_CSS

RESPONSIVE_GRID_LAYOUT = "responsive_grid_layout"

DEF_COLUMN_COUNT = 4
DEF_HEIGHT = 8
ADD_DASHBOARD_METRIC_BUTTON = "add_dashboard_metric"
DASHBOARD_ITEM_PREFIX = "dashboard_metric"
SAVED_METRICS_OFF_CANVAS = "dashboard_saved_metrics_off_canvas"
SAVED_METRICS_CONTAINER = "dashboard_saved_metrics_container"
SAVED_METRICS_SEARCH = "dashboard_saved_metrics_search"
DELETE_SAVED_METRICS_TYPE = "dashboard_delete_saved_metric"
ADD_SAVED_METRICS_TYPE = "dashboard_add_saved_metric"
DASHBOARD_NAME_INPUT = "dashboard_name_input"
DASHBOARD_ID = "dashboard_id"
DASHBOARD_REFRESH_BUTTON = "dashboard_refresh"
DASHBOARD_METRIC_GRAPH_TYPE = "dashboard_metric_graph_type"

DASHBOARD_SPINNER = "dashboard_spinner"


def create_layout_item(id: str, x: int, y: int, w: int, h: int) -> Dict[str, Dict]:
    default = {"i": id, "x": x, "y": y, "w": w, "h": h}
    small = {"i": id, "x": 0, "y": y, "w": 1, "h": h}
    return {"lg": default, "md": default, "sm": default, "xs": small, "xxs": small}


def create_grid_width(width: int) -> Dict:
    return {"lg": width, "md": width, "sm": width, "xs": 1, "xxs": 1}


def create_saved_metric_card(saved_metric: WM.SavedMetric) -> bc.Component:
    return dbc.Card(
        [
            html.A(
                children=[
                    dbc.Row(
                        [
                            dbc.Col(
                                dbc.CardImg(
                                    src=saved_metric.small_base64,
                                    className="img-fluid rounded-start",
                                    style={"width": "120px", "height": "100px"},
                                ),
                                className="col-md-4 ps-3",
                            ),
                            dbc.Col(
                                dbc.CardBody(
                                    [
                                        html.Div(
                                            [
                                                html.H5(
                                                    saved_metric.name,
                                                    className="card-title",
                                                ),
                                            ],
                                            className="d-flex justify-content-between",
                                        ),
                                        html.P(
                                            saved_metric.description,
                                            className="card-text",
                                        ),
                                        html.Small(
                                            saved_metric.created_at.strftime("%c"),
                                            className="card-text text-muted",
                                        ),
                                    ]
                                ),
                                className="col-md-8",
                            ),
                        ],
                        className="g-0 d-flex align-items-center",
                    )
                ],
                href="#",
                className="text-dark",
                id={"type": ADD_SAVED_METRICS_TYPE, "index": saved_metric.id},
            )
        ],
        class_name="mb-3 hover-shadow",
    )


def create_saved_metrics_off_canvas() -> bc.Component:
    return dbc.Offcanvas(
        children=[
            html.Hr(),
            dbc.Button(
                [html.B(className="bi bi-plus-circle me-1"), "Explore projects"],
                href=P.PROJECTS_PATH,
                target="_blank",
                size="small",
                color="light",
                class_name="w-100 mb-3",
            ),
            dmc.TextInput(
                id=SAVED_METRICS_SEARCH,
                placeholder="Search",
                debounce=200,
                className="mb-3",
                icon=DashIconify(icon="ic:baseline-search"),
            ),
            html.Div(
                children=[html.P("Loading saved metrics", className="lead")],
                id=SAVED_METRICS_CONTAINER,
            ),
        ],
        id=SAVED_METRICS_OFF_CANVAS,
        is_open=False,
        close_button=True,
        scrollable=True,
        placement="end",
        title="Add saved metric",
    )


def create_dashboard_metric_card(dm: WM.DashboardMetric) -> bc.Component:
    index = f"{DASHBOARD_ITEM_PREFIX}_{dm.get_saved_metric_id()}"
    if (
        dm.saved_metric is None
        or dm.saved_metric.metric is None
        or dm.saved_metric.project is None
    ):
        component = html.Div(
            "Missing chart",
            className="lead text-center pt-5 w-100 h-100 d-inline-block",
        )
        name = dm.saved_metric.name if dm.saved_metric is not None else "Missing chart"
        href = None
        edit_href = None
        missing_project = True
        border_class = "border border-2 border-warning"
    else:
        component = dcc.Graph(
            id={"type": DASHBOARD_METRIC_GRAPH_TYPE, "index": dm.get_saved_metric_id()},
            className="w-100 h-100",
            figure=PLT.plot_chart(dm.saved_metric.chart, dm.saved_metric.metric),
            config={"displayModeBar": False},
            responsive=True,
        )

        metric = dm.saved_metric.metric
        name = dm.saved_metric.name
        missing_project = dm.saved_metric.project is None
        if not missing_project:
            path = P.create_path(
                P.PROJECTS_EXPLORE_PATH, project_id=dm.saved_metric.get_project_id()
            )

            href = f"{path}?m={quote(SE.to_compressed_string(metric))}"
            edit_href = f"{path}?sm={dm.saved_metric.id}"
            border_class = ""
        else:
            border_class = MISSING_RESOURCE_CSS
            href = ""
            edit_href = ""

    return html.Div(
        html.Div(
            children=[
                component,
                html.Div(
                    [
                        html.Span(
                            name,
                            className="me-2 lead user-select-none",
                        ),
                        dmc.Menu(
                            children=[
                                dmc.MenuTarget(
                                    dmc.ActionIcon(
                                        DashIconify(
                                            icon="material-symbols:menu-rounded"
                                        ),
                                        size="lg",
                                        className="me-1",
                                    ),
                                ),
                                dmc.MenuDropdown(
                                    [
                                        dmc.MenuItem(
                                            "Explore",
                                            icon=DashIconify(
                                                icon="radix-icons:external-link"
                                            ),
                                            href=href,
                                            target="_blank",
                                            disabled=missing_project,
                                        ),
                                        dmc.MenuItem(
                                            "Edit",
                                            icon=DashIconify(icon="mdi:gear-outline"),
                                            href=edit_href,
                                            target="_blank",
                                            disabled=missing_project,
                                        ),
                                        dmc.MenuDivider(),
                                        dmc.MenuItem(
                                            "Delete",
                                            icon=DashIconify(icon="tabler:trash"),
                                            color="red",
                                            id={
                                                "type": DELETE_SAVED_METRICS_TYPE,
                                                "index": dm.get_saved_metric_id(),
                                            },
                                        ),
                                    ]
                                ),
                            ],
                            position="bottom-end",
                        ),
                    ],
                    className="position-absolute end-0 d-flex",
                    style={"top": "5px"},
                ),
            ],
            className="w-100 h-100",
        ),
        id=index,
        className="w-100 h-100 " + border_class,
    )


def create_responsive_layouts(dashboard: WM.Dashboard) -> Dict:
    layouts: Dict[str, List] = {"lg": [], "md": [], "sm": [], "xs": [], "xxs": []}
    for dm in dashboard.dashboard_metrics:
        lt = create_layout_item(
            id=f"{DASHBOARD_ITEM_PREFIX}_{dm.get_saved_metric_id()}",
            x=dm.x,
            y=dm.y,
            w=dm.width,
            h=dm.height,
        )
        for key, ref in lt.items():
            layouts[key].append(ref)
    return layouts


def create_manage_dashboard_container(
    dashboard: Optional[WM.Dashboard],
) -> bc.Component:
    if dashboard is None:
        dashboard = WM.Dashboard("New dashboard")

    layouts = create_responsive_layouts(dashboard)
    return html.Div(
        [
            create_saved_metrics_off_canvas(),
            dbc.Container(
                [
                    html.Div(
                        children=dashboard.id,
                        id=DASHBOARD_ID,
                        className="d-none",
                    ),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    dmc.TextInput(
                                        type="text",
                                        id=DASHBOARD_NAME_INPUT,
                                        placeholder="New dashboard",
                                        value=dashboard.name,
                                        required=True,
                                        className="name-input mb-3 d-inline-block",
                                        debounce=500,
                                    ),
                                ],
                                lg=4,
                                sm=12,
                            ),
                            dbc.Col(
                                width="auto",
                                class_name="ms-auto",
                                children=[
                                    dbc.Button(
                                        [
                                            html.B(
                                                className="bi bi-arrow-clockwise me-1"
                                            ),
                                            "Refresh",
                                        ],
                                        color="light",
                                        class_name="d-inline-block",
                                        id=DASHBOARD_REFRESH_BUTTON,
                                    ),
                                ],
                            ),
                            dbc.Col(
                                dbc.Button(
                                    [
                                        html.B(className="bi bi-plus-circle me-1"),
                                        "Add",
                                    ],
                                    color="primary",
                                    id=ADD_DASHBOARD_METRIC_BUTTON,
                                ),
                                width="auto",
                            ),
                        ]
                    ),
                    dbc.Progress(
                        id=DASHBOARD_SPINNER,
                        style={"height": "2px"},
                        color="primary",
                        value=0,
                        class_name="my-2",
                    ),
                    dd.ResponsiveGridLayout(
                        id=RESPONSIVE_GRID_LAYOUT,
                        nrows=1,
                        height=30,
                        save=False,
                        clearSavedLayout=True,
                        gridCols=create_grid_width(4),
                        layouts=layouts,
                        children=[
                            create_dashboard_metric_card(dm)
                            for dm in dashboard.dashboard_metrics
                        ],
                    ),
                ],
                class_name="mb-10",
            ),
        ],
    )


@callback(
    Output(SAVED_METRICS_OFF_CANVAS, "is_open"),
    Output(SAVED_METRICS_CONTAINER, "children"),
    Input(ADD_DASHBOARD_METRIC_BUTTON, "n_clicks"),
    Input(SAVED_METRICS_SEARCH, "value"),
    prevent_initial_call=True,
)
@restricted
def manage_saved_metrics_off_canvas(button: int, search_value: str):
    storage = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).storage
    sm_ids = storage.list_saved_metrics()
    saved_metrics = [storage.get_saved_metric(sm_id) for sm_id in sm_ids]
    if search_value:
        saved_metrics = [
            sm
            for sm in saved_metrics
            if (not search_value or (search_value.lower() in sm.name.lower()))
        ]

    if len(saved_metrics) > 0:
        children = [create_saved_metric_card(sm) for sm in saved_metrics]
    else:
        children = html.P("No results", className="lead")

    return True, children


@callback(
    Output(RESPONSIVE_GRID_LAYOUT, "children"),
    Output(RESPONSIVE_GRID_LAYOUT, "layouts"),
    Output(RESPONSIVE_GRID_LAYOUT, "clearSavedLayout"),
    Input({"type": ADD_SAVED_METRICS_TYPE, "index": ALL}, "n_clicks"),
    Input({"type": DELETE_SAVED_METRICS_TYPE, "index": ALL}, "n_clicks"),
    State(DASHBOARD_NAME_INPUT, "value"),
    State(DASHBOARD_ID, "children"),
    prevent_initial_call=True,
)
@restricted
def manage_dashboard_content(
    link_clicks: List[int],
    delete_clicks: List[int],
    dashboard_name: int,
    dashboard_id: str,
):
    if ctx.triggered_id is None:
        return no_update, no_update, no_update

    storage = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).storage

    if ctx.triggered_id.get("type") == DELETE_SAVED_METRICS_TYPE:
        delete_metric_id = ctx.triggered_id.get("index")
        if (
            delete_clicks is None
            or all([c is None for c in delete_clicks])
            or delete_clicks is None
        ):
            return no_update, no_update, no_update
        dashboard = storage.get_dashboard(dashboard_id)
        dashboard = dashboard.update(
            [
                dm
                for dm in dashboard.dashboard_metrics
                if dm.get_saved_metric_id() != delete_metric_id
            ]
        )

        storage.set_dashboard(dashboard.id, dashboard)
        children = [
            create_dashboard_metric_card(dm) for dm in dashboard.dashboard_metrics
        ]
        layouts = create_responsive_layouts(dashboard)
        return children, layouts, True
    if ctx.triggered_id.get("type") == ADD_SAVED_METRICS_TYPE:
        new_metric_id = ctx.triggered_id.get("index")
        if (
            link_clicks is None
            or all([c is None for c in link_clicks])
            or new_metric_id is None
        ):
            return no_update, no_update, no_update

        saved_metric = storage.get_saved_metric(new_metric_id)
        dashboard = storage.get_dashboard(dashboard_id)
        max_y = 0
        if dashboard is None:
            dashboard = WM.Dashboard(
                name=dashboard_name, id=dashboard_id, dashboard_metrics=[]
            )
        elif len(dashboard.dashboard_metrics) > 0:
            max_y = max(dashboard.dashboard_metrics, key=lambda dm: dm.y).y + DEF_HEIGHT

        new_dm = WM.DashboardMetric(
            saved_metric=saved_metric,
            x=0,
            y=max_y,
            width=2,
            height=DEF_HEIGHT,
        )
        for dm in dashboard.dashboard_metrics:
            if dm.get_saved_metric_id() == new_metric_id:
                return no_update, no_update, no_update

        dashboard.dashboard_metrics.append(new_dm)
        storage.set_dashboard(dashboard.id, dashboard)

        children = [
            create_dashboard_metric_card(dm) for dm in dashboard.dashboard_metrics
        ]
        layouts = create_responsive_layouts(dashboard)
        return children, layouts, True


@callback(
    Output(DASHBOARD_NAME_INPUT, "value"),
    Input(DASHBOARD_NAME_INPUT, "value"),
    State(DASHBOARD_ID, "children"),
    prevent_initial_call=True,
)
@restricted
def dashboard_name_changed(name: str, dashboard_id: str) -> str:
    if name is None:
        return no_update
    storage = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).storage

    dashboard = storage.get_dashboard(dashboard_id)
    if dashboard is not None:
        d_name = name if name else "Unnamed dashboard"
        dashboard = dashboard.rename(d_name)
        dashboard = storage.set_dashboard(dashboard_id, dashboard)
    return name


@callback(
    Output(DASHBOARD_ID, "children"),
    Input(RESPONSIVE_GRID_LAYOUT, "layouts"),
    State(DASHBOARD_ID, "children"),
    prevent_initial_call=True,
)
@restricted
def manage_layout_change(layouts: Dict[str, Dict], dashboard_id: str) -> str:
    if dashboard_id is None:
        return no_update

    storage = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).storage

    dashboard = storage.get_dashboard(dashboard_id)
    if dashboard is None:
        return no_update

    for lt in layouts["lg"]:
        prefix = len(DASHBOARD_ITEM_PREFIX) + 1
        id = lt["i"][prefix:]
        for dm in dashboard.dashboard_metrics:
            if dm.get_saved_metric_id() == id:
                dm.x = lt["x"]
                dm.y = lt["y"]
                dm.width = lt["w"]
                dm.height = lt["h"]

    dashboard = storage.set_dashboard(dashboard_id, dashboard)
    return no_update


@callback(
    Output({"type": DASHBOARD_METRIC_GRAPH_TYPE, "index": ALL}, "figure"),
    Output(DASHBOARD_SPINNER, "value"),
    Input(DASHBOARD_REFRESH_BUTTON, "n_clicks"),
    State(DASHBOARD_ID, "children"),
    prevent_initial_call=True,
    running=[
        (Output(RESPONSIVE_GRID_LAYOUT, "style"), {"opacity": "0.3"}, {"opacity": "1"}),
    ],
    progress=[
        Output(DASHBOARD_SPINNER, "value"),
    ],
    background=True,
)
@restricted
def refresh_dashboards(set_progress, refresh_button_click: int, dashboard_id: str):
    if dashboard_id is None or refresh_button_click is None:
        return no_update, 0

    storage = cast(
        DEPS.Dependencies, flask.current_app.config.get(DEPS.CONFIG_KEY)
    ).storage

    dashboard = storage.get_dashboard(dashboard_id)
    figures = []
    total = len(dashboard.dashboard_metrics)
    for index, dm in enumerate(dashboard.dashboard_metrics):
        if dm.saved_metric is not None:
            metric = dm.saved_metric.metric
            project = dm.saved_metric.project
            if metric is None or project is None:
                continue
            simple_chart = CHRT.get_simple_chart(metric)
            fig = PLT.plot_chart(simple_chart, metric)
            sm = WM.SavedMetric(
                metric=metric,
                name=dm.saved_metric.name,
                chart=simple_chart,
                project=project,
                image_base64=dm.saved_metric.small_base64,
                small_base64=dm.saved_metric.small_base64,
                id=dm.saved_metric.id,
            )
            storage.set_saved_metric(sm.id, sm)
            dm.set_saved_metric(sm)
            figures.append(fig)
        progress = int((index + 1) * 100.0 / total)
        set_progress(progress)

    storage.set_dashboard(dashboard_id, dashboard)
    return figures, 0
