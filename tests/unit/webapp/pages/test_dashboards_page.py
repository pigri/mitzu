from flask import Flask
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.model as WM
import mitzu.model as M
import mitzu.visualization.common as C
import pandas as pd
import mitzu.webapp.pages.dashboards_page as DP
import mitzu.webapp.pages.dashboards.manage_dashboards_component as MD

from tests.helper import to_dict, find_component_by_id
from unittest.mock import patch


def create_test_dashboard(
    dependencies: DEPS.Dependencies,
    discovered_project: M.DiscoveredProject,
):
    cm = discovered_project.create_notebook_class_model()
    sm = WM.SavedMetric(
        id="test_sm",
        name="test_saved_metric",
        project=discovered_project.project,
        image_base64="",
        small_base64="",
        metric=cm.page_visit,
        chart=C.SimpleChart(
            title="title",
            x_axis_label="x",
            y_axis_label="y",
            color_label="c",
            yaxis_ticksuffix="x_s",
            chart_type=M.SimpleChartType.LINE,
            dataframe=pd.DataFrame(),
        ),
    )

    dependencies.storage.set_saved_metric("test_sm", saved_metric=sm)
    dash = WM.Dashboard(
        "test_dash",
        dashboard_metrics=[WM.DashboardMetric(id="test_dm", saved_metric=sm)],
    )
    dependencies.storage.set_dashboard("test_dash", dash)
    return dash


def test_dashboards_layout(
    server: Flask,
    dependencies: DEPS.Dependencies,
    discovered_project: M.DiscoveredProject,
):
    with server.test_request_context():
        dash = create_test_dashboard(dependencies, discovered_project)
        res = DP.layout()

        assert res is not None
        layout_dict = to_dict(res)

        dash_cont = find_component_by_id(
            comp_id=DP.DASHBOARD_CONTAINER, input=layout_dict
        )
        assert dash_cont is not None
        assert len(dash_cont["children"]) == 1
        card_delete_button = find_component_by_id(
            comp_id={"type": "dashboard_delete", "index": f"{dash.id}###test_dash"},
            input=layout_dict,
        )
        assert card_delete_button is not None


@patch("mitzu.webapp.pages.dashboards_page.ctx")
def test_dashboard_delete_confirmed(
    ctx,
    server: Flask,
    dependencies: DEPS.Dependencies,
    discovered_project: M.DiscoveredProject,
):
    with server.test_request_context():
        ctx.triggered_id = DP.CONFIRM_DIALOG_ACCEPT

        dash = create_test_dashboard(dependencies, discovered_project)
        res = DP.confirm_button_clicked(1, dashboard_id=dash.id)

        assert res is not None
        layout_dict = to_dict(res)

        assert layout_dict["children"] == "You don't have any dashboards yet"
        assert dependencies.storage.list_dashboards() == []


@patch("mitzu.webapp.pages.dashboards_page.ctx")
def test_dashboard_name_changed(
    ctx,
    server: Flask,
    dependencies: DEPS.Dependencies,
    discovered_project: M.DiscoveredProject,
):
    with server.test_request_context():
        ctx.triggered_id = DP.CONFIRM_DIALOG_ACCEPT

        dash = create_test_dashboard(dependencies, discovered_project)
        res = MD.dashboard_name_changed("new_name", dashboard_id=dash.id)

        assert res == "new_name"
        assert dependencies.storage.get_dashboard(dash.id).name == "new_name"


@patch("mitzu.webapp.pages.dashboards.manage_dashboards_component.PLT")
@patch("mitzu.webapp.pages.dashboards.manage_dashboards_component.ctx")
def test_add_saved_metric(
    ctx,
    plt,
    server: Flask,
    dependencies: DEPS.Dependencies,
    discovered_project: M.DiscoveredProject,
):
    with server.test_request_context():
        ctx.triggered_id = {"type": MD.ADD_SAVED_METRICS_TYPE, "index": "test_sm_2"}
        cm = discovered_project.create_notebook_class_model()
        dash = create_test_dashboard(dependencies, discovered_project)

        dependencies.storage.set_saved_metric(
            "test_sm_2",
            saved_metric=WM.SavedMetric(
                id="test_sm_2",
                name="test_saved_metric_2",
                project=discovered_project.project,
                image_base64="",
                small_base64="",
                metric=cm.page_visit,
                chart=C.SimpleChart(
                    title="title",
                    x_axis_label="x",
                    y_axis_label="y",
                    color_label="c",
                    yaxis_ticksuffix="x_s",
                    chart_type=M.SimpleChartType.LINE,
                    dataframe=pd.DataFrame(),
                ),
            ),
        )

        chld, _, _ = MD.manage_dashboard_content(
            link_clicks=["test_sm_2"],
            delete_clicks=[],
            dashboard_name=dash.name,
            dashboard_id=dash.id,
        )

        assert len(chld) == 2
        new_dash = dependencies.storage.get_dashboard(dash.id)

        assert len(new_dash.dashboard_metrics) == 2
        assert new_dash.dashboard_metrics[1].saved_metric is not None
        assert new_dash.dashboard_metrics[1].saved_metric.name == "test_saved_metric_2"


@patch("mitzu.webapp.pages.dashboards.manage_dashboards_component.PLT")
@patch("mitzu.webapp.pages.dashboards.manage_dashboards_component.ctx")
def test_delete_saved_metric(
    ctx,
    plt,
    server: Flask,
    dependencies: DEPS.Dependencies,
    discovered_project: M.DiscoveredProject,
):
    with server.test_request_context():
        ctx.triggered_id = {"type": MD.DELETE_SAVED_METRICS_TYPE, "index": "test_sm"}
        discovered_project.create_notebook_class_model()
        dash = create_test_dashboard(dependencies, discovered_project)

        chld, _, _ = MD.manage_dashboard_content(
            link_clicks=[],
            delete_clicks=["test_sm"],
            dashboard_name=dash.name,
            dashboard_id=dash.id,
        )

        assert len(chld) == 0
        new_dash = dependencies.storage.get_dashboard(dash.id)

        assert len(new_dash.dashboard_metrics) == 0
