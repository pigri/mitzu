import mitzu.webapp.pages.explore.explore_page as EXP
from unittest.mock import patch
from flask import Flask
from dash import no_update
import mitzu as M
import mitzu.serialization as SE
from urllib.parse import quote
import mitzu.webapp.dependencies as DEPS


@patch("mitzu.webapp.pages.explore.explore_page.ctx")
def test_metric_dialog_open(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = EXP.METRIC_SAVE_NAVBAR_BUTTON

        res = EXP.handle_save_metric_dialog(
            navbar_btn_nclicks=1,
            save_new_nclicks=0,
            replace_nclicks=0,
            close_nclicks=0,
            metric_id=0,
            metric_name="",
            href="",
        )

        assert res == {
            "metric_name_input": None,
            "metric_save_dialog": True,
            "metric_save_dialog_info": "",
        }


@patch("mitzu.webapp.pages.explore.explore_page.ctx")
def test_metric_dialog_close(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = EXP.METRIC_SAVE_DIALOG_CLOSE_BUTTON

        res = EXP.handle_save_metric_dialog(
            navbar_btn_nclicks=0,
            save_new_nclicks=0,
            replace_nclicks=0,
            close_nclicks=1,
            metric_id="metric_id",
            metric_name="",
            href="",
        )

        assert res == {
            "metric_name_input": None,
            "metric_save_dialog": False,
            "metric_save_dialog_info": "",
        }


@patch("mitzu.webapp.pages.explore.explore_page.ctx")
def test_metric_save_invalid_url(ctx, server: Flask):
    with server.test_request_context():
        ctx.triggered_id = EXP.METRIC_SAVE_DIALOG_SAVE_NEW_BUTTON

        res = EXP.handle_save_metric_dialog(
            navbar_btn_nclicks=0,
            save_new_nclicks=1,
            replace_nclicks=0,
            close_nclicks=0,
            metric_id="metric_id",
            metric_name="Test metric",
            href="",
        )

        assert res == {
            "metric_name_input": no_update,
            "metric_save_dialog_info": "Couldn't save metric. Something went wrong.",
            "metric_save_dialog": True,
        }


@patch("mitzu.webapp.pages.explore.explore_page.ctx")
def test_metric_save_valid(
    ctx,
    server: Flask,
    discovered_project: M.DiscoveredProject,
    dependencies: DEPS.Dependencies,
):
    with server.test_request_context():
        ctx.triggered_id = EXP.METRIC_SAVE_DIALOG_SAVE_NEW_BUTTON

        m = discovered_project.create_notebook_class_model()
        query = SE.to_compressed_string(m.page_visit.config(lookback_days="2 years"))
        query_params = quote(query)

        res = EXP.handle_save_metric_dialog(
            navbar_btn_nclicks=0,
            save_new_nclicks=1,
            replace_nclicks=0,
            close_nclicks=0,
            metric_id="metric_id",
            metric_name="Test metric",
            href=f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore?m={query_params}",
        )

        assert res == {
            "metric_name_input": no_update,
            "metric_save_dialog_info": "",
            "metric_save_dialog": False,
        }
        new_metric_id = dependencies.storage.list_saved_metrics()[0]
        sm = dependencies.storage.get_saved_metric(metric_id=new_metric_id)

        assert sm is not None
        assert sm.name == "Test metric"


@patch("mitzu.webapp.pages.explore.explore_page.ctx")
def test_metric_save_update_metric(
    ctx,
    server: Flask,
    discovered_project: M.DiscoveredProject,
    dependencies: DEPS.Dependencies,
):
    with server.test_request_context():
        ctx.triggered_id = EXP.METRIC_SAVE_DIALOG_SAVE_NEW_BUTTON

        m = discovered_project.create_notebook_class_model()
        query = SE.to_compressed_string(m.page_visit.config(lookback_days="2 years"))
        query_params = quote(query)

        res = EXP.handle_save_metric_dialog(
            navbar_btn_nclicks=0,
            save_new_nclicks=1,
            replace_nclicks=0,
            close_nclicks=0,
            metric_id="metric_id",
            metric_name="Test metric",
            href=f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore?m={query_params}",
        )

        ctx.triggered_id = EXP.METRIC_SAVE_DIALOG_REPLACE_BUTTON
        new_metric_id = dependencies.storage.list_saved_metrics()[0]
        sm = dependencies.storage.get_saved_metric(metric_id=new_metric_id)

        res = EXP.handle_save_metric_dialog(
            navbar_btn_nclicks=0,
            save_new_nclicks=1,
            replace_nclicks=0,
            close_nclicks=0,
            metric_id=new_metric_id,
            metric_name="Test metric 2",
            href=f"http://127.0.0.1:8082/projects/{discovered_project.project.id}/explore?m={query_params}",
        )

        assert res == {
            "metric_name_input": no_update,
            "metric_save_dialog_info": "",
            "metric_save_dialog": False,
        }

        sm = dependencies.storage.get_saved_metric(metric_id=new_metric_id)

        assert sm is not None
        assert sm.name == "Test metric 2"

        assert len(dependencies.storage.list_saved_metrics()) == 1
