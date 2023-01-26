from flask import Flask
from tests.helper import to_dict, find_component_by_id
import mitzu.webapp.pages.admin_page as ADM
from unittest.mock import patch
import base64
import jsonpickle
import mitzu.model as M

from mitzu.webapp.dependencies import Dependencies


def test_admin_page(server: Flask):
    with server.test_request_context():

        connections_comp = ADM.layout()
        page = to_dict(connections_comp)

        container = find_component_by_id(comp_id=ADM.TBL_BODY_CONTAINER, input=page)

        assert container is not None
        assert len(container["children"]) > 5


@patch("mitzu.webapp.pages.admin_page.ctx")
def test_download_and_reset_flow(ctx, server: Flask, dependencies: Dependencies):
    with server.test_request_context():
        keys = dependencies.cache.list_keys()

        # Downloading Storage
        res = ADM.download_button_clicked(1)
        base64_bytes = base64.b64encode(res["content"].encode("ascii")).decode()

        # Deleting 2 keys
        ctx.triggered_id = {"index": keys[0], "type": ADM.DELETE_BUTTON_TYPE}
        ADM.update_tbl_body(1, None)

        ctx.triggered_id = {"index": keys[1], "type": ADM.DELETE_BUTTON_TYPE}
        tbl_body, error_message = ADM.update_tbl_body(1, None)

        assert len(tbl_body) == len(keys) - 2
        assert error_message == ""

        # Reseting Storage from downloaded
        ctx.triggered_id = ADM.STORAGE_RESET
        tbl_body, error_message = ADM.update_tbl_body(None, f",{base64_bytes}")

        assert len(tbl_body) == len(keys)
        assert error_message == ""


def test_json_pickle():
    p = jsonpickle.Pickler()

    res = p.flatten(M.State("Test"))

    assert res == {}
