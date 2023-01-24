from flask import Flask
from tests.helper import to_dict, find_component_by_id
import mitzu.webapp.pages.connections_page as con


def test_connections_page(server: Flask):
    with server.test_request_context():

        connections_comp = con.layout()
        comp_dict = to_dict(connections_comp)

        container = find_component_by_id(
            comp_id=con.CONNECTIONS_CONTAINER, input=comp_dict
        )
        title = find_component_by_id(comp_id=con.CONNECTION_TITLE, input=comp_dict)

        # By default we have 1 single connection
        assert container is not None
        assert len(container["children"]) == 1

        # The name of the default project is sample connection
        assert title is not None
        assert title["children"] == "Sample Connection"
