import mitzu.webapp.service.navbar_service as NB
import flask
from dash import html
from tests.unit.webapp.pages.test_edit_users_page import (
    RequestContextLoggedInAsRootUser,
)


def test_navbar_service_registers_the_default_items(server: flask.Flask):
    service = NB.NavbarService()
    root_id = "root_id"

    with RequestContextLoggedInAsRootUser(server):
        navbar = service.get_navbar_component(root_id)
        left_nav_bar_items = navbar.children.children[0].children
        assert left_nav_bar_items[0].children.id == {
            "type": NB.OFF_CANVAS_TOGGLER,
            "index": root_id,
        }
        assert left_nav_bar_items[1].children.id == {
            "type": NB.EXPLORE_DROPDOWN,
            "index": root_id,
        }
        assert len(left_nav_bar_items) == 2

        right_nav_bar_items = navbar.children.children[1].children
        assert right_nav_bar_items[0].children.id == {
            "type": NB.SIGNED_IN_AS_DIV,
            "index": root_id,
        }
        assert len(right_nav_bar_items) == 1


def test_navbar_service_adds_new_items_with_orders(server: flask.Flask):
    service = NB.NavbarService()
    root_id = "root_id"

    def first(id: str, **kwargs):
        return html.Div(
            "first",
            id={"type": "first", "index": id},
        )

    def last(id: str, **kwargs):
        return html.Div(
            "last",
            id={"type": "last", "index": id},
        )

    service.register_navbar_item_provider("left", first, priority=5)
    service.register_navbar_item_provider("left", last, priority=100)

    service.register_navbar_item_provider("right", first, priority=10)
    service.register_navbar_item_provider("right", last, priority=100)

    with RequestContextLoggedInAsRootUser(server):
        navbar = service.get_navbar_component(root_id)
        left_nav_bar_items = navbar.children.children[0].children
        assert left_nav_bar_items[0].children.id == {
            "type": NB.OFF_CANVAS_TOGGLER,
            "index": root_id,
        }
        assert left_nav_bar_items[1].children.id == {"type": "first", "index": root_id}
        assert left_nav_bar_items[2].children.id == {
            "type": NB.EXPLORE_DROPDOWN,
            "index": root_id,
        }
        assert left_nav_bar_items[3].children.id == {"type": "last", "index": root_id}
        assert len(left_nav_bar_items) == 4

        right_nav_bar_items = navbar.children.children[1].children
        assert right_nav_bar_items[0].children.id == {"type": "first", "index": root_id}
        assert right_nav_bar_items[1].children.id == {
            "type": NB.SIGNED_IN_AS_DIV,
            "index": root_id,
        }
        assert right_nav_bar_items[2].children.id == {"type": "last", "index": root_id}
        assert len(right_nav_bar_items) == 3
