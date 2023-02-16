import flask
from tests.helper import to_dict, find_component_by_id
import mitzu.webapp.pages.users as U
from tests.unit.webapp.pages.test_edit_users_page import (
    RequestContextLoggedInAsRootUser,
)


def test_users_list_page_layout(server: flask.Flask):
    with RequestContextLoggedInAsRootUser(server):
        users_comp = U.layout()
        page = to_dict(users_comp)

        # FIXME: assert on listed users
        add_user_button = find_component_by_id(comp_id=U.ADD_USER_BUTTON, input=page)

        assert add_user_button is not None
