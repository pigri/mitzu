import flask
from typing import Dict, Optional
from tests.helper import to_dict, find_component_by_id
import mitzu.webapp.pages.edit_user as U
import mitzu.webapp.dependencies as DEPS
import mitzu.webapp.storage as S
from tests.unit.webapp.fixtures import InMemoryCache
import mitzu.webapp.configs as configs
import mitzu.webapp.auth.authorizer as A
import mitzu.webapp.model as WM
import mitzu.webapp.service.user_service as US
import mitzu.webapp.service.events_service as ES
import mitzu.webapp.service.navbar_service as NB
import mitzu.webapp.service.secret_service as SS
import mitzu.webapp.service.notification_service as NS
import mitzu.webapp.pages.paths as P
from unittest.mock import MagicMock


class RequestContextLoggedInAsRootUser:
    def __init__(self, server: flask.Flask):
        self._server = server

    def __enter__(self):
        configs.AUTH_ROOT_USER_EMAIL = "root@local"
        cache = InMemoryCache()
        storage = S.MitzuStorage()
        storage.init_db_schema()
        user_service = US.UserService(storage)
        user_service.new_user(
            configs.AUTH_ROOT_USER_EMAIL,
            configs.AUTH_ROOT_PASSWORD,
            configs.AUTH_ROOT_PASSWORD,
            WM.Role.ADMIN,
        )

        auth_config = A.AuthConfig(
            oauth=None,
            token_validator=None,
            token_signing_key=configs.AUTH_JWT_SECRET,
            session_timeout=configs.AUTH_SESSION_TIMEOUT,
        )
        authorizer = A.OAuthAuthorizer.create(auth_config, user_service)

        @self._server.before_request
        def before_request() -> Optional[flask.Response]:
            request = flask.request
            return authorizer.authorize_request(request)

        @self._server.after_request
        def after_request(resp: flask.Response) -> flask.Response:
            request = flask.request
            return authorizer.refresh_auth_token(request, resp)

        root_user_id = user_service.get_user_by_email(configs.AUTH_ROOT_USER_EMAIL)
        token = authorizer._generate_new_token_with_claims(
            {"sub": root_user_id.id, A.JWT_CLAIM_ROLE: WM.Role.ADMIN.value}
        )

        event_service = ES.EventsService(storage)

        deps = DEPS.Dependencies(
            authorizer=authorizer,
            storage=storage,
            queue=None,
            cache=cache,
            user_service=user_service,
            events_service=event_service,
            navbar_service=NB.NavbarService(),
            secret_service=SS.SecretService(),
            notification_service=NS.DummyNotificationService(),
            tracking_service=MagicMock(),
            onboarding_service=MagicMock(),
        )

        self.context = self._server.test_request_context(
            headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"}
        )
        self._server.config[DEPS.CONFIG_KEY] = deps
        self.context.__enter__()
        return deps

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.context.__exit__(exc_type, exc_value, exc_tb)


def assert_input_field_with_value(index: str, expected_value: str, page: Dict):
    input = find_component_by_id(
        comp_id={"type": U.INDEX_TYPE, "index": index}, input=page
    )
    assert input is not None
    assert input["value"] == expected_value


def test_new_user_page_layout(server: flask.Flask):
    with RequestContextLoggedInAsRootUser(server):
        users_comp = U.layout(user_id="new")
        page = to_dict(users_comp)

        assert (
            find_component_by_id(comp_id=U.USER_CREATE_BUTTON, input=page) is not None
        )
        assert find_component_by_id(comp_id=U.USER_CLOSE_BUTTON, input=page) is not None

        assert_input_field_with_value(U.PROP_EMAIL, "", page)
        assert_input_field_with_value(U.PROP_PASSWORD, "", page)
        assert_input_field_with_value(U.PROP_CONFIRM_PASSWORD, "", page)


def test_delete_user(server: flask.Flask):
    with RequestContextLoggedInAsRootUser(server) as deps:
        user_service = deps.user_service
        res = U.create_new_user(
            0,
            email="a@b.c",
            role=WM.Role.MEMBER.value,
            all_inputs=["a@b.c", "member", "password", "password"],
        )
        assert res[U.SAVE_RESPONSE_CONTAINER] == "User created!"
        user = user_service.get_user_by_email_and_password("a@b.c", "password")
        assert user is not None
        assert user.role == WM.Role.MEMBER

        res = U.delete_user(0, P.create_path(P.USERS_HOME_PATH, user_id=user.id))
        deleted_user = user_service.get_user_by_email("a@b.c")
        assert deleted_user is None
        deps.tracking_service.register_new_user.assert_called_with("a@b.c", "member")


def test_change_password(server: flask.Flask):
    with RequestContextLoggedInAsRootUser(server) as deps:
        user_service = deps.user_service
        email = "test@local.com"
        old_password = "password"
        new_password = "new-password"

        user_id = user_service.new_user(email, old_password, old_password)
        res = U.update_password(
            0,
            password=new_password,
            confirm_password=new_password,
            pathname=P.create_path(P.USERS_HOME_PATH, user_id=user_id),
        )
        updated_user = user_service.get_user_by_email_and_password(email, new_password)

        assert res[U.CHANGE_PASSWORD_RESPONSE_CONTAINER] == "Password changed"
        assert updated_user is not None


def test_change_role(server: flask.Flask):
    with RequestContextLoggedInAsRootUser(server) as deps:
        user_service = deps.user_service
        email = "test@local.com"
        password = "password"

        user_id = user_service.new_user(email, password, password, role=WM.Role.MEMBER)
        res = U.update_role(
            0,
            role=WM.Role.ADMIN,
            pathname=P.create_path(P.USERS_HOME_PATH, user_id=user_id),
        )
        updated_user = user_service.get_user_by_id(user_id)

        assert res[U.CHANGE_ROLE_RESPONSE_CONTAINER] == "Role updated"
        assert updated_user is not None
        assert updated_user.role == WM.Role.ADMIN
