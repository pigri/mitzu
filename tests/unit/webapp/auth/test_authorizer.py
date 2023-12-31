import json
import flask
import pytest
import jwt
import time
from typing import Dict, Any
from unittest.mock import patch, MagicMock
from requests.models import Response
import mitzu.webapp.configs as configs
import mitzu.webapp.pages.paths as P
import mitzu.webapp.service.user_service as U
import mitzu.webapp.storage as S
from mitzu.webapp.auth.authorizer import (
    OAuthAuthorizer,
    OAuthConfig,
    AuthConfig,
    JWT_ALGORITHM,
    JWT_CLAIM_ROLE,
)
import mitzu.webapp.model as WM
import mitzu.webapp.configs as C
from typing import Optional

C.HOME_URL = "http://mitzu.io:8082"

app = flask.Flask(__name__)

auth_token = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ."
    "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

oauth_config = OAuthConfig(
    client_id="client_id",
    client_secret="secret",
    jwks_url="https://jwks_url/",
    sign_in_url="https://sign_in_ulr/",
    sign_out_url=None,
    token_url="https://token_url/",
    jwt_algorithms=["RS256"],
)
token_validator = MagicMock()
storage = S.MitzuStorage()
storage.init_db_schema()
user_service = U.UserService(storage)
auth_config = AuthConfig(
    token_signing_key="test",
    oauth=oauth_config,
    token_validator=token_validator,
)
authorizer = OAuthAuthorizer.create(auth_config, user_service)


def setup_authorizer(app: flask.Flask, authorizer: OAuthAuthorizer):
    @app.before_request
    def before_request() -> Optional[flask.Response]:
        request = flask.request
        return authorizer.authorize_request(request)

    @app.after_request
    def after_request(resp: flask.Response) -> flask.Response:
        request = flask.request
        return authorizer.refresh_auth_token(request, resp)


setup_authorizer(app, authorizer)


@pytest.fixture(autouse=True)
def before_and_after_test():
    token_validator.return_value = None
    token_validator.side_effect = None
    configs.AUTH_BACKEND = "local"

    for user in user_service.list_users():
        user_service.delete_user(user.id)


def get_cookie_by_name(cookie_name, resp: flask.Response) -> Optional[str]:
    for cookie in resp.headers.getlist("Set-Cookie"):
        if cookie.startswith(cookie_name):
            return cookie
    return None


def assert_auth_token_removed(resp: flask.Response):
    cookie_name = authorizer._config.token_cookie_name
    cookie = get_cookie_by_name(cookie_name, resp)
    assert cookie is not None
    assert cookie.startswith(f"{cookie_name}=; ")


def assert_auth_token(
    resp: flask.Response, identity: str, expected_custom_claims: Dict[str, Any] = {}
):
    cookie_name = authorizer._config.token_cookie_name
    cookie = get_cookie_by_name(cookie_name, resp)
    assert cookie is not None
    assert cookie.startswith(f"{cookie_name}=")
    assert cookie.endswith("; HttpOnly; Path=/")
    token = cookie.split(";")[0].replace(f"{cookie_name}=", "")
    decoded = jwt.decode(
        token, key=auth_config.token_signing_key, algorithms=[JWT_ALGORITHM]
    )
    assert decoded["sub"] == identity

    for key, value in expected_custom_claims.items():
        assert decoded[key] == value


def assert_redirected_to_unauthorized_page(
    resp: Optional[flask.Response], expected_redirect_cookie: Optional[str] = None
):
    assert resp is not None
    assert resp.status_code == 307
    assert resp.headers["Location"] == P.UNAUTHORIZED_URL
    assert_auth_token_removed(resp)

    redirect_cookie = authorizer._config.redirect_cookie_name
    if expected_redirect_cookie:
        assert (
            get_cookie_by_name(redirect_cookie, resp)
            == f"{redirect_cookie}={expected_redirect_cookie}; HttpOnly; Path=/"
        )
    else:
        assert get_cookie_by_name(redirect_cookie, resp) is None


def assert_request_authorized(resp: Optional[flask.Response]):
    assert resp is None


def test_unauthorized_request_redirected_to_unauthorized_page():
    with app.test_request_context("/example_project?m=params"):
        resp = app.preprocess_request()
        assert_redirected_to_unauthorized_page(
            resp,
            expected_redirect_cookie="http://mitzu.io:8082/example_project?m=params",
        )


def test_request_with_previously_signed_token_is_accepted():
    now = int(time.time())
    user_id = user_service.new_user("email@b.c", "password", "password")
    claims = {
        "iat": now - 10,
        "exp": now + 10,
        "iss": "mitzu",
        "sub": user_id,
    }
    token = jwt.encode(
        claims, key=auth_config.token_signing_key, algorithm=JWT_ALGORITHM
    )

    with app.test_request_context(
        "/", headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"}
    ):
        resp = app.preprocess_request()
        assert_request_authorized(resp)


def test_redirects_to_sign_in_url():
    with app.test_request_context(P.REDIRECT_TO_LOGIN_URL):
        resp = app.preprocess_request()
        assert resp.status_code == 307
        assert resp.headers["Location"] == oauth_config.sign_in_url


@patch("mitzu.webapp.auth.authorizer.requests.post")
def test_oauth_code_url_called_with_valid_code(req_mock):
    response = Response()
    response.code = "success"
    response.status_code = 200
    response._content = json.dumps(
        {
            "id_token": auth_token,
        }
    ).encode("utf-8")

    req_mock.return_value = response
    email = "a@b.c"
    user_id = user_service.new_user(email, "password", "password")
    token_validator.validate_token.return_value = {"email": email}

    code = "1234567890"
    with app.test_request_context(f"{P.OAUTH_CODE_URL}?code={code}"):
        resp = app.preprocess_request()
        req_mock.assert_called_with(
            "https://token_url/",
            params={
                "grant_type": "authorization_code",
                "client_id": "client_id",
                "code": "1234567890",
                "redirect_uri": "http://mitzu.io:8082/auth/oauth",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "Basic Y2xpZW50X2lkOnNlY3JldA==",
            },
        )
        assert resp is not None
        assert resp.status_code == 307
        assert resp.headers["Location"] == "http://mitzu.io:8082"
        assert_auth_token(resp, user_id)


@patch("mitzu.webapp.auth.authorizer.requests.post")
def test_oauth_code_url_called_with_valid_code_and_redirection_cookie(req_mock):
    response = Response()
    response.code = "success"
    response.status_code = 200
    response._content = json.dumps(
        {
            "id_token": auth_token,
        }
    ).encode("utf-8")

    req_mock.return_value = response
    email = "a@b.c"
    user_id = user_service.new_user(email, "password", "password")
    token_validator.validate_token.return_value = {"email": email}
    redirect_after_login = "/example-project"

    code = "1234567890"
    with app.test_request_context(
        f"{P.OAUTH_CODE_URL}?code={code}",
        headers={
            "Cookie": f"{authorizer._config.redirect_cookie_name}={redirect_after_login}"
        },
    ):
        resp = app.preprocess_request()
        req_mock.assert_called_with(
            "https://token_url/",
            params={
                "grant_type": "authorization_code",
                "client_id": "client_id",
                "code": "1234567890",
                "redirect_uri": "http://mitzu.io:8082/auth/oauth",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "Basic Y2xpZW50X2lkOnNlY3JldA==",
            },
        )
        assert resp is not None
        assert resp.status_code == 307
        assert resp.headers["Location"] == redirect_after_login
        assert_auth_token(resp, user_id)
        assert get_cookie_by_name(
            authorizer._config.redirect_cookie_name, resp
        ).startswith(f"{authorizer._config.redirect_cookie_name}=;")


@patch("mitzu.webapp.auth.authorizer.requests.post")
def test_oauth_code_url_called_with_invalid_code(req_mock):
    response = Response()
    response.code = "success"
    response.status_code = 200
    response._content = json.dumps(
        {
            "id_token": auth_token,
        }
    ).encode("utf-8")

    req_mock.return_value = response
    token_validator.validate_token.return_value = None

    code = "1234567890"
    with app.test_request_context(f"{P.OAUTH_CODE_URL}?code={code}"):
        resp = app.preprocess_request()
        req_mock.assert_called_with(
            "https://token_url/",
            params={
                "grant_type": "authorization_code",
                "client_id": "client_id",
                "code": "1234567890",
                "redirect_uri": "http://mitzu.io:8082/auth/oauth",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "Basic Y2xpZW50X2lkOnNlY3JldA==",
            },
        )
        assert_redirected_to_unauthorized_page(resp)


def test_invalid_forged_tokens_are_rejected():
    token = "invalid-token"
    with app.test_request_context(
        "/", headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"}
    ):
        resp = app.preprocess_request()
        assert_redirected_to_unauthorized_page(
            resp, expected_redirect_cookie="http://mitzu.io:8082/"
        )


def test_sign_out_without_sign_out_url():
    with app.test_request_context(P.SIGN_OUT_URL):
        resp = app.preprocess_request()
        assert_redirected_to_unauthorized_page(resp)


def test_sign_out_with_sign_out_url():
    app = flask.Flask(__name__)

    oauth_config = OAuthConfig(
        client_id="client_id",
        client_secret="secret",
        jwks_url="https://jwks_url/",
        sign_in_url="https://sign_in_ulr/",
        sign_out_url="https://sign_out_url/",
        token_url="https://token_url/",
        jwt_algorithms=["RS256"],
    )
    authorizer = OAuthAuthorizer.create(
        AuthConfig(
            token_signing_key=auth_config.token_signing_key,
            token_validator=token_validator,
            oauth=oauth_config,
        ),
        user_service=user_service,
    )
    setup_authorizer(app, authorizer)

    with app.test_request_context(P.SIGN_OUT_URL):
        resp = app.preprocess_request()
        assert resp is not None
        assert resp.status_code == 307
        assert resp.headers["Location"] == oauth_config.sign_out_url
        assert_auth_token_removed(resp)


def test_healthcheck_request():
    with app.test_request_context(P.HEALTHCHECK_PATH):
        resp = app.preprocess_request()
        assert resp is None


@patch("mitzu.webapp.auth.authorizer.requests.post")
def test_rejects_sso_logins_when_user_is_missing_from_the_local_users(req_mock):
    response = Response()
    response.code = "success"
    response.status_code = 200
    response._content = json.dumps(
        {
            "id_token": auth_token,
        }
    ).encode("utf-8")

    req_mock.return_value = response
    email = "user@email.com"
    token_validator.validate_token.return_value = {"email": email}

    storage = S.MitzuStorage()
    storage.init_db_schema()
    user_service = U.UserService(storage)
    app = flask.Flask(__name__)
    authorizer = OAuthAuthorizer.create(
        AuthConfig(
            token_signing_key=auth_config.token_signing_key,
            token_validator=token_validator,
            oauth=oauth_config,
        ),
        user_service=user_service,
    )
    setup_authorizer(app, authorizer)
    configs.AUTH_BACKEND = "cognito"

    code = "1234567890"
    with app.test_request_context(f"{P.OAUTH_CODE_URL}?code={code}"):
        resp = app.preprocess_request()
        req_mock.assert_called_with(
            "https://token_url/",
            params={
                "grant_type": "authorization_code",
                "client_id": "client_id",
                "code": "1234567890",
                "redirect_uri": "http://mitzu.io:8082/auth/oauth",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "Basic Y2xpZW50X2lkOnNlY3JldA==",
            },
        )
        assert_redirected_to_unauthorized_page(resp)

    user_id = user_service.new_user(email, "password", "password")
    with app.test_request_context(f"{P.OAUTH_CODE_URL}?code={code}"):
        resp = app.preprocess_request()
        req_mock.assert_called_with(
            "https://token_url/",
            params={
                "grant_type": "authorization_code",
                "client_id": "client_id",
                "code": "1234567890",
                "redirect_uri": "http://mitzu.io:8082/auth/oauth",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "Basic Y2xpZW50X2lkOnNlY3JldA==",
            },
        )
        assert resp is not None
        assert resp.status_code == 307
        assert resp.headers["Location"] == "http://mitzu.io:8082"
        assert_auth_token(resp, user_id)


def test_token_is_not_refreshed_for_assets_and_some_auth_urls():
    now = int(time.time())
    user_id = user_service.new_user("email@b.c", "password", "password")
    claims = {
        "iat": now - 10,
        "exp": now + 10,
        "iss": "mitzu",
        "sub": user_id,
    }
    token = jwt.encode(
        claims, key=auth_config.token_signing_key, algorithm=JWT_ALGORITHM
    )

    with app.test_request_context(
        "/assets/test.png",
        headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"},
    ):
        resp = flask.make_response("ok", 200)
        resp = app.process_response(resp)
        assert len(resp.headers.getlist("Set-Cookie")) == 0

    with app.test_request_context(
        P.SIGN_OUT_URL,
        headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"},
    ):
        resp = flask.make_response("ok", 200)
        resp = app.process_response(resp)
        assert len(resp.headers.getlist("Set-Cookie")) == 0

    with app.test_request_context(
        P.UNAUTHORIZED_URL,
        headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"},
    ):
        resp = flask.make_response("ok", 200)
        resp = app.process_response(resp)
        assert len(resp.headers.getlist("Set-Cookie")) == 0


def test_token_is_refreshed_for_callbacks():
    now = int(time.time())
    user_id = user_service.new_user("email@b.c", "password", "password")
    claims = {
        "iat": now - authorizer._config.token_refresh_threshold - 10,
        "exp": now + 10,
        "iss": "mitzu",
        "sub": user_id,
        "xxx": "custom claim",
    }
    token = jwt.encode(
        claims, key=auth_config.token_signing_key, algorithm=JWT_ALGORITHM
    )

    with app.test_request_context(
        "/", headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"}
    ):
        resp = flask.make_response("ok", 200)
        resp = app.process_response(resp)
        assert_auth_token(resp, user_id, expected_custom_claims={"xxx": "custom claim"})


def test_token_is_not_refreshed_for_callbacks_if_the_token_is_quite_new():
    now = int(time.time())
    user_id = user_service.new_user("email@b.c", "password", "password")
    claims = {
        "iat": now - 10,
        "exp": now + 10,
        "iss": "mitzu",
        "sub": user_id,
        "xxx": "custom claim",
    }
    token = jwt.encode(
        claims, key=auth_config.token_signing_key, algorithm=JWT_ALGORITHM
    )

    with app.test_request_context(
        "/", headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"}
    ):
        resp = flask.make_response("ok", 200)
        resp = app.process_response(resp)
        assert get_cookie_by_name(authorizer._config.token_cookie_name, resp) is None


def test_unauthorized_when_user_is_deleted():
    email = "a@b.c"
    password = "password"
    storage = S.MitzuStorage()
    storage.init_db_schema()
    user_service = U.UserService(storage)
    user_id = user_service.new_user(email, password, password)
    app = flask.Flask(__name__)
    authorizer = OAuthAuthorizer.create(
        AuthConfig(
            token_signing_key=auth_config.token_signing_key,
            token_validator=token_validator,
            oauth=oauth_config,
        ),
        user_service=user_service,
    )
    setup_authorizer(app, authorizer)

    token = authorizer._generate_new_token_with_claims(
        {"sub": user_id, JWT_CLAIM_ROLE: WM.Role.MEMBER.value}
    )

    with app.test_request_context(
        "/",
        headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"},
    ):
        resp = app.preprocess_request()
        assert_request_authorized(resp)

    user_service.delete_user(user_id)
    with app.test_request_context(
        "/",
        headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"},
    ):
        resp = app.preprocess_request()
        assert_redirected_to_unauthorized_page(
            resp, expected_redirect_cookie="http://mitzu.io:8082/"
        )


def test_login_local_user_unauthorized():
    auth_config = AuthConfig(
        token_signing_key="test",
        oauth=None,
        token_validator=None,
    )
    authorizer = OAuthAuthorizer.create(auth_config, user_service)
    with app.test_request_context():
        # None means unauthorized
        assert authorizer.login_local_user("a@b.c", "password") is None


def test_login_local_user_authorized():
    auth_config = AuthConfig(
        token_signing_key="test",
        oauth=None,
        token_validator=None,
    )
    authorizer = OAuthAuthorizer.create(auth_config, user_service)
    email = "a@b.c"
    password = "password"
    user_service.new_user(email, password, password)
    with app.test_request_context():
        assert authorizer.login_local_user(email, password) == "http://mitzu.io:8082"


def test_login_local_user_authorized_and_redirected_based_on_a_cookie():
    auth_config = AuthConfig(
        token_signing_key="test",
        oauth=None,
        token_validator=None,
    )
    authorizer = OAuthAuthorizer.create(auth_config, user_service)
    email = "a@b.c"
    password = "password"
    user_service.new_user(email, password, password)
    redirect_to = "/dashboards"
    with app.test_request_context(
        "/",
        headers={"Cookie": f"{authorizer._config.redirect_cookie_name}={redirect_to}"},
    ):
        assert authorizer.login_local_user(email, password) == redirect_to
