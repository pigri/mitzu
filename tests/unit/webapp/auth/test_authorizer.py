import json
import flask
import pytest
from unittest.mock import patch, MagicMock
from requests.models import Response

from mitzu.webapp.auth.authorizer import (
    OAuthAuthorizer,
    OAuthConfig,
    OAUTH_CODE_URL,
    HOME_URL,
    REDIRECT_TO_LOGIN_URL,
    UNAUTHORIZED_URL,
    SIGN_OUT_URL,
)

from typing import Optional

app = flask.Flask(__name__)

auth_token = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ."
    "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

config = OAuthConfig(
    client_id="client_id",
    client_secret="secret",
    jwks_url="https://jwks_url/",
    sign_in_url="https://sign_in_ulr/",
    sign_out_url=None,
    token_url="https://token_url/",
    jwt_algorithms=["RS256"],
)
token_validator = MagicMock()
authorizer = OAuthAuthorizer.create(config, token_validator)
authorizer.setup_authorizer(app)


@pytest.fixture(autouse=True)
def before_and_after_test():
    token_validator.return_value = None
    token_validator.side_effect = None


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


def assert_redirected_to_unauthorized_page(
    resp: Optional[flask.Response], expected_redirect_cookie: Optional[str] = None
):
    assert resp is not None
    assert resp.status_code == 307
    assert resp.headers["Location"] == UNAUTHORIZED_URL
    assert_auth_token_removed(resp)

    redirect_cookie = authorizer._config.redirect_cookie_name
    if expected_redirect_cookie:
        assert (
            get_cookie_by_name(redirect_cookie, resp)
            == f"{redirect_cookie}={expected_redirect_cookie}; Path=/"
        )
    else:
        assert get_cookie_by_name(redirect_cookie, resp) is None


def assert_request_authorized(resp: Optional[flask.Response]):
    assert resp is None


def test_unauthorized_request_redirected_to_unauthorized_page():
    with app.test_request_context("/example_project?m=params"):
        resp = app.preprocess_request()
        assert_redirected_to_unauthorized_page(
            resp, expected_redirect_cookie='"/example_project?m=params"'
        )


def test_request_with_cached_token_is_allowed():
    token = "allowed-token"
    authorizer._tokens[token] = "dummy@user.com"
    with app.test_request_context(
        "/", headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"}
    ):
        resp = app.preprocess_request()
        assert_request_authorized(resp)


def test_redirects_to_sign_in_url():
    with app.test_request_context(REDIRECT_TO_LOGIN_URL):
        resp = app.preprocess_request()
        assert resp.status_code == 307
        assert resp.headers["Location"] == config.sign_in_url


def test_oauth_code_url_called_without_code():
    with app.test_request_context(OAUTH_CODE_URL):
        resp = app.preprocess_request()
        assert_request_authorized(resp)


@patch("requests.post")
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

    code = "1234567890"
    with app.test_request_context(f"{OAUTH_CODE_URL}?code={code}"):
        resp = app.preprocess_request()
        req_mock.assert_called_with(
            "https://token_url/",
            params={
                "grant_type": "authorization_code",
                "client_id": "client_id",
                "code": "1234567890",
                "redirect_uri": "http://localhost:8082/auth/oauth",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "Basic Y2xpZW50X2lkOnNlY3JldA==",
            },
        )
        assert resp is not None
        assert resp.status_code == 307
        assert resp.headers["Location"] == HOME_URL
        assert (
            get_cookie_by_name(authorizer._config.token_cookie_name, resp)
            == f"{authorizer._config.token_cookie_name}={auth_token}; Path=/"
        )


@patch("requests.post")
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
    redirect_after_login = "/example-project"

    code = "1234567890"
    with app.test_request_context(
        f"{OAUTH_CODE_URL}?code={code}",
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
                "redirect_uri": "http://localhost:8082/auth/oauth",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "Basic Y2xpZW50X2lkOnNlY3JldA==",
            },
        )
        assert resp is not None
        assert resp.status_code == 307
        assert resp.headers["Location"] == redirect_after_login
        assert (
            get_cookie_by_name(authorizer._config.token_cookie_name, resp)
            == f"{authorizer._config.token_cookie_name}={auth_token}; Path=/"
        )
        assert get_cookie_by_name(
            authorizer._config.redirect_cookie_name, resp
        ).startswith(f"{authorizer._config.redirect_cookie_name}=;")


@patch("requests.post")
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
    with app.test_request_context(f"{OAUTH_CODE_URL}?code={code}"):
        resp = app.preprocess_request()
        req_mock.assert_called_with(
            "https://token_url/",
            params={
                "grant_type": "authorization_code",
                "client_id": "client_id",
                "code": "1234567890",
                "redirect_uri": "http://localhost:8082/auth/oauth",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "Basic Y2xpZW50X2lkOnNlY3JldA==",
            },
        )
        assert_redirected_to_unauthorized_page(resp)


def test_authenticated_session_survives_mitzu_restart():
    token = "valid-token"
    token_validator.validate_token.return_value = {"email": "user@email.com"}
    with app.test_request_context(
        "/", headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"}
    ):
        resp = app.preprocess_request()
        token_validator.validate_token.assert_called_with(token)
        assert resp is None


def test_invalid_forged_tokens_are_rejected():
    token = "invalid-token"
    token_validator.validate_token.side_effect = Exception("token rejected")
    with app.test_request_context(
        "/", headers={"Cookie": f"{authorizer._config.token_cookie_name}={token}"}
    ):
        resp = app.preprocess_request()
        token_validator.validate_token.assert_called_with(token)
        assert_redirected_to_unauthorized_page(resp, expected_redirect_cookie="/")


def test_unauthorized_page_is_shown_for_unauthorized_requests():
    with app.test_request_context(UNAUTHORIZED_URL):
        resp = app.preprocess_request()
        assert resp.status_code == 200
        assert resp.data.decode("utf-8") == authorizer._unauthorized_page_content


def test_sign_out_without_sign_out_url():
    with app.test_request_context(SIGN_OUT_URL):
        resp = app.preprocess_request()
        assert_redirected_to_unauthorized_page(resp)


def test_sign_out_with_sign_out_url():
    app = flask.Flask(__name__)

    config = OAuthConfig(
        client_id="client_id",
        client_secret="secret",
        jwks_url="https://jwks_url/",
        sign_in_url="https://sign_in_ulr/",
        sign_out_url="https://sign_out_url/",
        token_url="https://token_url/",
        jwt_algorithms=["RS256"],
    )
    authorizer = OAuthAuthorizer.create(config)
    authorizer.setup_authorizer(app)

    with app.test_request_context(SIGN_OUT_URL):
        resp = app.preprocess_request()
        assert resp is not None
        assert resp.status_code == 307
        assert resp.headers["Location"] == config.sign_out_url
        assert_auth_token_removed(resp)


@patch("requests.post")
def test_rejects_not_allowed_email_domains_when_configured(req_mock):
    response = Response()
    response.code = "success"
    response.status_code = 200
    response._content = json.dumps(
        {
            "id_token": auth_token,
        }
    ).encode("utf-8")

    req_mock.return_value = response
    token_validator.validate_token.return_value = {"email": "user@email.com"}

    app = flask.Flask(__name__)
    authorizer = OAuthAuthorizer.create(
        oauth_config=config,
        token_validator=token_validator,
        allowed_email_domain="allowed.com",
    )
    authorizer.setup_authorizer(app)

    code = "1234567890"
    with app.test_request_context(f"{OAUTH_CODE_URL}?code={code}"):
        resp = app.preprocess_request()
        req_mock.assert_called_with(
            "https://token_url/",
            params={
                "grant_type": "authorization_code",
                "client_id": "client_id",
                "code": "1234567890",
                "redirect_uri": "http://localhost:8082/auth/oauth",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "Basic Y2xpZW50X2lkOnNlY3JldA==",
            },
        )
        assert_redirected_to_unauthorized_page(resp)