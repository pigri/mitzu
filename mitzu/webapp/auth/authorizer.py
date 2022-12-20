from __future__ import annotations

import os
from abc import ABC, abstractmethod
import flask
import werkzeug
import jwt
import requests
import base64
from typing import Any, Dict, Optional, List
from urllib import parse
from mitzu.helper import LOGGER


REDIRECT_TO_COOKIE = "redirect_to"
HOME_URL = os.getenv("HOME_URL", "http://localhost:8082")
MITZU_WEBAPP_URL = os.getenv("MITZU_WEBAPP_URL", HOME_URL)

SIGN_OUT_URL = "/auth/logout"
UNAUTHORIZED_URL = "/auth/unauthorized"
REDIRECT_TO_LOGIN_URL = "/auth/redirect-to-login"
OAUTH_CODE_URL = "/auth/oauth"


class OAuthConfig:
    @property
    @abstractmethod
    def client_id(self) -> str:
        pass

    @property
    @abstractmethod
    def client_secret(self) -> str:
        pass

    @property
    @abstractmethod
    def jwks_url(self) -> str:
        pass

    @property
    @abstractmethod
    def sign_in_url(self) -> str:
        pass

    @property
    @abstractmethod
    def token_url(self) -> str:
        pass

    @property
    @abstractmethod
    def jwt_algorithms(self) -> List[str]:
        pass


class TokenValidator(ABC):
    @abstractmethod
    def validate_token(self, token: str) -> Dict[str, Any]:
        pass


class JWTTokenValidator(TokenValidator):
    def __init__(self, jwks_url: str, algorithms: List[str], audience: str):
        self._jwks_client = jwt.PyJWKClient(jwks_url)
        self._algorithms = algorithms
        self._audience = audience

    def validate_token(self, token: str) -> Dict[str, Any]:
        signing_key = self._jwks_client.get_signing_key_from_jwt(token)

        return jwt.decode(
            token,
            signing_key.key,
            algorithms=self._algorithms,
            audience=self._audience,
        )


class OAuthAuthorizer(ABC):

    _oauth_config: OAuthConfig

    _unauthorized_url_prefixes = [
        "/auth/",
        "/assets/",
    ]
    _cookie_name = "auth-token"
    _tokens: Dict[str, Dict[str, str]]

    def __init__(
        self,
        oauth_config: OAuthConfig,
        token_validator: Optional[TokenValidator] = None,
    ):
        self._oauth_config = oauth_config
        self._tokens = {}

        if token_validator is not None:
            self._token_validator = token_validator
        else:
            self._token_validator = JWTTokenValidator(
                oauth_config.jwks_url,
                oauth_config.jwt_algorithms,
                oauth_config.client_id,
            )

        unauthorized_html_path = os.path.join(
            os.path.dirname(__file__), "../assets/unauthorized.html"
        )
        page_content = open(unauthorized_html_path, "r").read()
        self._unauthorized_page_content = page_content.replace(
            "LOGIN_URL", REDIRECT_TO_LOGIN_URL
        )

    def get_user_email(self, encoded_token: str) -> Optional[str]:
        val = self._tokens.get(encoded_token, {})
        if val is not None:
            return val.get("email")
        return None

    def _get_unauthenticated_response(self) -> werkzeug.wrappers.response.Response:
        resp = self._redirect(UNAUTHORIZED_URL)
        resp.set_cookie(self._cookie_name, "", expires=0)
        return resp

    def _redirect(self, location: str) -> werkzeug.wrappers.response.Response:
        resp = flask.redirect(code=307, location=location)
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        resp.headers["Cache-Control"] = "public, max-age=0"
        return resp

    def _get_oauth_code(self) -> Optional[str]:
        code = flask.request.values.get("code")
        if code is not None:
            return code
        parse_result = parse.urlparse(flask.request.url)
        params = parse.parse_qs(parse_result.query)
        code_ls = params.get("code")
        if code_ls is not None:
            return code_ls[0]
        return None

    def _get_identity_token(self, auth_code) -> str:
        message = bytes(
            f"{self._oauth_config.client_id}:{self._oauth_config.client_secret}",
            "utf-8",
        )
        secret_hash = base64.b64encode(message).decode()
        payload = {
            "grant_type": "authorization_code",
            "client_id": self._oauth_config.client_id,
            "code": auth_code,
            "redirect_uri": f"{HOME_URL}{OAUTH_CODE_URL}",
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {secret_hash}",
        }

        resp = requests.post(
            self._oauth_config.token_url, params=payload, headers=headers
        )
        if resp.status_code != 200:
            raise Exception(
                f"Unexpected response: {resp.status_code}, {resp.content.decode('utf-8')}"
            )

        return resp.json()["id_token"]

    def _validate_and_store_token(self, token) -> Optional[Dict[str, Any]]:
        try:
            decoded_token = self._token_validator.validate_token(token)
            if decoded_token is None:
                return None

            self._tokens[token] = decoded_token
            user_email = self.get_user_email(token)
            LOGGER.info(f"Identity token stored for user: {user_email}")

            return decoded_token
        except Exception as e:
            LOGGER.warning(f"Failed to validate token: {str(e)}")
            return None

    def setup_authorizer(self, server: flask.Flask):
        @server.before_request
        def authorize_request():
            request = flask.request

            if request.path == REDIRECT_TO_LOGIN_URL:
                resp = self._redirect(self._oauth_config.sign_in_url)
                return resp

            if request.path == OAUTH_CODE_URL:
                code = self._get_oauth_code()
                if code is not None:
                    LOGGER.debug(f"Redirected with code={code}")
                    try:
                        id_token = self._get_identity_token(code)
                        redirect_url = flask.request.cookies.get(
                            REDIRECT_TO_COOKIE, MITZU_WEBAPP_URL
                        )

                        if not self._validate_and_store_token(id_token):
                            raise Exception("Unauthorized (Invalid jwt token)")

                        resp = self._redirect(redirect_url)
                        resp.set_cookie(self._cookie_name, id_token)
                        resp.set_cookie(REDIRECT_TO_COOKIE, "", expires=0)
                        return resp
                    except Exception as e:
                        LOGGER.warning(f"Failed to authenticate: {str(e)}")
                        return self._get_unauthenticated_response()

            auth_token = flask.request.cookies.get(self._cookie_name)

            if request.path == SIGN_OUT_URL:
                if auth_token in self._tokens.keys():
                    self._tokens.pop(auth_token)
                return self._get_unauthenticated_response()

            if request.path == UNAUTHORIZED_URL:
                resp = flask.Response(self._unauthorized_page_content, 200)
                return resp

            for prefix in self._unauthorized_url_prefixes:
                if request.path.startswith(prefix):
                    return None

            if auth_token in self._tokens.keys():
                return None

            if auth_token and self._validate_and_store_token(auth_token):
                return None

            return self._get_unauthenticated_response()
