from __future__ import annotations

import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import flask
import werkzeug
import jwt
import requests
import base64
from typing import Any, Dict, List, Optional
from urllib import parse
from mitzu.helper import LOGGER
import mitzu.webapp.pages.paths as P
import mitzu.webapp.configs as configs
import mitzu.webapp.service.user_service as U
import mitzu.webapp.model as WM

JWT_ALGORITHM = "HS256"
JWT_CLAIM_ROLE = "rol"


@dataclass(frozen=True)
class OAuthConfig:
    """
    Contains the minimal configuration for an OAuth backend.

    :param client_id: Client ID, used for validing the JWT token claim and for fetching the identity token
    :param client_secret: Client secret, used for fetching the identity toke
    :param jwks_url: URL to fetch the JSON Web Key Set (JWKS)
    :param sign_in_url: URL to where the user is redirected at the beginning of the sign in flow
    :param sign_out_url: default is None, if set then the user is redirected to this URL at the nd of the sing out flow
    :param token_url: URL where the tokens can be fetched during the sign in flow
    :param jwt_algorithms: List of supported signing algorithms for the JWT tokens
    """

    client_id: str
    client_secret: str
    jwks_url: str
    sign_in_url: str
    sign_out_url: Optional[str]
    token_url: str
    jwt_algorithms: List[str]


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

    @staticmethod
    def create_from_oauth_config(oauth_config: OAuthConfig) -> JWTTokenValidator:
        return JWTTokenValidator(
            oauth_config.jwks_url,
            oauth_config.jwt_algorithms,
            oauth_config.client_id,
        )


@dataclass(frozen=True)
class AuthConfig:
    user_service: U.UserService

    token_cookie_name: str = "auth-token"
    redirect_cookie_name: str = "redirect-to"

    session_timeout: int = 7 * 24 * 60 * 60
    token_signing_key: str = configs.AUTH_JWT_SECRET

    oauth: Optional[OAuthConfig] = None
    token_validator: Optional[TokenValidator] = None


@dataclass(frozen=True)
class OAuthAuthorizer:
    _config: AuthConfig

    _authorized_url_prefixes: List[str] = field(
        default_factory=lambda: [
            P.UNAUTHORIZED_URL,
            P.HEALTHCHECK_PATH,
            "/assets/",
            "/_dash-update-component",
            "/_dash-component-suites/",
            "/_dash-layout",
            "/_dash-dependencies",
        ]
    )
    _ignore_token_refresh_prefixes: List[str] = field(
        default_factory=lambda: [
            P.UNAUTHORIZED_URL,
            P.SIGN_OUT_URL,
            P.HEALTHCHECK_PATH,
            "/assets/",
        ]
    )

    def get_home_url(self):
        url = flask.request.host_url
        return url[:-1] if url.endswith("/") else url

    @classmethod
    def create(cls, config: AuthConfig) -> OAuthAuthorizer:
        return OAuthAuthorizer(
            _config=config,
        )

    def _get_unauthenticated_response(
        self, redirect_url: Optional[str] = None
    ) -> werkzeug.wrappers.response.Response:
        resp = self._redirect(P.UNAUTHORIZED_URL)
        self.clear_cookie(resp, self._config.token_cookie_name)
        if redirect_url:
            url = parse.urlparse(redirect_url)

            if (
                url.path
                and not url.path.startswith("/assets/")
                and not url.path.startswith("/_dash")
                and not url.path.startswith("/_reload-hash")
                and not url.path.startswith("/auth")
                and not url.path.startswith(P.HEALTHCHECK_PATH)
            ):
                self.set_cookie(resp, self._config.redirect_cookie_name, redirect_url)
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
        if not self._config.oauth:
            raise ValueError("OAuth is not configured")
        message = bytes(
            f"{self._config.oauth.client_id}:{self._config.oauth.client_secret}",
            "utf-8",
        )
        secret_hash = base64.b64encode(message).decode()
        payload = {
            "grant_type": "authorization_code",
            "client_id": self._config.oauth.client_id,
            "code": auth_code,
            "redirect_uri": f"{self.get_home_url()}{P.OAUTH_CODE_URL}",
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {secret_hash}",
        }

        resp = requests.post(
            self._config.oauth.token_url, params=payload, headers=headers
        )

        if resp.status_code != 200:
            raise Exception(
                f"Unexpected response: {resp.status_code}, {resp.content.decode('utf-8')}"
            )

        return resp.json()["id_token"]

    def _generate_new_token_for_identity(
        self, identity: str, role: WM.Role = WM.Role.MEMBER
    ) -> str:
        now = int(time.time())
        claims = {
            "iat": now - 10,
            "exp": now + self._config.session_timeout,
            "iss": "mitzu",
            "sub": identity,
            JWT_CLAIM_ROLE: role.value,
        }
        return jwt.encode(
            claims, key=self._config.token_signing_key, algorithm=JWT_ALGORITHM
        )

    def _validate_token(self, token: str) -> Optional[Dict]:
        try:
            claims = jwt.decode(
                token, self._config.token_signing_key, algorithms=[JWT_ALGORITHM]
            )

            token_subject = claims["sub"]
            user = self._config.user_service.get_user_by_id(token_subject)
            if user is None:
                raise Exception("User not found")
            claims[JWT_CLAIM_ROLE] = user.role
            return claims
        except Exception as e:
            LOGGER.warning(f"Failed to validate token: {str(e)}")
            return None

    def _validate_foreign_token(self, token) -> Optional[str]:
        if not self._config.token_validator:
            raise ValueError("Token validator is not configured")
        try:
            decoded_token = self._config.token_validator.validate_token(token)
            if decoded_token is None:
                return None

            user_email = decoded_token.get("email")
            if user_email is None:
                LOGGER.warning("Email field is missing from the identity token")
                return None

            return user_email
        except Exception as e:
            LOGGER.warning(f"Failed to validate token: {str(e)}")
            return None

    def authorize_request(
        self, request: flask.Request
    ) -> Optional[werkzeug.wrappers.response.Response]:
        if self._config.oauth and request.path == P.REDIRECT_TO_LOGIN_URL:
            resp = self._redirect(self._config.oauth.sign_in_url)
            return resp

        if self._config.oauth and request.path == P.OAUTH_CODE_URL:
            code = self._get_oauth_code()
            if code is not None:
                LOGGER.debug(f"Redirected with code={code}")
                try:
                    id_token = self._get_identity_token(code)
                    redirect_url = flask.request.cookies.get(
                        self._config.redirect_cookie_name, request.host_url
                    )

                    user_email = self._validate_foreign_token(id_token)
                    if not user_email:
                        raise Exception("Unauthorized (Invalid jwt token)")

                    user = self._config.user_service.get_user_by_email(user_email)
                    if user is None:
                        raise Exception(
                            f"User tried to login without having a local user: {user_email}"
                        )
                    user_role = user.role
                    token_identity = user.id

                    token = self._generate_new_token_for_identity(
                        token_identity, role=user_role
                    )

                    resp = self._redirect(redirect_url)
                    self.set_cookie(resp, self._config.token_cookie_name, token)
                    self.clear_cookie(resp, self._config.redirect_cookie_name)
                    return resp
                except Exception as exc:
                    traceback.print_exception(type(exc), exc, exc.__traceback__)
                    LOGGER.warning(f"Failed to authenticate: {str(exc)}")
                    if self._config.oauth and self._config.oauth.sign_out_url:
                        resp = self._redirect(self._config.oauth.sign_out_url)
                        self.clear_cookie(resp, self._config.token_cookie_name)
                        return resp
                    return self._get_unauthenticated_response()

        auth_token = flask.request.cookies.get(self._config.token_cookie_name)

        if request.path == P.SIGN_OUT_URL:
            if self._config.oauth and self._config.oauth.sign_out_url:
                resp = self._redirect(self._config.oauth.sign_out_url)
                self.clear_cookie(resp, self._config.token_cookie_name)
                return resp
            return self._get_unauthenticated_response()

        for prefix in self._authorized_url_prefixes:
            if request.path.startswith(prefix):
                return None

        if auth_token and self._validate_token(auth_token) is not None:
            return None

        return self._get_unauthenticated_response(request.url)

    def refresh_auth_token(
        self, request: flask.Request, resp: flask.Response
    ) -> werkzeug.wrappers.response.Response:
        for prefix in self._ignore_token_refresh_prefixes:
            if request.path.startswith(prefix):
                return resp

        auth_token = flask.request.cookies.get(self._config.token_cookie_name)
        if auth_token is not None:
            identity = self._validate_token(auth_token)
            if identity is not None:
                new_token = self._generate_new_token_for_identity(
                    identity["sub"], role=identity[JWT_CLAIM_ROLE]
                )
                self.set_cookie(resp, self._config.token_cookie_name, new_token)
        return resp

    def is_request_authorized(self, request: flask.Request) -> bool:
        auth_token = request.cookies.get(self._config.token_cookie_name)
        return auth_token is not None and self._validate_token(auth_token) is not None

    def get_current_user_role(self, request: flask.Request) -> Optional[WM.Role]:
        auth_token = request.cookies.get(self._config.token_cookie_name)

        if auth_token is None:
            return None

        claims = self._validate_token(auth_token)
        if claims is None or JWT_CLAIM_ROLE not in claims.keys():
            return None

        return WM.Role(claims[JWT_CLAIM_ROLE])

    def login_local_user(
        self, email: str, password: str, response: Optional[flask.Response] = None
    ) -> Optional[str]:
        if self._config.oauth:
            raise ValueError("Password login is not enabled, need to use SSO")

        user = self._config.user_service.get_user_by_email_and_password(email, password)
        if user is None:
            return None

        token = self._generate_new_token_for_identity(user.id, role=user.role)

        if response:
            self.set_cookie(response, self._config.token_cookie_name, token)
            self.clear_cookie(response, self._config.redirect_cookie_name)

        return flask.request.cookies.get(
            self._config.redirect_cookie_name, self.get_home_url()
        )

    def get_current_user_id(self) -> Optional[str]:
        auth_token = flask.request.cookies.get(self._config.token_cookie_name)
        if auth_token is None:
            return None
        token_claims = self._validate_token(auth_token)
        if token_claims is None:
            return None
        return token_claims.get("sub")

    def set_cookie(
        self,
        response: werkzeug.wrappers.response.Response,
        cookie_name: str,
        value: str,
        **params,
    ):
        response.set_cookie(cookie_name, value, path="/", httponly=True, **params)

    def clear_cookie(
        self, response: werkzeug.wrappers.response.Response, cookie_name: str
    ):
        self.set_cookie(response, cookie_name, "", expires=0)
