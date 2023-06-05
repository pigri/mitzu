from __future__ import annotations

import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
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
    token_cookie_name: str = "auth-token"
    redirect_cookie_name: str = "redirect-to"

    session_timeout: int = 7 * 24 * 60 * 60
    token_refresh_threshold: int = 60
    token_signing_key: str = configs.AUTH_JWT_SECRET

    oauth: Optional[OAuthConfig] = None
    token_validator: Optional[TokenValidator] = None


@dataclass(frozen=True)
class Authorizer:
    _config: AuthConfig

    @property
    def _authorized_url_prefixes(self) -> List[str]:
        return [
            P.UNAUTHORIZED_URL,
            P.HEALTHCHECK_PATH,
            "/assets/",
            "/_dash-update-component",
            "/_dash-component-suites/",
            "/_dash-layout",
            "/_dash-dependencies",
        ]

    @property
    def _ignore_token_refresh_prefixes(self) -> List[str]:
        return [
            P.UNAUTHORIZED_URL,
            P.SIGN_OUT_URL,
            P.HEALTHCHECK_PATH,
            "/assets/",
            "/_dash-component-suites/",
        ]

    def get_home_url(self) -> str:
        home_url = flask.request.url_root
        if configs.HOME_URL:
            home_url = configs.HOME_URL
        # trailing slashes can cause issues with SSO login
        return home_url.strip("/")

    def get_fixed_request_url(self) -> str:
        domain = self.get_home_url()
        path = flask.request.path
        query_string = flask.request.query_string.decode("utf-8")
        if query_string:
            query_string = f"?{query_string}"

        url = f"{domain}{path}{query_string}"
        return url

    def get_auth_token(self):
        return flask.request.cookies.get(self._config.token_cookie_name)

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
        parse_result = parse.urlparse(self.get_fixed_request_url())
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

    def _generate_new_token_with_claims(
        self,
        claims: Dict[str, Any],
    ) -> str:
        now = int(time.time())
        claims["iat"] = now - 10
        claims["exp"] = now + self._config.session_timeout
        claims["iss"] = "mitzu"
        return jwt.encode(
            claims, key=self._config.token_signing_key, algorithm=JWT_ALGORITHM
        )

    def _validate_token(self, token: str) -> Optional[Dict]:
        try:
            claims = jwt.decode(
                token, self._config.token_signing_key, algorithms=[JWT_ALGORITHM]
            )

            user_id = claims["sub"]
            expected_claims = self._get_token_claims_for_user_id(user_id)
            # apply the recent user role changes in the current auth session
            claims[JWT_CLAIM_ROLE] = expected_claims[JWT_CLAIM_ROLE]
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

    def _get_token_claims_for_email(self, user_email: str) -> Dict[str, Any]:
        raise NotImplementedError()

    def _get_token_claims_for_user_id(self, user_id: str) -> Dict[str, Any]:
        raise NotImplementedError()

    def _get_token_claims_for_email_and_password(
        self, email: str, password: str
    ) -> Dict[str, Any]:
        raise NotImplementedError()

    def authorize_request(
        self, request: flask.Request
    ) -> Optional[werkzeug.wrappers.response.Response]:

        if request.path == P.REDIRECT_TO_LOGIN_URL:
            if self._config.oauth:
                resp = self._redirect(self._config.oauth.sign_in_url)
                return resp
            else:
                resp = self._redirect(P.REDIRECT_TO_LOGIN_URL)

        if request.path == P.SIGN_OUT_URL:
            if self._config.oauth and self._config.oauth.sign_out_url:
                resp = self._redirect(self._config.oauth.sign_out_url)
                self.clear_cookie(resp, self._config.token_cookie_name)
                return resp
            else:
                return self._get_unauthenticated_response()

        for prefix in self._authorized_url_prefixes:
            if request.path.startswith(prefix):
                return None

        if self._config.oauth and request.path == P.OAUTH_CODE_URL:
            code = self._get_oauth_code()
            if code is not None:
                LOGGER.debug(f"Redirected with code={code}")
                try:
                    id_token = self._get_identity_token(code)
                    redirect_url = flask.request.cookies.get(
                        self._config.redirect_cookie_name, self.get_home_url()
                    )

                    user_email = self._validate_foreign_token(id_token)
                    if not user_email:
                        raise Exception("Unauthorized (Invalid jwt token)")

                    token_claims = self._get_token_claims_for_email(user_email)
                    token = self._generate_new_token_with_claims(token_claims)

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

        auth_token = self.get_auth_token()
        if auth_token:
            return self._authorize_request_with_token(request, auth_token)

        return self._get_unauthenticated_response(self.get_fixed_request_url())

    def _authorize_request_with_token(
        self, request: flask.Request, auth_token: str
    ) -> Optional[werkzeug.wrappers.response.Response]:
        if self._validate_token(auth_token) is not None:
            return None
        return self._get_unauthenticated_response(self.get_fixed_request_url())

    def refresh_auth_token(
        self, request: flask.Request, resp: flask.Response
    ) -> werkzeug.wrappers.response.Response:
        for prefix in self._ignore_token_refresh_prefixes:
            if request.path.startswith(prefix):
                return resp

        auth_token = self.get_auth_token()
        if auth_token is not None:
            token_claims = self._validate_token(auth_token)
            if token_claims is not None:

                if (
                    token_claims["iat"]
                    >= int(time.time()) - self._config.token_refresh_threshold
                ):
                    return resp

                new_token = self._generate_new_token_with_claims(token_claims)
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

        try:
            token_claims = self._get_token_claims_for_email_and_password(
                email, password
            )
        except Exception as e:
            LOGGER.warning(e)
            return None
        token = self._generate_new_token_with_claims(token_claims)

        if response:
            self.set_cookie(response, self._config.token_cookie_name, token)
            self.clear_cookie(response, self._config.redirect_cookie_name)

        return flask.request.cookies.get(
            self._config.redirect_cookie_name, self.get_home_url()
        )

    def get_current_user_id(self) -> Optional[str]:
        auth_token = self.get_auth_token()
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


@dataclass(frozen=True)
class OAuthAuthorizer(Authorizer):
    _user_service: U.UserService

    @classmethod
    def create(cls, config: AuthConfig, user_service: U.UserService) -> OAuthAuthorizer:
        return OAuthAuthorizer(
            _config=config,
            _user_service=user_service,
        )

    def _get_token_claims_for_email(self, user_email: str) -> Dict[str, Any]:
        user = self._user_service.get_user_by_email(user_email)
        if user is None:
            raise Exception(f"Local user not found with email: {user_email}")

        return {"sub": user.id, JWT_CLAIM_ROLE: user.role.value}

    def _get_token_claims_for_user_id(self, user_id: str) -> Dict[str, Any]:
        user = self._user_service.get_user_by_id(user_id)
        if user is None:
            raise Exception(f"Local user not found with id: {user_id}")

        return {"sub": user.id, JWT_CLAIM_ROLE: user.role.value}

    def _get_token_claims_for_email_and_password(
        self, email: str, password: str
    ) -> Dict[str, Any]:
        user = self._user_service.get_user_by_email_and_password(email, password)
        if user is None:
            raise Exception("Bad credentials")

        return {"sub": user.id, JWT_CLAIM_ROLE: user.role.value}
