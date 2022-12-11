from __future__ import annotations

import base64
import os
from copy import copy
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib import parse
import flask
import jwt
import requests
from mitzu.helper import LOGGER
from mitzu.webapp.auth.authorizer import (
    NOT_FOUND_URL,
    HOME_URL,
    REDIRECT_TO_COOKIE,
    SIGN_OUT_URL,
    MITZU_WEBAPP_URL,
    MitzuAuthorizer,
)


@dataclass(frozen=True)
class CognitoConfig:
    _client_id: str
    _client_secret: str
    _domain: str
    _region: str
    _pool_id: str
    _redirect_url: str
    _jwt_algo: List[str]
    _cookie_name: str

    def __init__(
        self,
        pool_id: Optional[str] = None,
        region: Optional[str] = None,
        domain: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_url: Optional[str] = None,
        jwt_algo: List[str] = ["RS256"],
    ):
        object.__setattr__(
            self,
            "_pool_id",
            self.fallback_to_env_var(pool_id, "COGNITO_POOL_ID"),
        )
        object.__setattr__(
            self,
            "_region",
            self.fallback_to_env_var(region, "COGNITO_REGION"),
        )
        object.__setattr__(
            self,
            "_domain",
            self.fallback_to_env_var(domain, "COGNITO_DOMAIN"),
        )
        object.__setattr__(
            self, "_client_id", self.fallback_to_env_var(client_id, "COGNITO_CLIENT_ID")
        )
        object.__setattr__(
            self,
            "_client_secret",
            self.fallback_to_env_var(client_secret, "COGNITO_CLIENT_SECRET"),
        )
        object.__setattr__(
            self,
            "_redirect_url",
            self.fallback_to_env_var(redirect_url, "COGNITO_REDIRECT_URL"),
        )
        object.__setattr__(
            self,
            "_jwt_algo",
            self.fallback_to_env_var(
                ",".join(jwt_algo), "COGNITO_JWT_ALGORITHMS"
            ).split(","),
        )
        object.__setattr__(self, "_cookie_name", "access_token")

    @classmethod
    def fallback_to_env_var(cls, value: Optional[str], env_var: str):
        if value is not None:
            return value
        return os.getenv(env_var)

    @property
    def _sign_in_url(self) -> str:
        return (
            f"https://{self._domain}/oauth2/authorize?"
            f"client_id={self._client_id}&"
            "response_type=code&"
            "scope=email+openid&"
            f"redirect_uri={self._redirect_url}"
        )

    @property
    def _sign_out_url(self) -> str:
        return (
            f"https://{self._domain}/logout?"
            f"client_id={self._client_id}&"
            "response_type=code&"
            "scope=email+openid&"
            f"redirect_uri={self._redirect_url}"
        )

    @property
    def _token_url(self) -> str:
        return f"https://{self._domain}/oauth2/token"

    @property
    def _jwks_url(self) -> str:
        return f"https://cognito-idp.{self._region}.amazonaws.com/{self._pool_id}/.well-known/jwks.json"


@dataclass
class CognitoAuthorizer(MitzuAuthorizer):
    _config: CognitoConfig = field(default_factory=lambda: CognitoConfig())
    tokens: Dict[str, Dict[str, str]] = field(default_factory=lambda: copy({}))

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
            f"{self._config._client_id}:{self._config._client_secret}", "utf-8"
        )
        secret_hash = base64.b64encode(message).decode()
        payload = {
            "grant_type": "authorization_code",
            "client_id": self._config._client_id,
            "code": auth_code,
            "redirect_uri": self._config._redirect_url,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {secret_hash}",
        }

        resp = requests.post(self._config._token_url, params=payload, headers=headers)
        if resp.status_code != 200:
            raise Exception(
                f"Unexpected response: {resp.status_code}, {resp.content.decode('utf-8')}"
            )

        return resp.json()["id_token"]

    def _get_unauthenticated_response(self) -> flask.Response:
        resp = flask.redirect(code=307, location=NOT_FOUND_URL)
        resp.set_cookie(self._config._cookie_name, "", expires=0)
        resp = self.add_no_cache_headers(resp)
        return resp

    def add_no_cache_headers(self, resp) -> flask.Response:
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        resp.headers["Cache-Control"] = "public, max-age=0"
        return resp

    def setup_authorizer(self, server: flask.Flask):
        @server.before_request
        def authorize_request():
            resp: flask.Response

            # Not found URL
            if flask.request.url == NOT_FOUND_URL:
                LOGGER.debug(f"Allowing not found url: {flask.request.url}")
                page_404 = flask.render_template("404.html")
                page_404 = page_404.format(home_url=HOME_URL, sign_out_url=SIGN_OUT_URL)
                resp = flask.Response(status=200, response=page_404)
                resp.set_cookie(self._config._cookie_name, "", expires=0)
                return resp

            # [CodeFlow] - OAuth2 code is in path
            code = self._get_oauth_code()
            if code is not None:
                LOGGER.debug(f"Redirected with code= {code}")
                try:
                    id_token = self._get_identity_token(code)
                    redirect_url = flask.request.cookies.get(
                        REDIRECT_TO_COOKIE, MITZU_WEBAPP_URL
                    )

                    jwks_client = jwt.PyJWKClient(self._config._jwks_url)
                    signing_key = jwks_client.get_signing_key_from_jwt(id_token)
                    decoded_token = jwt.decode(
                        id_token,
                        signing_key.key,
                        algorithms=self._config._jwt_algo,
                        audience=self._config._client_id,
                    )

                    if decoded_token is None:
                        raise Exception("Unauthorized (Invalid jwt token)")

                    LOGGER.info("Authorization finished (caching)")
                    self.tokens[id_token] = decoded_token
                    LOGGER.info(f"User email: {self.get_user_email(id_token)}")

                    resp = flask.redirect(code=307, location=redirect_url)
                    resp.set_cookie(self._config._cookie_name, id_token)
                    resp.set_cookie(REDIRECT_TO_COOKIE, "", expires=0)
                    resp = self.add_no_cache_headers(resp)
                    return resp
                except Exception as e:
                    LOGGER.warn(f"Failed to authenticate: {str(e)}")
                    return self._get_unauthenticated_response()

            # Signout Flow
            jwt_encoded = flask.request.cookies.get(self._config._cookie_name)
            if SIGN_OUT_URL is not None and flask.request.url == SIGN_OUT_URL:
                if jwt_encoded in self.tokens.keys():
                    self.tokens.pop(jwt_encoded)
                location = self._config._sign_out_url
                resp = flask.redirect(code=307, location=location)
                resp.set_cookie(self._config._cookie_name, "", max_age=120)
                resp = self.add_no_cache_headers(resp)
                return resp

            # OAuth2 code flow starting + storing original link
            if not jwt_encoded:
                LOGGER.debug("Unauthorized (missing jwt_token cookie)")
                resp = flask.redirect(code=307, location=self._config._sign_in_url)
                clean_url = (
                    f"{flask.request.base_url}?{flask.request.query_string.decode()}"
                )
                redirect_cookie = flask.request.cookies.get(REDIRECT_TO_COOKIE)
                if redirect_cookie is None:
                    resp.set_cookie(REDIRECT_TO_COOKIE, clean_url)
                resp = self.add_no_cache_headers(resp)
                return resp

            # Authenticated
            if jwt_encoded in self.tokens.keys():
                return None

            # Unauthenticated
            return self._get_unauthenticated_response()

    def get_user_email(self, encoded_token: str) -> Optional[str]:
        val = self.tokens.get(encoded_token, {})
        if val is not None:
            return val.get("email")
        return None

    @classmethod
    def from_env_vars(cls, server: flask.Flask) -> MitzuAuthorizer:
        authorizer = CognitoAuthorizer()
        authorizer.setup_authorizer(server)
        return authorizer
