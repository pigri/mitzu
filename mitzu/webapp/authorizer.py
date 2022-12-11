from __future__ import annotations

import base64
import os
import re
from abc import ABC, abstractmethod
from copy import copy
from dataclasses import dataclass, field
from typing import Dict, Optional
from urllib import parse

import flask
import jwt
import requests
from mitzu.helper import LOGGER

REDIRECT_TO_COOKIE = "redirect_to"
HOME_URL = os.getenv("HOME_URL")
MITZU_WEBAPP_URL = os.getenv("MITZU_WEBAPP_URL")
NOT_FOUND_URL = os.getenv("NOT_FOUND_URL")
SIGN_OUT_URL = os.getenv("SIGN_OUT_URL")
OAUTH_SIGN_OUT_REDIRECT_URL = os.getenv("OAUTH_SIGN_OUT_REDIRECT_URL")
OAUTH_SIGN_IN_URL = os.getenv("OAUTH_SIGN_IN_URL")
OAUTH_JWT_ALGORITHMS = os.getenv("OAUTH_JWT_ALGORITHMS", "RS256").split(",")
OAUTH_JWT_AUDIENCE = os.getenv("OAUTH_JWT_AUDIENCE")
OAUTH_JWT_COOKIE = os.getenv("OAUTH_JWT_COOKIE", "access_token")
OAUTH_REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI")
OAUTH_TOKEN_URL = os.getenv("OAUTH_TOKEN_URL")
OAUTH_CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET")
OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID")
OAUTH_AUTHORIZED_EMAIL_REG = os.getenv("OAUTH_AUTHORIZED_EMAIL_REG")
OAUTH_JWKS_URL = os.getenv("OAUTH_JWKS_URL")


def get_oauth_code() -> Optional[str]:
    code = flask.request.values.get("code")
    if code is not None:
        return code
    parse_result = parse.urlparse(flask.request.url)
    params = parse.parse_qs(parse_result.query)
    code_ls = params.get("code")
    if code_ls is not None:
        return code_ls[0]
    return None


class MitzuAuthorizer(ABC):
    @abstractmethod
    def get_user_email(self, encoded_token: str) -> Optional[str]:
        pass


class GuestMitzuAuthorizer(MitzuAuthorizer):
    def get_user_email(self, _: str) -> Optional[str]:
        return "Guest"


@dataclass
class JWTMitzuAuthorizer(MitzuAuthorizer):
    server: flask.Flask
    tokens: Dict[str, Dict[str, str]] = field(default_factory=lambda: copy({}))

    def _get_identity_token(self, auth_code) -> str:
        message = bytes(f"{OAUTH_CLIENT_ID}:{OAUTH_CLIENT_SECRET}", "utf-8")
        secret_hash = base64.b64encode(message).decode()
        payload = {
            "grant_type": "authorization_code",
            "client_id": OAUTH_CLIENT_ID,
            "code": auth_code,
            "redirect_uri": OAUTH_REDIRECT_URI,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {secret_hash}",
        }
        LOGGER.debug(f"Payload: {payload}")
        LOGGER.debug(f"Oauth Token URL: {OAUTH_TOKEN_URL}")

        resp = requests.post(OAUTH_TOKEN_URL, params=payload, headers=headers)
        if resp.status_code != 200:
            raise Exception(f"Unexpected response: {resp.status_code}, {resp.content}")

        return resp.json()["id_token"]

    def _get_unauthenticated_response(self) -> flask.Response:
        resp = flask.redirect(code=307, location=NOT_FOUND_URL)
        resp.set_cookie(OAUTH_JWT_COOKIE, "", expires=0)
        resp = self.add_no_cache_headers(resp)
        return resp


    def add_no_cache_headers(self, resp: flask.Response) -> flask.Response:
        if resp is not None:
            resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            resp.headers["Pragma"] = "no-cache"
            resp.headers["Expires"] = "0"
            resp.headers["Cache-Control"] = "public, max-age=0"
        return resp

    def setup_authorizer(self):
        @self.server.before_request
        def authorize_request():
            resp: flask.Response

            # Not found URL
            if flask.request.url == NOT_FOUND_URL:
                LOGGER.debug(f"Allowing not found url: {flask.request.url}")
                page_404 = flask.render_template("404.html")
                page_404 = page_404.format(home_url=HOME_URL, sign_out_url=SIGN_OUT_URL)
                resp = flask.Response(status=200, response=page_404)
                resp.set_cookie(OAUTH_JWT_COOKIE, "", expires=0)
                return resp

            # [CodeFlow] - OAuth2 code is in path
            code = get_oauth_code()
            if code is not None:
                LOGGER.debug(f"Redirected with code= {code}")
                try:
                    id_token = self._get_identity_token(code)
                    redirect_url = flask.request.cookies.get(
                        REDIRECT_TO_COOKIE, MITZU_WEBAPP_URL
                    )
                    resp = flask.redirect(code=307, location=redirect_url)
                    resp.set_cookie(OAUTH_JWT_COOKIE, id_token)
                    resp.set_cookie(REDIRECT_TO_COOKIE, "", expires=0)
                    LOGGER.debug(f"Setting cookie resp: {id_token}")

                    jwks_client = jwt.PyJWKClient(OAUTH_JWKS_URL)
                    signing_key = jwks_client.get_signing_key_from_jwt(id_token)
                    decoded_token = jwt.decode(
                        id_token,
                        signing_key.key,
                        algorithms=OAUTH_JWT_ALGORITHMS,
                        audience=OAUTH_JWT_AUDIENCE,
                    )

                    if decoded_token is None:
                        raise Exception("Unauthorized (Invalid jwt token)")

                    LOGGER.info("Authorization finished (caching)")
                    self.tokens[id_token] = decoded_token
                    LOGGER.info(f"User email: {self.get_user_email(id_token)}")

                    resp = self.add_no_cache_headers(resp)
                    return resp
                except Exception as e:
                    LOGGER.warn(f"Failed to authenticate: {str(e)}")
                    return self._get_unauthenticated_response()

            # Signout Flow
            jwt_encoded = flask.request.cookies.get(OAUTH_JWT_COOKIE)
            if SIGN_OUT_URL is not None and flask.request.url == SIGN_OUT_URL:
                if jwt_encoded in self.tokens.keys():
                    self.tokens.pop(jwt_encoded)
                LOGGER.debug(f"Signed out URL: {SIGN_OUT_URL}")
                location = (
                    f"{OAUTH_SIGN_OUT_REDIRECT_URL}?"
                    "response_type=code&"
                    f"client_id={OAUTH_CLIENT_ID}&"
                    f"redirect_uri={OAUTH_REDIRECT_URI}&"
                    # state=STATE& todo
                    f"scope=email+openid"
                )
                LOGGER.debug(f"Signout Redirect {location}")
                resp = flask.redirect(code=307, location=location)
                resp.set_cookie(OAUTH_JWT_COOKIE, "", max_age=120)
                resp = self.add_no_cache_headers(resp)
                return resp

            # OAuth2 code flow starting + storing original link
            if not jwt_encoded:
                LOGGER.debug("Unauthorized (missing jwt_token cookie)")
                resp = flask.redirect(code=307, location=OAUTH_SIGN_IN_URL)
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
        authorizer = JWTMitzuAuthorizer(server=server)
        authorizer.setup_authorizer()
        return authorizer
