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

REDIRECT_TO = "redirect_to"
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
HEALTH_CHECK_PATH = os.getenv("HEALTH_CHECK_PATH", "/_health")


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

    def handle_code_redirect(self):
        code = get_oauth_code()

        message = bytes(f"{OAUTH_CLIENT_ID}:{OAUTH_CLIENT_SECRET}", "utf-8")
        secret_hash = base64.b64encode(message).decode()
        payload = {
            "grant_type": "authorization_code",
            "client_id": OAUTH_CLIENT_ID,
            "code": code,
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
            LOGGER.debug(f"Failed token resp: {resp.status_code}, {resp.content}")
            return flask.Response(status=resp.status_code, response=resp.content)

        cookie_val = f"{resp.json()['id_token']}"
        redirect_url = flask.request.cookies.get(REDIRECT_TO, MITZU_WEBAPP_URL)
        final_resp = flask.redirect(code=307, location=redirect_url)
        final_resp.set_cookie(OAUTH_JWT_COOKIE, cookie_val)
        final_resp.set_cookie(REDIRECT_TO, "", expires=0)
        LOGGER.debug(f"Setting cookie resp: {cookie_val}")
        return final_resp

    def setup_authorizer(self):
        @self.server.before_request
        def authorize_request():
            jwt_encoded = flask.request.cookies.get(OAUTH_JWT_COOKIE)
            code = get_oauth_code()
            resp: flask.Response
            if code is not None:
                LOGGER.debug(f"Redirected with code= {code}")
                resp = self.handle_code_redirect()
            elif flask.request.path == HEALTH_CHECK_PATH:
                LOGGER.debug("Health check")
                resp = flask.Response("ok", status=200)
            elif flask.request.url == NOT_FOUND_URL:
                LOGGER.debug(f"Allowing not found url: {flask.request.url}")
                page_404 = flask.render_template("404.html")
                page_404 = page_404.format(home_url=HOME_URL, sign_out_url=SIGN_OUT_URL)
                resp = flask.Response(status=200, response=page_404)
            elif SIGN_OUT_URL is not None and flask.request.url == SIGN_OUT_URL:
                if jwt_encoded in self.tokens:
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
                LOGGER.debug(f"Redirect {location}")
                resp = flask.redirect(code=307, location=location)
                resp.set_cookie(OAUTH_JWT_COOKIE, "", expires=0)
            elif not jwt_encoded:
                LOGGER.debug("Unauthorized (missing jwt_token cookie)")
                resp = flask.redirect(code=307, location=OAUTH_SIGN_IN_URL)
                clean_url = (
                    f"{flask.request.base_url}?{flask.request.query_string.decode()}"
                )
                resp.set_cookie(REDIRECT_TO, clean_url)
            elif jwt_encoded in self.tokens.keys():
                resp = None
            else:
                LOGGER.debug("Authorization started")
                try:
                    jwks_client = jwt.PyJWKClient(OAUTH_JWKS_URL)
                    signing_key = jwks_client.get_signing_key_from_jwt(jwt_encoded)
                    decoded_token = jwt.decode(
                        jwt_encoded,
                        signing_key.key,
                        algorithms=OAUTH_JWT_ALGORITHMS,
                        audience=OAUTH_JWT_AUDIENCE,
                    )

                    if decoded_token is None:
                        LOGGER.debug("Unauthorized (Invalid jwt token)")
                        resp = flask.redirect(code=307, location=SIGN_OUT_URL)
                    else:
                        LOGGER.debug("Authorization finished (caching)")
                        self.tokens[jwt_encoded] = decoded_token
                        LOGGER.debug(f"User email: {self.get_user_email(jwt_encoded)}")
                        resp = None
                except Exception as exc:
                    LOGGER.debug(f"Authorization error: {exc}")
                    resp = flask.redirect(code=307, location=NOT_FOUND_URL)

            if (
                OAUTH_AUTHORIZED_EMAIL_REG is not None
                and resp is None
                and flask.request.url
                not in (
                    NOT_FOUND_URL,
                    SIGN_OUT_URL,
                )
                and re.search(
                    OAUTH_AUTHORIZED_EMAIL_REG, self.get_user_email(jwt_encoded)
                )
                is None
            ):
                LOGGER.debug(f"Unauthorized email, redirecting to {NOT_FOUND_URL}")
                resp = flask.redirect(code=307, location=NOT_FOUND_URL)

            if resp is not None:
                resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                resp.headers["Pragma"] = "no-cache"
                resp.headers["Expires"] = "0"
                resp.headers["Cache-Control"] = "public, max-age=0"

            return resp

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
