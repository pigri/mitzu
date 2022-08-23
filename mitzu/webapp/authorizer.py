from __future__ import annotations

import base64
import logging
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib import parse

import flask
import jwt
import mitzu.model as M
import requests

LOG_HANDLER = sys.stderr if os.getenv("LOG_HANDLER") == "stderr" else sys.stdout

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", logging.INFO))
logger.addHandler(logging.StreamHandler(LOG_HANDLER))

MITZU_WEBAPP_URL = "MITZU_WEBAPP_URL"
UNAUTHORIZED_URL = "UNAUTHORIZED_URL"
SIGN_OUT_URL = "SIGN_OUT_URL"
SIGN_OUT_REDIRECT_URL = "SIGN_OUT_REDIRECT_URL"

OAUTH_JWT_ALGORITHMS = "OAUTH_JWT_ALGORITHMS"
OAUTH_JWT_AUDIENCE = "OAUTH_JWT_AUDIENCE"
OAUTH_JWT_COOKIE = "OAUTH_JWT_COOKIE"
OAUTH_REDIRECT_URI = "OAUTH_REDIRECT_URI"
OAUTH_TOKEN_URL = "OAUTH_TOKEN_URL"
OAUTH_CLIENT_SECRET = "OAUTH_CLIENT_SECRET"
OAUTH_CLIENT_ID = "OAUTH_CLIENT_ID"
OAUTH_JWKS_URL = "OAUTH_JWKS_URL"


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
    def get_user_email(self) -> Optional[str]:
        pass


class GuestMitzuAuthorizer(MitzuAuthorizer):
    def get_user_email(self) -> Optional[str]:
        return "Guest"


@dataclass
class JWTMitzuAuthorizer(MitzuAuthorizer):

    server: flask.Flask
    unauthorized_url: str
    jwt_cookie: str
    jwt_audience: str
    jwt_algorithms: List[str]
    jwks_url: str
    client_id: str
    client_secret: str
    oauth_token_url: str
    app_url: str
    redirect_uri: str
    signed_out_url: Optional[str] = None
    signed_out_redirect_url: Optional[str] = None

    jwt_token: M.ProtectedState[Dict[str, Any]] = M.ProtectedState(None)
    jwt_encoded: M.ProtectedState[str] = M.ProtectedState(None)

    def handle_code_redirect(self):
        code = get_oauth_code()

        message = bytes(f"{self.client_id}:{self.client_secret}", "utf-8")
        secret_hash = base64.b64encode(message).decode()
        payload = {
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "code": code,
            "redirect_uri": self.redirect_uri,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {secret_hash}",
        }
        logger.info(f"Payload: {payload}")
        logger.info(f"Oauth Token URL: {self.oauth_token_url}")
        resp = requests.post(self.oauth_token_url, params=payload, headers=headers)
        if resp.status_code != 200:
            logger.info(f"Failed token resp: {resp.status_code}, {resp.content}")
            return flask.Response(status=resp.status_code, response=resp.content)

        cookie_val = f"{resp.json()['id_token']}"
        final_resp = flask.redirect(code=301, location=self.app_url)
        final_resp.set_cookie(self.jwt_cookie, cookie_val)
        logger.info(f"Setting cookie resp: {cookie_val}")
        return final_resp

    def setup_authorizer(self):
        @self.server.before_request
        def authorize_request():
            jwt_encoded = flask.request.cookies.get(self.jwt_cookie)
            code = get_oauth_code()
            resp: flask.Response
            if code is not None:
                logger.info(f"Redirected with code= {code}")
                resp = self.handle_code_redirect()
            elif flask.request.url == self.unauthorized_url:
                logger.info(f"Unauthorized URL: {self.unauthorized_url}")
                resp = None
            elif (
                self.signed_out_url is not None
                and flask.request.url == self.signed_out_url
            ):
                logger.info(f"Signed out URL: {self.signed_out_url}")
                location = (
                    f"{self.signed_out_redirect_url}?"
                    "response_type=code&"
                    f"client_id={self.client_id}&"
                    f"redirect_uri={self.redirect_uri}&"
                    # state=STATE& todo
                    f"scope=email+openid"
                )
                logger.info(f"Redirect {location}")
                resp = flask.redirect(code=301, location=location)
                resp.set_cookie(self.jwt_cookie, "", expires=0)
            elif not jwt_encoded:
                logger.info("Unauthorized (missing jwt_token cookie)")
                resp = flask.redirect(code=301, location=self.unauthorized_url)
            elif jwt_encoded == self.jwt_encoded.get_value():
                resp = None
            else:
                logger.info("Authorization started")
                try:
                    jwks_client = jwt.PyJWKClient(self.jwks_url)
                    signing_key = jwks_client.get_signing_key_from_jwt(jwt_encoded)
                    decoded_token = jwt.decode(
                        jwt_encoded,
                        signing_key.key,
                        algorithms=self.jwt_algorithms,
                        audience=self.jwt_audience,
                    )
                    if decoded_token is None:
                        logger.info("Unauthorized (Invalid jwt token)")
                        resp = flask.redirect(code=301, location=self.unauthorized_url)
                    else:
                        logger.info("Authorization finished (caching)")
                        self.jwt_encoded.set_value(jwt_encoded)
                        self.jwt_token.set_value(decoded_token)
                        resp = None
                except Exception as exc:
                    logger.info(f"Authorization error: {exc}")
                    resp = flask.redirect(code=301, location=self.unauthorized_url)

            if resp is not None:
                resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                resp.headers["Pragma"] = "no-cache"
                resp.headers["Expires"] = "0"
                resp.headers["Cache-Control"] = "public, max-age=0"
            return resp

    def get_jwt_token(self) -> Optional[Dict[str, Any]]:
        return self.jwt_token.get_value()

    def get_user_email(self) -> Optional[str]:
        val = self.jwt_token.get_value()
        if val is not None:
            return val.get("email")
        return None

    @classmethod
    def from_env_vars(cls, server: flask.Flask) -> MitzuAuthorizer:
        unauthorized_url = os.getenv(UNAUTHORIZED_URL)
        jwt_cookie = os.getenv(OAUTH_JWT_COOKIE)
        jwks_url = os.getenv(OAUTH_JWKS_URL)
        jwt_audience = os.getenv(OAUTH_JWT_AUDIENCE)
        jwt_algorithms = os.getenv(OAUTH_JWT_ALGORITHMS, "RS256").split(",")
        client_id = os.getenv(OAUTH_CLIENT_ID)
        client_secret = os.getenv(OAUTH_CLIENT_SECRET)
        oauth_token_url = os.getenv(OAUTH_TOKEN_URL)
        redirect_uri = os.getenv(OAUTH_REDIRECT_URI)
        app_url = os.getenv(MITZU_WEBAPP_URL)
        jwt_cookie = os.getenv(OAUTH_JWT_COOKIE)

        if unauthorized_url is None:
            raise Exception(f"{UNAUTHORIZED_URL} env var is missing")
        if jwt_cookie is None:
            raise Exception(f"{OAUTH_JWT_COOKIE} env var is missing")
        if jwks_url is None:
            raise Exception(f"{OAUTH_JWKS_URL} env var is missing")
        if jwt_audience is None:
            raise Exception(f"{OAUTH_JWT_AUDIENCE} env var is missing")
        if client_id is None:
            raise Exception(f"{OAUTH_CLIENT_ID} env var is missing")
        if client_secret is None:
            raise Exception(f"{OAUTH_CLIENT_SECRET} env var is missing")
        if oauth_token_url is None:
            raise Exception(f"{OAUTH_TOKEN_URL} env var is missing")
        if redirect_uri is None:
            raise Exception(f"{OAUTH_REDIRECT_URI} env var is missing")
        if app_url is None:
            raise Exception(f"{MITZU_WEBAPP_URL} env var is missing")

        authorizer = JWTMitzuAuthorizer(
            server=server,
            unauthorized_url=unauthorized_url,
            jwt_cookie=jwt_cookie,
            jwt_algorithms=jwt_algorithms,
            jwt_audience=jwt_audience,
            jwks_url=jwks_url,
            client_id=client_id,
            client_secret=client_secret,
            oauth_token_url=oauth_token_url,
            redirect_uri=redirect_uri,
            app_url=app_url,
            signed_out_url=os.getenv(SIGN_OUT_URL),
            signed_out_redirect_url=os.getenv(SIGN_OUT_REDIRECT_URL),
        )
        authorizer.setup_authorizer()
        return authorizer
