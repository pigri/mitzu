from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import flask
import jwt
import mitzu.model as M

UNAUTHORIZED_URL = "UNAUTHORIZED_URL"
SIGN_OUT_URL = "SIGN_OUT_URL"
SIGN_OUT_REDIRECT_URL = "SIGN_OUT_REDIRECT_URL"
JWT_COOKIE = "JWT_COOKIE"
JWKS_URL = "JWKS_URL"
JWT_ALGORITHMS = "JWT_ALGORITHMS"
JWT_AUDIENCE = "JWT_AUDIENCE"


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
    signed_out_url: Optional[str] = None
    signed_out_redirect_url: Optional[str] = None

    jwt_token: M.ProtectedState[Dict[str, Any]] = M.ProtectedState(None)
    jwt_encoded: M.ProtectedState[str] = M.ProtectedState(None)

    def setup_authorizer(self):
        @self.server.before_request
        def check_cookies():
            jwt_encoded = flask.request.cookies.get(self.jwt_cookie)
            # print(
            #     f"<<<<< JWT Cookie ({self.jwt_cookie}): {flask.request.cookies.get(self.jwt_cookie)}"
            # )
            # print(f"<<<<< Cookies: {flask.request.cookies}")
            # print(f"<<<<< Code: {flask.request.args.get('code')}")

            resp: flask.Response
            if flask.request.url == self.unauthorized_url:
                resp = None
            elif (
                self.signed_out_url is not None
                and flask.request.full_path == self.signed_out_url
            ):
                resp = flask.redirect(code=301, location=self.signed_out_redirect_url)
                resp.set_cookie(self.jwt_cookie, "", expires=0)
            elif (
                not jwt_encoded
                or self.jwt_encoded.has_value()
                and jwt_encoded != self.jwt_encoded.get_value()
            ):
                resp = flask.redirect(code=301, location=self.unauthorized_url)

            elif jwt_encoded == self.jwt_encoded.get_value():
                resp = None

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
                    resp = flask.redirect(code=301, location=self.unauthorized_url)
                else:
                    self.jwt_encoded.set_value(jwt_encoded)
                    self.jwt_token.set_value(decoded_token)
                    resp = None
            except Exception as exc:
                print(exc)
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
        jwt_cookie = os.getenv(JWT_COOKIE)
        jwks_url = os.getenv(JWKS_URL)
        jwt_audience = os.getenv(JWT_AUDIENCE)
        jwt_algorithms = os.getenv(JWT_ALGORITHMS, "RS256").split(",")

        if unauthorized_url is None:
            raise Exception(f"{UNAUTHORIZED_URL} env var is missing")
        if jwt_cookie is None:
            raise Exception(f"{JWT_COOKIE} env var is missing")
        if jwks_url is None:
            raise Exception(f"{JWKS_URL} env var is missing")
        if jwt_audience is None:
            raise Exception(f"{JWT_AUDIENCE} env var is missing")

        authorizer = JWTMitzuAuthorizer(
            server=server,
            unauthorized_url=unauthorized_url,
            jwt_cookie=jwt_cookie,
            jwt_algorithms=jwt_algorithms,
            jwt_audience=jwt_audience,
            jwks_url=jwks_url,
            signed_out_url=os.getenv(SIGN_OUT_URL),
            signed_out_redirect_url=os.getenv(SIGN_OUT_REDIRECT_URL),
        )
        authorizer.setup_authorizer()
        return authorizer
