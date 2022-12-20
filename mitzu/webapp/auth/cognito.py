from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional
from mitzu.webapp.auth.authorizer import (
    OAuthConfig,
)


@dataclass(frozen=True)
class CognitoConfig(OAuthConfig):
    _client_id: str
    _client_secret: str
    _domain: str
    _region: str
    _pool_id: str
    _redirect_url: str
    _jwt_algo: List[str]

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

    @classmethod
    def fallback_to_env_var(cls, value: Optional[str], env_var: str):
        if value is not None:
            return value
        return os.getenv(env_var)

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def client_secret(self) -> str:
        return self._client_secret

    @property
    def sign_in_url(self) -> str:
        return (
            f"https://{self._domain}/oauth2/authorize?"
            f"client_id={self._client_id}&"
            "response_type=code&"
            "scope=email+openid&"
            f"redirect_uri={self._redirect_url}"
        )

    @property
    def sign_out_url(self) -> str:
        return (
            f"https://{self._domain}/logout?"
            f"client_id={self._client_id}&"
            "response_type=code&"
            "scope=email+openid&"
            f"redirect_uri={self._redirect_url}"
        )

    @property
    def token_url(self) -> str:
        return f"https://{self._domain}/oauth2/token"

    @property
    def jwks_url(self) -> str:
        return f"https://cognito-idp.{self._region}.amazonaws.com/{self._pool_id}/.well-known/jwks.json"

    @property
    def jwt_algorithms(self) -> List[str]:
        return self._jwt_algo
