from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class GoogleOAuthConfig:
    _client_id: str
    _client_secret: str
    _project_id: str
    _redirect_url: str
    _cookie_name: str
    _jwt_algo: List[str]

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        project_id: Optional[str] = None,
        redirect_url: Optional[str] = None,
        jwt_algo: List[str] = ["RS256"],
    ):
        object.__setattr__(
            self, "_client_id", self.fallback_to_env_var(client_id, "GOOGLE_CLIENT_ID")
        )
        object.__setattr__(
            self,
            "_client_secret",
            self.fallback_to_env_var(client_secret, "GOOGLE_CLIENT_SECRET"),
        )
        object.__setattr__(
            self,
            "_project_id",
            self.fallback_to_env_var(project_id, "GOOGLE_PROJECT_ID"),
        )
        object.__setattr__(
            self,
            "_redirect_url",
            self.fallback_to_env_var(redirect_url, "GOOGLE_REDIRECT_URL"),
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
            " https://accounts.google.com/o/oauth2/auth?"
            "approval_prompt=force&"
            f"client_id={self._client_id}&"
            "response_type=code&"
            "scope=email+openid&"
            "access_type=offline&"
            f"redirect_uri={self._redirect_url}"
        )

    @property
    def token_url(self) -> str:
        return "https://oauth2.googleapis.com/token"

    @property
    def jwks_url(self) -> str:
        return "https://www.googleapis.com/oauth2/v3/certs"

    @property
    def jwt_algorithms(self) -> List[str]:
        return self._jwt_algo
