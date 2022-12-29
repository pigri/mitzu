from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional
from mitzu.webapp.auth.authorizer import OAuthConfig


@dataclass(frozen=True)
class GoogleOAuth:
    _client_id: str
    _client_secret: str
    _project_id: str
    _redirect_url: str
    _cookie_name: str
    _jwt_algo: List[str]

    @classmethod
    def get_config(
        cls,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        project_id: Optional[str] = None,
        redirect_url: Optional[str] = None,
        jwt_algo: List[str] = ["RS256"],
    ) -> OAuthConfig:
        client_id = cls.fallback_to_env_var(client_id, "GOOGLE_CLIENT_ID")
        project_id = cls.fallback_to_env_var(project_id, "GOOGLE_PROJECT_ID")
        redirect_url = cls.fallback_to_env_var(redirect_url, "GOOGLE_REDIRECT_URL")
        jwt_algorithms = cls.fallback_to_env_var(
            ",".join(jwt_algo), "GOOGLE_JWT_ALGORITHMS"
        ).split(",")

        return OAuthConfig(
            client_id=client_id,
            client_secret=cls.fallback_to_env_var(
                client_secret, "GOOGLE_CLIENT_SECRET"
            ),
            jwks_url="https://www.googleapis.com/oauth2/v3/certs",
            sign_in_url=(
                " https://accounts.google.com/o/oauth2/auth?"
                "approval_prompt=force&"
                f"client_id={client_id}&"
                "response_type=code&"
                "scope=email+openid&"
                "access_type=offline&"
                f"redirect_uri={redirect_url}"
            ),
            sign_out_url=None,
            token_url="https://oauth2.googleapis.com/token",
            jwt_algorithms=jwt_algorithms,
        )

    @classmethod
    def fallback_to_env_var(cls, value: Optional[str], env_var: str):
        if value is not None:
            return value
        return os.getenv(env_var)
