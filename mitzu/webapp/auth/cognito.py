from __future__ import annotations

import os
from typing import List, Optional
from mitzu.webapp.auth.authorizer import (
    OAuthConfig,
)


class Cognito:
    @classmethod
    def get_config(
        cls,
        pool_id: Optional[str] = None,
        region: Optional[str] = None,
        domain: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_url: Optional[str] = None,
        jwt_algo: List[str] = ["RS256"],
    ) -> OAuthConfig:
        """
        Creates a new OAuthConfig instance using the Cognito specific settings.

        :param pool_id: ID of the AWS Cognito User Pool (something like ``<aws region>_xxxxxxxxx``),
            if it's not set then the ``COGNITO_POOL_ID`` environmental variable will be used
        :param region: AWS region of the AWS Cognito User Pool (eg. eu-west-1),
            if it's not set then the ``COGNITO_REGION`` environmental variable will be used
        :param domain: AWS Cognito domain set for the App integrations
            (``xxxxx`` from the ``https://xxxxx.auth.eu-west-1.amazoncognito.com``),
            if it's not set the the ``COGNITO_DOMAIN`` environmental variable will be used
        :param client_id: Client ID of the configured App,
            if it's not set then the ``COGNITO_CLIENT_ID`` environmental variable will be used
        :param client_secret: Client secret of the configured App,
            if it's not set then the ``COGNITO_CLIENT_SECRET`` environmental variable will be used
        :param redirect_url: OAuth redirection URL, it's ``<your Mitzu base URL>/auth/oauth`` eg. ``http://localhost:8082/auth/oauth``,
            if it's not set then the ``COGNITO_REDIRECT_URL`` environmental variable will be used
        :param jwt_algo: List of accepted JWT token signing algorithms, by default it's ``["RS256"]``
            it can be overwritted with the ``COGNITO_JWT_ALGORITHMS`` environmental variable
        """
        pool_id = cls.fallback_to_env_var(pool_id, "COGNITO_POOL_ID")
        region = cls.fallback_to_env_var(region, "COGNITO_REGION")
        domain = cls.fallback_to_env_var(domain, "COGNITO_DOMAIN")
        client_id = cls.fallback_to_env_var(client_id, "COGNITO_CLIENT_ID")
        redirect_url = cls.fallback_to_env_var(redirect_url, "COGNITO_REDIRECT_URL")
        jwt_algorithms = cls.fallback_to_env_var(
            ",".join(jwt_algo), "COGNITO_JWT_ALGORITHMS"
        ).split(",")

        return OAuthConfig(
            client_id=client_id,
            client_secret=cls.fallback_to_env_var(
                client_secret, "COGNITO_CLIENT_SECRET"
            ),
            jwks_url=f"https://cognito-idp.{region}.amazonaws.com/{pool_id}/.well-known/jwks.json",
            sign_in_url=(
                f"https://{domain}/oauth2/authorize?"
                f"client_id={client_id}&"
                "response_type=code&"
                "scope=email+openid&"
                f"redirect_uri={redirect_url}"
            ),
            sign_out_url=(
                f"https://{domain}/logout?"
                f"client_id={client_id}&"
                "response_type=code&"
                "scope=email+openid&"
                f"redirect_uri={redirect_url}"
            ),
            token_url=f"https://{domain}/oauth2/token",
            jwt_algorithms=jwt_algorithms,
        )

    @classmethod
    def fallback_to_env_var(cls, value: Optional[str], env_var: str):
        if value is not None:
            return value
        return os.getenv(env_var)
