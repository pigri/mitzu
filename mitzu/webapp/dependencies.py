from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from flask import Flask

import mitzu.webapp.auth.authorizer as A
import mitzu.webapp.cache as C
import mitzu.webapp.configs as configs
import mitzu.webapp.storage as S

CONFIG_KEY = "dependencies"


@dataclass(frozen=True)
class Dependencies:

    authorizer: Optional[A.OAuthAuthorizer]
    storage: S.MitzuStorage
    queue: C.MitzuCache
    cache: C.MitzuCache

    @classmethod
    def from_configs(cls, server: Flask) -> Dependencies:
        authorizer = None
        oauth_config = None
        if configs.OAUTH_BACKEND == "cognito":
            from mitzu.webapp.auth.cognito import Cognito

            oauth_config = Cognito.get_config()
        elif configs.OAUTH_BACKEND == "google":
            from mitzu.webapp.auth.google import GoogleOAuth

            oauth_config = GoogleOAuth.get_config()

        if oauth_config:
            auth_config = A.AuthConfig(
                oauth=oauth_config,
                token_validator=A.JWTTokenValidator.create_from_oauth_config(
                    oauth_config
                ),
                allowed_email_domain=configs.AUTH_ALLOWED_EMAIL_DOMAIN,
                token_signing_key=configs.AUTH_JWT_SECRET,
                session_timeout=configs.AUTH_SESSION_TIMEOUT,
            )
            authorizer = A.OAuthAuthorizer.create(auth_config)
            authorizer.setup_authorizer(server)

        queue: C.MitzuCache
        if configs.QUEUE_REDIS_HOST is not None:
            queue = C.RedisMitzuCache()
        else:
            queue = C.DiskMitzuCache()

        cache: C.MitzuCache
        if configs.STORAGE_REDIS_HOST is not None:
            cache = C.RedisMitzuCache()
        else:
            cache = C.DiskMitzuCache()

        # Adding cache layer over storage
        storage = S.MitzuStorage(cache)
        if configs.SETUP_SAMPLE_PROJECT:
            S.setup_sample_project(storage)

        return Dependencies(
            authorizer=authorizer, cache=cache, storage=storage, queue=queue
        )
