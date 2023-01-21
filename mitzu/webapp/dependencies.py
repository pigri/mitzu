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
    cache: C.MitzuCache

    @classmethod
    def from_configs(cls, server: Flask) -> Dependencies:
        authorizer = None
        auth_config = None
        if configs.OAUTH_BACKEND == "cognito":
            from mitzu.webapp.auth.cognito import Cognito

            auth_config = Cognito.get_config()
        elif configs.OAUTH_BACKEND == "google":
            from mitzu.webapp.auth.google import GoogleOAuth

            auth_config = GoogleOAuth.get_config()

        if auth_config:
            authorizer = A.OAuthAuthorizer.create(
                oauth_config=auth_config,
                allowed_email_domain=configs.AUTH_ALLOWED_EMAIL_DOMAIN,
            )
            authorizer.setup_authorizer(server)

        cache: C.MitzuCache
        if configs.REDIS_URL is not None:
            cache = C.RedisMitzuCache()
        else:
            cache = C.DiskMitzuCache()

        # Adding cache layer over storage
        storage = S.MitzuStorage(cache)
        if configs.SETUP_SAMPLE_PROJECT:
            S.setup_sample_project(storage)

        return Dependencies(authorizer=authorizer, cache=cache, storage=storage)
