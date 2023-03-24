from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from flask import Flask

import mitzu.webapp.auth.authorizer as A
import mitzu.webapp.cache as C
import mitzu.webapp.configs as configs
import mitzu.webapp.storage as S
import mitzu.webapp.service.user_service as U
import mitzu.webapp.service.events_service as E

CONFIG_KEY = "dependencies"


@dataclass(frozen=True)
class Dependencies:

    authorizer: Optional[A.OAuthAuthorizer]
    storage: S.MitzuStorage
    queue: C.MitzuCache
    cache: C.MitzuCache
    events_service: E.EventsService
    user_service: Optional[U.UserService] = None

    @classmethod
    def from_configs(cls, server: Flask) -> Dependencies:
        delegate_cache: C.MitzuCache
        if configs.STORAGE_REDIS_HOST is not None:
            delegate_cache = C.RedisMitzuCache(global_prefix=configs.CACHE_PREFIX)
        else:
            delegate_cache = C.DiskMitzuCache(
                "cache", global_prefix=configs.CACHE_PREFIX
            )
        cache = C.RequestCache(delegate_cache)
        storage = S.MitzuStorage(cache)

        authorizer = None
        oauth_config = None
        user_service = None
        if configs.AUTH_BACKEND == "cognito":
            from mitzu.webapp.auth.cognito import Cognito

            oauth_config = Cognito.get_config()
        elif configs.AUTH_BACKEND == "google":
            from mitzu.webapp.auth.google import GoogleOAuth

            oauth_config = GoogleOAuth.get_config()

        elif configs.AUTH_BACKEND == "local" or configs.AUTH_SSO_ONLY_FOR_LOCAL_USERS:
            user_service = U.UserService(
                storage, root_password=configs.AUTH_ROOT_PASSWORD
            )

        if oauth_config or user_service:
            auth_config = A.AuthConfig(
                oauth=oauth_config,
                token_validator=A.JWTTokenValidator.create_from_oauth_config(
                    oauth_config
                )
                if oauth_config is not None
                else None,
                allowed_email_domain=configs.AUTH_ALLOWED_EMAIL_DOMAIN,
                token_signing_key=configs.AUTH_JWT_SECRET,
                session_timeout=configs.AUTH_SESSION_TIMEOUT,
                user_service=user_service,
            )
            authorizer = A.OAuthAuthorizer.create(auth_config)
            authorizer.setup_authorizer(server)

        queue: C.MitzuCache
        if configs.QUEUE_REDIS_HOST is not None:
            queue = C.RedisMitzuCache()
        else:
            queue = C.DiskMitzuCache("queue")

        # Adding cache layer over storage
        events_service = E.EventsService(storage)

        return Dependencies(
            authorizer=authorizer,
            cache=cache,
            storage=storage,
            queue=queue,
            user_service=user_service,
            events_service=events_service,
        )
