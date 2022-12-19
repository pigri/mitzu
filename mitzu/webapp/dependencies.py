from __future__ import annotations

from dataclasses import dataclass
import mitzu.webapp.auth.authorizer as A
import mitzu.webapp.storage as S
import mitzu.webapp.cache as C
import mitzu.webapp.configs as configs
from flask import Flask
from typing import Optional

CONFIG_KEY = "dependencies"


@dataclass(frozen=True)
class Dependencies:

    authorizer: Optional[A.MitzuAuthorizer]
    storage: S.MitzuStorage
    cache: C.MitzuCache

    @classmethod
    def from_configs(cls, server: Flask) -> Dependencies:
        authorizer = None
        if configs.OAUTH_BACKEND == "cognito":
            from mitzu.webapp.auth.cognito import CognitoConfig

            config = CognitoConfig()
            authorizer = A.OAuthAuthorizer(oauth_config=config)
            authorizer.setup_authorizer(server)

        cache: C.MitzuCache
        if configs.REDIS_URL is not None:
            cache = C.RedisMitzuCache()
        else:
            cache = C.DiskMitzuCache()

        storage: S.MitzuStorage
        if configs.MITZU_BASEPATH.startswith("s3://"):
            storage = S.S3MitzuStorage(configs.MITZU_BASEPATH[5:])
        else:
            storage = S.FileSystemStorage(configs.MITZU_BASEPATH)

        # Adding cache layer over storage
        storage = S.CachingMitzuStorage(storage)

        return Dependencies(authorizer=authorizer, cache=cache, storage=storage)
