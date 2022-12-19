from __future__ import annotations

from dataclasses import dataclass
import mitzu.webapp.auth.authorizer as A
import mitzu.webapp.storage as S
import mitzu.webapp.cache as C
import mitzu.webapp.configs as configs
from flask import Flask

CONFIG_KEY = "dependencies"


@dataclass(frozen=True)
class Dependencies:

    authorizer: A.MitzuAuthorizer
    storage: S.MitzuStorage
    cache: C.MitzuCache

    @classmethod
    def from_configs(cls, server: Flask) -> Dependencies:
        auth: A.MitzuAuthorizer
        if configs.OAUTH_BACKEND == 'cognito':
            from mitzu.webapp.auth.cognito import CognitoAuthorizer
            auth = CognitoAuthorizer.from_env_vars(server)
        else:
            from mitzu.webapp.auth.authorizer import GuestAuthorizer
            auth = GuestAuthorizer()

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

        return Dependencies(authorizer=auth, cache=cache, storage=storage)
