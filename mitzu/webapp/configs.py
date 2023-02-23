from uuid import uuid4
import os
from typing import Tuple, Optional

# base
LAUNCH_UID = str(uuid4())
HEALTH_CHECK_PATH = os.getenv("HEALTH_CHECK_PATH", "/_health")


# dash
GRAPH_POLL_INTERVAL_MS = int(os.getenv("GRAPH_POLL_INTERVAL_MS", 100))
BACKGROUND_CALLBACK = bool(os.getenv("BACKGROUND_CALLBACK", "true").lower() != "false")
DASH_ASSETS_FOLDER = os.getenv("DASH_ASSETS_FOLDER", "assets")
DASH_ASSETS_URL_PATH = os.getenv("DASH_ASSETS_URL_PATH", "assets")
DASH_SERVE_LOCALLY = bool(os.getenv("DASH_SERVE_LOCALLY", True))
DASH_TITLE = os.getenv("DASH_TITLE", "Mitzu")
DASH_FAVICON_PATH = os.getenv("DASH_FAVICON_PATH", "assets/favicon.ico")
DASH_COMPRESS_RESPONSES = bool(os.getenv("DASH_COMPRESS_RESPONSES", True))
DASH_LOGO_PATH = os.getenv("DASH_LOGO_PATH", "/assets/mitzu-logo-light.svg")


# auth
AUTH_BACKEND = os.getenv("AUTH_BACKEND")
AUTH_ALLOWED_EMAIL_DOMAIN = os.getenv("AUTH_ALLOWED_EMAIL_DOMAIN")
AUTH_JWT_SECRET = os.getenv("AUTH_JWT_SECRET", "mitzu-dev-env")
AUTH_SESSION_TIMEOUT = os.getenv("AUTH_SESSION_TIMEOUT", 7 * 24 * 60 * 60)
AUTH_ROOT_PASSWORD = os.getenv("AUTH_ROOT_PASSWORD", "test")
AUTH_ROOT_USER_EMAIL = os.getenv("AUTH_ROOT_USER_EMAIL", "root@local")


# cache
CACHE_EXPIRATION = int(os.getenv("CACHE_EXPIRATION", "600"))

# redis cache
QUEUE_REDIS_HOST = os.getenv("QUEUE_REDIS_HOST")
QUEUE_REDIS_PORT = int(os.getenv("QUEUE_REDIS_PORT", 6379))
QUEUE_REDIS_DB = os.getenv("STORAGE_REDIS_DB")
QUEUE_REDIS_USERNAME = os.getenv("QUEUE_REDIS_USERNAME")
QUEUE_REDIS_PASSWORD = os.getenv("QUEUE_REDIS_PASSWORD")

STORAGE_REDIS_HOST = os.getenv("STORAGE_REDIS_HOST")
STORAGE_REDIS_PORT = int(os.getenv("STORAGE_REDIS_PORT", 6379))
STORAGE_REDIS_DB = os.getenv("STORAGE_REDIS_DB")
STORAGE_REDIS_USERNAME = os.getenv("STORAGE_REDIS_USERNAME")
STORAGE_REDIS_PASSWORD = os.getenv("STORAGE_REDIS_PASSWORD")


# disk cache
DISK_CACHE_PATH = os.getenv("DISK_CACHE_PATH", "./cache")

# storage
SETUP_SAMPLE_PROJECT = bool(
    os.getenv("SETUP_SAMPLE_PROJECT", "true").lower() != "false"
)


KALEIDO_CONFIGS = os.getenv("KALEIDO_CONFIGS", "--disable-gpu-*,--single-process")


def get_kaleido_configs() -> Optional[Tuple[str, ...]]:
    if KALEIDO_CONFIGS:
        return tuple(KALEIDO_CONFIGS.split(","))
    else:
        return None
