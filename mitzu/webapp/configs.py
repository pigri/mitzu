import os
from typing import Tuple, Optional

HOME_URL = os.getenv("HOME_URL")
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
# dash
GRAPH_POLL_INTERVAL_MS = int(os.getenv("GRAPH_POLL_INTERVAL_MS", 300))
DASH_TITLE = os.getenv("DASH_TITLE", "Mitzu")
DASH_FAVICON_PATH = os.getenv("DASH_FAVICON_PATH", "assets/favicon.ico")
DASH_LOGO_PATH = os.getenv("DASH_LOGO_PATH", "/assets/mitzu-logo-light.svg")


# auth
AUTH_BACKEND = os.getenv("AUTH_BACKEND")
AUTH_ALLOWED_EMAIL_DOMAIN = os.getenv("AUTH_ALLOWED_EMAIL_DOMAIN")
AUTH_JWT_SECRET = os.getenv("AUTH_JWT_SECRET", "mitzu-dev-env")
AUTH_SESSION_TIMEOUT = os.getenv("AUTH_SESSION_TIMEOUT", 7 * 24 * 60 * 60)
AUTH_ROOT_PASSWORD = os.getenv("AUTH_ROOT_PASSWORD")
AUTH_ROOT_USER_EMAIL = os.getenv("AUTH_ROOT_USER_EMAIL")

# cache
CACHE_EXPIRATION = int(os.getenv("CACHE_EXPIRATION", "600"))
CACHE_PREFIX = os.getenv("CACHE_PREFIX")
CACHE_REDIS_URL = os.getenv("CACHE_REDIS_URL")

# storage
SETUP_SAMPLE_PROJECT = bool(
    os.getenv("SETUP_SAMPLE_PROJECT", "false").lower() != "false"
)

SECRET_ENCRYPTION_KEY = os.getenv("SECRET_ENCRYPTION_KEY", None)
STORAGE_CONNECTION_STRING = os.getenv(
    "STORAGE_CONNECTION_STRING",
    "sqlite:///storage.db?cache=shared&check_same_thread=False",
)

ENABLE_USAGE_TRACKING = os.getenv("ENABLE_USAGE_TRACKING", "true").lower() != "false"
TRACKING_API_KEY = os.getenv("TRACKING_API_KEY", "")
TRACKING_HOST = os.getenv("TRACKING_HOST")

KALEIDO_CONFIGS = os.getenv("KALEIDO_CONFIGS", "--disable-gpu-*,--single-process")


def get_kaleido_configs() -> Optional[Tuple[str, ...]]:
    if KALEIDO_CONFIGS:
        return tuple(KALEIDO_CONFIGS.split(","))
    else:
        return None
