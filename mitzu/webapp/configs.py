from uuid import uuid4
import os


# base
LAUNCH_UID = str(uuid4())
MITZU_BASEPATH = os.getenv("BASEPATH", "mitzu-webapp")
HEALTH_CHECK_PATH = os.getenv("HEALTH_CHECK_PATH", "/_health")


# dash
GRAPH_POLL_INTERVAL_MS = int(os.getenv("GRAPH_POLL_INTERVAL_MS", 200))
BACKGROUND_CALLBACK = bool(os.getenv("BACKGROUND_CALLBACK", "true").lower() != "false")
DASH_ASSETS_FOLDER = os.getenv("DASH_ASSETS_FOLDER", "assets")
DASH_ASSETS_URL_PATH = os.getenv("DASH_ASSETS_URL_PATH", "assets")
DASH_SERVE_LOCALLY = bool(os.getenv("DASH_SERVE_LOCALLY", True))
DASH_TITLE = os.getenv("DASH_TITLE", "Mitzu")
DASH_FAVICON_PATH = os.getenv("DASH_FAVICON_PATH", "assets/favicon.ico")
DASH_COMPONENTS_CSS = os.getenv("DASH_COMPONENTS_CSS", "assets/components.css")
DASH_COMPRESS_RESPONSES = bool(os.getenv("DASH_COMPRESS_RESPONSES", True))
DASH_LOGO_PATH = os.getenv("DASH_LOGO_PATH", "/assets/mitzu-logo-light.svg")


# auth
OAUTH_SIGN_IN_URL = os.getenv("OAUTH_SIGN_IN_URL")
SIGN_OUT_URL = os.getenv("SIGN_OUT_URL")


# cache
CACHE_EXPIRATION = int(os.getenv("CACHE_EXPIRATION", "600"))

# redis cache
REDIS_URL = os.getenv("REDIS_URL")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# disk cache
DISK_CACHE_PATH = os.getenv("DISK_CACHE_PATH", "./cache")