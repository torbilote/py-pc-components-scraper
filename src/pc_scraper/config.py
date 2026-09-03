"""Runtime configuration loaded from environment variables."""

import os
import tempfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
COOKIE_STORAGE_DIR = Path(tempfile.gettempdir()) / "pc_scraper_cookies"

REQUEST_TIMEOUT_SECONDS: float = 30.0
CATEGORY_SLEEP_SECONDS: float = 5.0
URL_SLEEP_SECONDS: float = 1.0

# The target website occasionally returns a transient error (e.g. a stray
# 404) for an otherwise-valid page. Retry a bounded number of times before
# giving up.
MAX_PAGE_FETCH_ATTEMPTS: int = 3
RETRY_BACKOFF_SECONDS: float = 2.0

# No default is committed here on purpose: the scraped site's domain must
# not appear anywhere in the repository. Set it locally via .env.
_raw_target_base_url = os.getenv("TARGET_BASE_URL")
if not _raw_target_base_url:
    raise RuntimeError(
        "TARGET_BASE_URL environment variable must be set (e.g. in a "
        "local .env file); no default is committed to this repository."
    )
TARGET_BASE_URL: str = _raw_target_base_url.rstrip("/")

# The target website's CSS class names are hashed and change on every
# frontend redeploy. Override these without rebuilding the Docker image
# whenever they drift.
PRODUCT_CONTAINER_CLASS = os.getenv("TARGET_PRODUCT_CONTAINER_CLASS", "KWQcA")
PRODUCT_NAME_CLASS = os.getenv("TARGET_PRODUCT_NAME_CLASS", "jNOIct")
PRODUCT_PRICE_CLASS = os.getenv("TARGET_PRODUCT_PRICE_CLASS", "fDzLzF")
PRODUCT_ATTRIBUTES_CLASS = os.getenv(
    "TARGET_PRODUCT_ATTRIBUTES_CLASS", "iOXoLL"
)
PAGINATION_CLASS = os.getenv("TARGET_PAGINATION_CLASS", "jTuhUe")
