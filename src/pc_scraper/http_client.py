"""Factory for the cloudscraper session used to fetch target-website pages."""

import cloudscraper

from pc_scraper.config import COOKIE_STORAGE_DIR


def create_scraper() -> cloudscraper.CloudScraper:
    """Build a cloudscraper session with stealth mode enabled."""
    COOKIE_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    return cloudscraper.create_scraper(
        cookie_storage_dir=str(COOKIE_STORAGE_DIR),
        enable_stealth=True,
        stealth_options={
            "min_delay": 2.5,
            "max_delay": 4.5,
            "human_like_delays": True,
            "randomize_headers": True,
        },
    )
